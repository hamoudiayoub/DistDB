# 0008 — Distributed transactions (Percolator, snapshot isolation)

> Draft written while building Phase 7. The theme is "atomicity without a coordinator": a
> transaction touches keys in many ranges, each its own Raft group, and either all of its writes
> become visible at once or none do, even if its client dies halfway. The parts to defend without
> notes are the commit point, lock resolution, and why GC must resolve locks first.

## Problem

Phase 6 made every single-key operation linearizable across many ranges. A bank transfer is two
keys, often in two ranges: debit one account, credit another. Done as two separate writes, a crash
between them loses money, and a reader between them sees the wrong total. Phase 7 adds
**transactions with snapshot isolation (SI)**:

- every transaction reads from one consistent snapshot, as of its start;
- its writes become visible all at once, at its commit;
- two concurrent transactions that write the same key cannot both commit (first committer wins).

SI still allows **write skew** (two transactions read the same keys and write disjoint ones), and
it is not serializable. It forbids dirty reads, lost updates and read skew. The tests check both
sides.

## The shape

```
 TxnClient (distdb-txn)                       PD leader: timestamp oracle (TSO)
   begin ── start_ts ◀───────────────────────  physical ms << 18 | logical, limit kept by CPut
   get(k) ─▶ TxnGet(k, start_ts) ─▶ range of k ─▶ lock? ─▶ resolve it ─▶ retry
   put/delete: buffered
   commit:
     1. Prewrite every key (primary first in order)  ─▶ ranges: lock + data, or conflict
     2. commit_ts ◀─ TSO
     3. Commit the primary   ═══ the commit point ═══
     4. Commit the secondaries (best effort; readers finish the job)
 GcWorker (on the PD leader): floor ─▶ resolve old locks ─▶ safe point ─▶ ranges drop old versions
```

## Timestamps: the TSO

A timestamp is `physical_ms << 18 | logical`: 2^18 per millisecond, comparable as plain longs.
The PD leader issues them, strictly increasing, in batches (a client asks for as many as it has
waiting transactions).

The leader may issue only below a **limit** that it owns in the PD's replicated state. To own it,
it moves the limit forward with a conditional put, from the value it read to `max(that, now) + 3s`,
and starts issuing above the old value. Two PD nodes that both believe they lead can't both
succeed in moving the same limit, so their timestamps never overlap.

Uniqueness is not enough. If transaction T1 commits, and T2 *then* begins, T2's start must be
above T1's commit, or T2 misses T1's writes. A deposed leader could still issue a small timestamp
after the new one issued larger ones. So each batch first does a **linearizable read of the
limit** (a ReadIndex read in the PD group) and checks that it still owns it. A request that
arrives after another leader took over then sees the new limit and is refused.

## Storage: three column families

Built on Phase 2's `MvccKey` (`key@ts`, newest first), next to the plain KV space, inside every
range's span:

```
0x03 key@start_ts    → value                      data
0x04 key             → lock: primary, start_ts, TTL, kind    (at most one per key)
0x05 key@commit_ts   → write record: start_ts, kind (PUT, DELETE or ROLLBACK)
```

A **write record** makes a version visible: `key@commit_ts → start_ts` points at the data written
at `start_ts`. A ROLLBACK record sits at `key@start_ts` and marks a transaction that must never
commit there. The plain KV API (`0x02`) and the transactional one are separate key spaces.

Splits, snapshots and replica removal now cover all four spaces. Each is a span of the same client
keys, because the escaped key encoding sorts like the keys themselves.

## The commands

All of them go through the range's Raft log and are decided when applied, identically on every
replica. All are **idempotent**, so retrying them needs no request ids: Percolator's own state
recognizes a repeat.

| Command | On key `k`, transaction `start_ts = S` |
|---|---|
| `Prewrite(k, primary, S, ttl, put/delete)` | refuse if `S` is below the range's floor (`TxnTooOld`); a lock of another transaction → `KeyLocked`; a newer committed write (`commit_ts > S`) → `WriteConflict`; our own ROLLBACK → `TxnAborted`; already committed by us → ok; else write data and lock |
| `Commit(k, S, C)` | our lock → write record `k@C`, remove the lock; already committed → ok; rolled back or never prewritten → `TxnAborted` |
| `Rollback(k, S)` | our lock → remove it and its data; committed → `TxnCommitted(C)`; always leave a ROLLBACK record |
| `CheckTxnStatus(primary, S, now)` | our lock, alive → LOCKED; our lock, expired or below the floor → roll back → ROLLED_BACK; committed → COMMITTED(C); nothing → write ROLLBACK → ROLLED_BACK |
| `SetTxnFloor(F)`, `Gc(G)` | the range's floor and safe point (below) |

The ROLLBACK record is what makes "nothing → rolled back" safe. A prewrite still in flight when a
reader declared the transaction dead arrives to find the record, and is refused.

## The protocol

1. **Begin:** take `start_ts` from the TSO.
2. **Read** `k`: if `k` has a lock with `start_ts ≤` ours, the owner may commit below our start,
   so we can't decide yet: resolve the lock (below) and retry. Otherwise, the newest write record
   with `commit_ts ≤ start_ts` (skipping ROLLBACKs) gives the version. Reads are ReadIndex reads,
   so a lock prewritten before our timestamp was issued is always seen.
3. **Write:** buffered in the client.
4. **Prewrite** every written key. The smallest key is the **primary**; every lock names it.
5. Take `commit_ts`. It is issued after every prewrite succeeded, so any reader whose `start_ts`
   is above it finds either our locks or our write records.
6. **Commit the primary: the commit point.** Its write record existing *is* the fact that the
   transaction committed. Before it, a reader that finds our lock and decides we're dead may roll
   us back (and our commit then fails with `TxnAborted`). After it, we are committed, whatever
   happens to our client.
7. **Commit the secondaries**, in the background. If the client dies here, readers finish the job.

**Lock resolution** (a reader met a lock of transaction `S` with primary `p`): ask `p`'s range
`CheckTxnStatus(p, S, now)`. COMMITTED(C): commit the lock we met at `C` (roll forward).
ROLLED_BACK: roll it back. LOCKED: the owner is alive; back off and read again. The TTL is
measured on the timestamps themselves (the physical part of `now` vs `S`), so every replica
reaches the same verdict.

No coordinator, no transaction log: the primary key's state is the transaction's state.

## Garbage collection with a safe point

Old versions must go, but carefully. A lock resolution needs the primary's write record to tell
committed from rolled back. If GC deleted that record while some secondary lock still pointed at
it, a reader would conclude "rolled back" for a transaction that committed: half a transfer.

So GC is three steps, run by the **GC worker** on the PD leader, walking the key space range by
range as a scan does (so splits during the walk are handled the usual way):

1. **Floor.** `G = TSO time − lifetime`. Send every range `SetTxnFloor(G)`. From then on, the
   range refuses prewrites below `G`. After the walk, no new lock below `G` can appear anywhere.
2. **Resolve.** Scan every range for locks with `start_ts < G` and resolve them. A transaction
   still unfinished after the lifetime is rolled back, even if its TTL hasn't run out.
3. **Safe point.** Store `G` in the PD (conditional put, so it only grows). Stores learn it from
   heartbeat answers, and each range's leader proposes `Gc(G)`. For each key it keeps every version
   newer than `G` plus the newest one at or below it (unless that is a delete), and drops the
   rest. Reads below `G` get `TxnTooOld`.

A transaction older than the lifetime fails and must start again: the price of a bounded history.

## Verification (the Phase 7 gate)

**`BankSimulationTest`** runs Phase 6's cluster with bank clients instead of single-key ones. The
cluster is a PD of three, four or five data nodes, and four clients over 4 to 20 accounts. The
faults are all of Phase 6's:

- partitions, also of PD nodes and clients;
- power cuts of data nodes and of the PD leader;
- disk failures, stalled timers and clock drift;
- forced splits between accounts, and forced replica moves.

On top of them:

- **clients die mid-commit:** one transaction in twenty stops after its prewrites, or right after
  its commit point;
- **GC** runs every second with a lifetime of 2 to 5 seconds, so transactions that run too long
  fail with `TOO_OLD` and old versions are really dropped.

Each client loops: a transfer of a random amount between two random accounts (if the balance
allows), or an audit, a read-only transaction that reads every account. The checks:

1. every audit finds exactly the money the bank opened with, and no account missing or negative;
2. a final audit after the faults finds the same total;
3. every client got something done after the faults stopped;
4. Phase 6's checks: no overlapping replicas on a node, every replica applies the same entry at
   each index, and the ranges tile the key space with identical replicas, now comparing the
   transactional columns byte for byte.

- **Gate:** 2,000 seeds pass in about 28 minutes.
  - **Money:** 144,429 transfers committed, and 50,701 audits each found the opening total.
    4,439 transactions gave up after three conflicts, 196 ended with an unknown outcome, and
    2,142 clients were killed mid-commit, leaving their locks for readers.
  - **Reshuffling:** 13,601 splits and 369,687 membership changes, with 16,843 GC rounds.
  - **Faults:** 10,496 power cuts (2,481 of them on the PD leader), 4,590 disk failures and
    10,556 partitions.

  No audit was ever off.
- **Bugs found on the way:** the transaction code itself had none the runs found. They found
  three in Phase 6's replica lifecycle instead, which bank runs stress differently: a message
  stamped with the configuration that removed its receiver, an orphan that nobody answered, and
  an orphaned learner that was never probed. See the journal.

**`TxnAnomalyTest`** drives transactions by hand on a fault-free simulated cluster, one test per
anomaly:

- **dirty read:** forbidden. A reader waits out an abandoned transaction's lock, rolls it back,
  and reads the committed value.
- **lost update:** forbidden. Of two read-modify-writes of one key, the second to commit gets
  `CONFLICT`.
- **read skew:** forbidden. A reader keeps its snapshot while a transfer commits between its two
  reads.
- **stale snapshot:** forbidden. A transaction sees everything that committed before it began.
- **dead client after the commit point:** the reader rolls the secondary forward.
- **write skew:** *allowed*. Two transactions each keep `x + y >= 1` on their own snapshot, and
  break it together.

**`PercolatorTest`** checks each command's rules on one engine (9 tests).

**Mutation checks** (one planted bug at a time, then the unit tests):

| Bug put in on purpose | Simulation | Unit tests |
|---|---|---|
| T1: prewrite ignores newer writes (no write conflicts) | seed 1: an audit found 1574, not 1600 | `firstCommitterWins`, `lostUpdatesAreImpossible` |
| T2: reads ignore locks | seed 1: an audit found 1629, not 1600 | `aReaderAtOrAfterALocksStartMustResolveIt`, the roll-forward test |
| T3: CheckTxnStatus finds nothing and leaves no ROLLBACK record | seed 2: 1113, not 1100 | `anExpiredLockIsRolledBackByWhoeverAsks` |
| T4: readers roll back locks of committed transactions | seed 1: 1582, not 1600 | `aReaderFinishesATransactionWhoseClientDiedAfterItsCommitPoint` |
| T5: GC sets the safe point without resolving old locks | seed 263: 912, not 900 | |
| T6: the TSO issues from its cached limit without re-reading it | seed 129: a transaction read before the accounts were opened ("acct-006 vanished") | |
| T7: the commit timestamp is taken before the prewrites finish | seed 3: 608, not 600 | |

What the rare ones say:

- **T5** needs a client to die right after its commit point, and its secondary lock to outlive the
  GC lifetime while the primary key is overwritten. Then GC deletes the primary's commit record,
  and a reader takes the committed transaction for a dead one.
- **T6** needs a PD leader that does not yet know it was deposed, is still reachable by clients,
  and holds a limit. With the planted bug, that leader issued timestamps below ones its successor had already issued, so a transaction began "before" the accounts were opened. A first attempt at this mutation skipped only the ownership comparison and survived 300 seeds: the linearizable read it left in place already stops a deposed leader, which cannot complete a read without a majority.

## Not yet

- Batching keys per range in one prewrite or commit (each key is its own Raft proposal).
- Async commit, 1PC, pessimistic locks, and deadlock detection (there are no waits-for cycles:
  writers never wait, they fail or resolve).
- Serializable isolation (SSI): write skew is allowed, as SI allows it.
- Large transactions (all writes are buffered in the client).

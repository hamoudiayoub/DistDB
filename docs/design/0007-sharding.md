# 0007 — Multi-Raft and sharding

> Draft written while building Phase 6. The theme is "one Raft group is not a database": the key
> space must be cut into ranges that split and move while clients keep reading and writing, and no
> client may notice anything but a little extra latency. The parts to be able to defend without
> notes are the split rules, replica removal, and why the placement driver is never trusted for
> safety.

## Problem

Phase 5 replicates the whole key space with one Raft group, so the cluster is no bigger and no
faster than one machine. Phase 6 cuts the key space into **ranges** `[start, end)`. Each range is
its own Raft group of (usually) three replicas, every node hosts replicas of many ranges, ranges
**split** as they grow, and replicas **move** between nodes to balance the load or to replace a
dead node. The bar stays the same: every client operation is linearizable, and no write takes
effect twice, whatever splits, moves, crashes and partitions happen meanwhile.

## The shape

```
                 ┌────────────── placement driver (PD): 3 nodes, its own Raft group ──────────┐
                 │  range-id allocator (replicated)   directory, leaders, liveness (soft)     │
                 │  scheduler: repair, balance, split → operators                             │
                 └───────▲ heartbeats (leaders report ranges)       │ operators ──────────────┘
                         │                                          ▼
 client ─ locate(key) ─▶ PD          node 1           node 2           node 3          node 4
   │ range cache                 ┌─ Store ────┐   ┌─ Store ────┐   ┌─ Store ────┐   ┌─ Store ─┐
   └─ request(range id, epoch) ─▶│ r1 r2 r5   │◀─▶│ r1 r2 r7   │◀─▶│ r1 r5 r7   │◀─▶│ r2 r5 r7│
                                 │ one engine │   │ one engine │   │ ...        │   │         │
                                 └────────────┘   └────────────┘   └────────────┘   └─────────┘
                                 Raft messages of all ranges, batched into one frame per peer
```

| Piece | Where | What it does |
|---|---|---|
| `RangeDescriptor` | `distdb-rpc` (proto) | id, `[start, end)`, voters, learners, epoch `(conf_ver, version)` |
| `Store` | `distdb-kv` | one node: many replicas, one engine, one mailbox, one transport |
| `Replica` | `distdb-kv` | one range on one node: `RaftNode`, `RaftStore`, state machine, pending requests |
| `RangeStateMachine` | `distdb-kv` | applies one range's log to its span: writes, splits, membership |
| `StoreFrames` | `distdb-kv` | the batched wire format between stores |
| `PlacementDriver` | `distdb-pd` | the PD's logic, on top of a Phase 5 `ReplicatedKv` group |
| `PdClient` | `distdb-rpc` | follows the PD leader, for stores and clients |
| `KvClient` + `RangeCache` | `distdb-client` | key → range → leader, invalidated by typed errors |

## Ranges and epochs

A **range descriptor** names a range and its replicas:

```
RangeDescriptor { range_id, start_key, end_key (absent = +∞), voters, learners, conf_ver, version }
```

The **epoch** is two counters. `version` goes up when the span changes (a split), and `conf_ver`
when the replicas change. Every client request carries the range id and the `version` it was
routed with. A node answers:

- `RangeNotFound` if it has no initialized replica of that range;
- `EpochMismatch` if the version differs or a key is outside the span: the client's cache is
  stale, so it asks the PD again;
- `NotLeader` with the leader's address, as in Phase 5.

`conf_ver` is the log index of the entry that last changed the replica set (or the range's first
index). That makes it comparable with Raft's own "index of the latest config", which the replica
removal rule below needs.

## Engine layout: many ranges, one engine

Every node has **one** LSM engine for all its replicas, so one WAL and one fsync can cover many
ranges. Keys:

```
0x01 'A' range            applied index           ┐
0x01 'D' range            range descriptor        │ replicated (part of the range's
0x01 'r' range client seq client write results    │ state machine, and of its snapshots)
0x01 's' range client     client sessions         ┘ except 'A', which is per node
0x01 'H' range            Raft hard state         ┐
0x01 'L' range index      Raft log                │ per node
0x01 'P' range            Raft snapshot           │
0x01 'T' range            tombstone               ┘
0x02 user key             client data, shared by all ranges on the node
```

Client data is **not** prefixed by range: a split must not rewrite keys. That is only correct
because of one invariant: **the initialized replicas on one node have disjoint spans.** Splits keep
it (one span becomes two), and a snapshot that would break it is refused (see below).

## The multi-Raft host

`Store` replaces `ReplicatedKv` on data nodes. It keeps Phase 5's shape (one mailbox, run on the
`Scheduler`, the same code in the simulator) and runs many `RaftNode`s in it:

- **One tick** for the whole store ticks every replica.
- **Batched messages.** All the Raft messages produced while the mailbox runs go into one frame
  per peer node, sent once the mailbox is empty. A frame entry is `(range id, sender's config
  index, message)`. With thousands of ranges that is the difference between one RPC per tick per
  peer and thousands.
- **Ready order per replica** is unchanged: persist (synced), send, apply, advance. All the
  replicas share the engine, so their persists could share an fsync too (group commit, not done
  yet).
- **Uninitialized replicas.** A message for an unknown range (a vote request, an append, a
  snapshot) creates a replica with no data and no members: it can vote and accept a snapshot,
  nothing else. That is how a node joins a range it is being added to.

## Splits

The PD decides that a range is too big (or the simulator forces one) and sends its leader a split
operator carrying a **new range id** (allocated by the PD, unique forever). The leader picks the
split key (the middle key, if the PD named none) and proposes

```
Split { split_key, new_range_id, expected_version }
```

through the range's own log. **Every replica applies it at the same log index**, in one engine
batch that:

1. narrows the left descriptor to `[start, split_key)` and bumps its version;
2. writes the right-hand descriptor `[split_key, end)` with the same replicas and the new id;
3. copies the left range's client sessions to the right range;
4. creates the right range's Raft state: a snapshot at index 5, term 5, holding the right range's
   data, so its log starts at 6;
5. records the left range's applied index.

The batch is atomic, so after a crash either the split happened completely or not at all. A
`Split` whose `expected_version` no longer matches (a duplicate, or a stale operator) changes
nothing.

Why each rule:

- **Sessions are copied** because a write applied in the left range before the split may be
  retried after it, and the retry now routes to the right range. Without the records there, it
  would be applied a second time (mutation S1).
- **The right range starts at index 5, not 0.** A replica that has not applied the split yet may
  receive messages for the new range and create it uninitialized. An empty log matches index 0,
  so a leader whose log started at 0 would send it entries from 1, and it would apply them to no
  data. Starting at 5, the consistency check fails and the leader sends a snapshot instead (the
  same trick as CockroachDB's initial index 10).
- **A split keeps an existing hard state.** That uninitialized replica may have voted in an
  election of the new range. When the split later creates the range on that node, it keeps that
  term and vote, or the node could vote twice in one term.
- **Writes check their span at apply time.** A write proposed before the split but ordered after
  it in the log would land in the left group's log for a key that now belongs to the right group.
  Applying it would change data that the right group's other replicas never see. So the state
  machine answers `EpochMismatch` instead, identically on every replica, and the client retries at
  the right range (mutation S2). Reads check too, before they are served.
- **The old leader campaigns at once** for the new range (with pre-vote), so the new range does
  not wait an election timeout for its first leader.

## Moving replicas

A move from node A to node B is four Raft steps, each one committed before the next:

1. **add B as a learner** (it receives the log but does not vote);
2. B catches up, usually by a snapshot;
3. **promote** B to voter;
4. **remove** A. If A leads, it first **hands over its leadership** (`TimeoutNow`, below), because a
   leader that removes itself leaves the range leaderless until an election.

Adding a learner first means the range never has a voter that cannot vote usefully yet: with
three voters and one of them empty, one more failure would stop the range.

**Leadership transfer** (thesis 3.10) was added to the Raft library. The leader stops accepting
proposals, brings the target up to date, and sends it `TimeoutNow`. The target campaigns at once,
and its vote requests are marked as a transfer, so voters grant them even though they still hear
from the old leader (the lease check would otherwise refuse). If nothing happens within an
election timeout, the transfer is abandoned.

## Removing a replica safely

A removed replica is not told: the leader stops sending to it as soon as the change is appended.
It goes on believing it is a member, holds its data, and blocks any snapshot that would overlap its
span. The rules:

- **Removed notices.** When a replica gets a message from a node that is in neither its applied
  descriptor nor its latest configuration, it answers `Removed(range, conf_ver)`. The receiver
  destroys its replica **only if its own latest configuration index is below `conf_ver`.** Then
  its log does not contain the configuration that removed it, so it cannot contain any later one
  that re-added it: it is not a voter now. (Destroying a replica that is still a voter would be
  like losing its disk: it could forget an entry it acknowledged.)
- **A replica added back does not vote until it has caught up.** Destroying a removed replica is
  not the same as removing its vote. A candidate that is behind may still use a configuration
  from before the removal, and that configuration counts this node. An emptied replica sees any
  candidate's log as complete enough, and could help elect a leader that lacks entries it once
  acknowledged. Seed 231 found exactly that: two different committed entries at one index (see
  the journal). So the tombstone also records how long the log was, and a replica created again
  neither votes nor campaigns until its log is that long. By then it holds every entry it ever
  acknowledged, because it caught up from a leader, and leaders hold every committed entry.
  CockroachDB reaches the same end differently: a replica added back gets a new replica id, so
  old configurations do not count it at all.
- **Destroy keeps the hard state.** Data, sessions, log and descriptor go; the term and vote stay,
  plus a tombstone with `conf_ver`. If the node is added back later, it can't vote twice in a term
  it already voted in. Messages from configurations at or below the tombstone are dropped; newer
  ones (it was re-added) create the replica again.
- **A split that finds a tombstone** for the new range on this node creates nothing and deletes
  that part of the data: the node was already removed from the range that the split creates.
- **Snapshots must not overlap.** A snapshot whose span overlaps another initialized replica on
  the node is dropped (the leader retries later). The overlapping one is a replica that has not
  applied a split or a removal yet; once it has, the snapshot goes through.

## The placement driver

The PD is three processes running **their own Raft group, which is a Phase 5 `ReplicatedKv`**: one
group, one range covering everything, never split. The PD's logic runs on whichever PD node
leads.

- **Replicated state: only the range-id allocator.** Ids are handed out in blocks by a
  conditional put (`CPut(next_id, expected, expected + 100)`), so even two PD nodes that both
  believe they lead can't hand out the same id.
- **Soft state, rebuilt from heartbeats:** the directory (each range's descriptor with the highest
  epoch seen, and its leader), each store's address and last heartbeat. A new PD leader starts
  empty and knows everything again within one heartbeat interval. The truth is in the ranges
  themselves: each descriptor is replicated by its own group.
- **Heartbeats.** Every store sends one each interval (and at once after a split or membership
  change), with a report for each range it leads: descriptor, term, each replica's progress, and
  the key count. The answer carries all store addresses and the operators for those ranges.
- **The scheduler**, per reported range, first match wins: finish the operator in flight; promote
  a caught-up learner, or remove one that is stuck; replace a replica on a dead store (no heartbeat
  for a while); add a replica if there are too few, remove one if there are too many; split if the
  range has too many keys; move a replica from the fullest store to the emptiest; hand leadership
  from the store with the most leaders to the one with the fewest. Operators time out. At most a
  few run at once.

**The PD is never trusted for safety.** Every operator is a suggestion that the range's own leader
checks and turns into a Raft proposal: the version check makes stale splits harmless, the Raft
library refuses unsafe membership changes, and a stale directory only costs a client a retry.

## Clients

`KvClient` keeps a **range cache**: start key → descriptor and leader. For each operation it
looks up the key; on a miss it asks the PD (`Locate`). Then:

| Answer | Meaning | Client does |
|---|---|---|
| `NotLeader` with an address | another replica leads | update the cached leader, retry at once |
| `NotLeader` without one, or no answer | election, or node down | try the range's next replica |
| `RangeNotFound`, `EpochMismatch` | the cache is stale | drop the entry, ask the PD, retry |

A scan walks the ranges in key order, clipping each request to the range's span. Request ids,
deadlines, backoff and the "unknown outcome" rules are unchanged from Phase 3.

## Verification (the Phase 6 gate)

**`ShardedKvSimulation`** runs, all on one thread from one seed:

- a PD of three `ReplicatedKv` nodes;
- four or five data `Store`s on simulated disks;
- four routed `KvClient`s over 16 to 48 keys.

The faults are Phase 5's, plus some aimed at the new parts:

- partitions that also cut PD nodes and clients;
- power cuts, a third of them aimed at the PD leader;
- disks that fail mid-write;
- stalled timers and per-node clock drift;
- a data node cut off from its peers but not from its clients.

On top of the PD's own scheduling, the run **forces a random split and a random replica move**
every 0.3 to 1.5 seconds, so ranges are split and reshuffled all the time. The knobs vary per seed:

- 4 or 5 nodes;
- a split threshold of 4, 8 or 16 keys;
- compaction every 16, 64 or 1000 entries;
- heartbeats every 150 to 500 ms;
- forced operations on or off.

As it runs, the simulation checks that every replica of a range applies the same entry (term
and data) at each index, Raft's state machine safety, and every 100 ms that no node holds
overlapping replicas. At the end:

1. the history is **linearizable** per key, across every split and move;
2. no write took effect twice, in one range or in two; every acknowledged write took effect; none
   reported as certainly failed did;
3. after the faults stop, every client makes progress;
4. with the scheduler paused, the newest descriptors **tile the key space** (no gap, no
   overlap), and every member of every range holds that descriptor and the same data.

- **Gate:** 500 seeds pass in about 5.5 minutes.
  - **Linearizable:** 426,071 operations: 276,948 acknowledged writes, 148,397 reads and 726
    writes of unknown outcome.
  - **Reshuffling:** 5,144 splits and 123,400 membership changes, with 31,559 replicas destroyed
    by `Removed` notices. The runs ended with 5,644 ranges.
  - **Faults:** 2,641 power cuts (617 of them on the PD leader), 1,280 disk failures and 2,699
    partitions.

  No violation occurred.
- **Bugs found on the way:** the journal has seven entries for this phase.
  - The simulation found five bugs: one in the harness, and four in the store. Of those, one was a
    Raft safety violation, which only a full 500-seed run hit.
  - Reading the simulator found a blind spot: it could hide any exception as a disk failure.
  - The real processes showed that 10 ms ticks were too eager on a loaded machine.

**Unit tests:**

- `RangeStateMachineTest`: each split rule, mirroring membership into the descriptor, and a
  snapshot that clears a wider old span.
- `StoreTest`:
  - routing by range and by key;
  - a split on three stores;
  - a retry after a split;
  - a move whose removed replica is destroyed by a notice, then added back;
  - a leader that hands over before it is removed.
- `RaftNodeTest`: leadership transfer (four tests), and a learner that lost its log and is caught
  up again.
- `RaftSimulation`: now also hands leadership over at random under faults.

**On real processes:** `ShardedClusterTest` starts three `distdb pd` and four `distdb node` JVMs
over gRPC, nodes 1 to 3 founding range 1. It writes 200 keys, then waits for the PD's split
threshold (60 keys) and balancer to act. Then it kills node 1 outright, writes 200 more keys
through the failovers of the ranges node 1 led, and reads all 400 back, one by one and in a scan
across every range. Across runs on this machine, once the cluster had formed:

| | Fastest run | Slowest run |
|---|---|---|
| the first 200 writes | 1.0 s | 3.7 s |
| until four ranges existed, with replicas on node 4 | 4.3 s | 6.8 s |
| 200 writes with node 1 dead | 0.4 s | 10.8 s |

**Mutation checks** (one planted bug at a time, 100 seeds each, then the unit tests):

| Bug put in on purpose | Simulation | Unit test |
|---|---|---|
| S1: a split does not copy client sessions | seed 2: a write took effect twice, in r1 and in r2 | |
| S2: a write is not checked against the span when applied | seed 2: not linearizable | `aWriteForAKeyOutsideTheSpanChangesNothing` |
| S3: a read is not checked against the span when served | survived | none (see below) |
| S4: a split forgets an uninitialized replica's vote | survived | `aSplitKeepsTheVoteOfAnUninitializedReplicaOfTheNewRange` |
| S5: a `Removed` notice destroys the replica whatever its configuration | seed 10: a range was lost (the ranges no longer tile the key space) | |
| S6: no overlap check before installing a snapshot | seed 1: not linearizable | |
| S7: no compaction right after a split | seed 2: a learner never initialized | |
| S8: destroying a replica also forgets its term and vote | survived | `aMovedReplicaIsDestroyedByARemovedNoticeAndCanBeAddedBack` |
| S9: a split creates the new range on a node that is not a member | survived | `aReplicaThatIsNoLongerAMemberDoesNotCreateTheNewRange` |
| S10: a replica added back votes before it has caught up | seed 231, the only failure in 300 seeds: two different entries committed at one index | |

What the survivors say:

- **S4 and S8** break Raft's one-vote-per-term rule, but only if a node votes in a term, loses the
  vote, and is asked again in the same term by a different candidate. That needs three unlikely
  events in a row; the targeted tests pin the rule instead.
- **S9** used to be caught (it is the second journal entry of this phase), but compacting at each
  split made it rare: the orphan it creates only matters once it blocks a snapshot.
- **S3 is not a bug today.** A read waiting on the left range began before the split committed. The
  range's leader applies a committed entry before any follower even learns it is committed, so no
  write in the right range can have completed before the read began, and the value served comes
  from a state inside the read's own interval. The check stays as a safeguard: follower reads, or
  any change to who applies first, would make it necessary.

## Not yet

- Merges of small ranges.
- Load-based splitting and placement (only key counts and replica counts are used).
- Snapshots streamed in chunks; splits that copy the right range's data into its snapshot are
  O(range size), fine at simulator sizes.
- Group commit across ranges.
- Quiescing idle ranges (every range ticks and heartbeats forever).
- A persistent PD directory (it is rebuilt from heartbeats after a PD failover).

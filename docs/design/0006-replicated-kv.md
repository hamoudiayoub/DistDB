# 0006 — Replicated KV service

> Draft written while building Phase 5. The theme is "the gap between a consensus algorithm and
> a usable service": Raft gives you a replicated log; everything in this note is what it takes to
> turn that into a service whose clients can't tell it isn't one machine.
>
> **Since Phase 6:** data nodes run `Store`, which hosts many ranges, each a group like the one
> described here; [0007](0007-sharding.md) explains what sharding changed. `ReplicatedKv` remains
> for a single group, and it is what the placement driver's own group runs on. Its write path is
> shared with the ranges (`KvWrites`), which added a conditional put.

## Problem

Put Raft (Phase 4) inside the KV server (Phase 3), so every write is replicated to a majority
before it's acknowledged and a cluster of 3 or 5 nodes keeps serving when any minority fails. The
bar is **linearizability**: every client, whatever the failures, sees results that a single
machine executing the operations one at a time, in real-time order, could have produced.

## The shape

```
client ──▶ any node ──NotLeader(leader address)──▶ client follows it
client ──▶ leader: propose(write) ──▶ Raft log ──majority──▶ committed
                                                   │
                     every replica applies it: KvStateMachine → engine (data + applied index)
leader answers the client once it has applied the entry
```

| Piece | Where | What it does |
|---|---|---|
| `ReplicatedKv` | `distdb-kv` | one replica: the Raft loop, proposals, ReadIndex reads, redirects |
| `KvStateMachine` | `distdb-kv` | applies committed entries: dedup, data, applied index, snapshots |
| `RaftStore` | `distdb-kv` | the Raft log and hard state in the same engine (Phase 4) |
| `KvReads`, `KvRequests` | `distdb-kv` | the read path and validation, shared from Phase 3 |
| `GrpcTransport` | `distdb-rpc` | Raft messages: one long-lived gRPC stream per peer |
| `KvClient` | `distdb-client` | seeds, leader redirects, moving on from dead nodes |
| `distdb node --id N --peers …` | `distdb-server` | a replica process |

Phase 3's `KvService` is gone: a single node is a group of one, so there is exactly one server
implementation.

## Writes

1. The client sends to the node it thinks leads. A follower answers `NotLeader` with the leader's
   address, and the client goes there at once.
2. The leader validates the request and proposes a `WriteCommand`, which carries the request and
   **the leader's wall-clock time**.
3. When the entry commits, **every replica applies it** through `KvStateMachine`. The leader
   answers once it has applied it: by then the write is on a majority of disks.

**Deduplication moved to apply time.** In Phase 3 the server checked "have I answered this request
id?" before writing. With replication, a client's retry can reach a *new* leader while the first
copy is still in the log. Both copies get committed, and both reach every replica. So the check
has to happen where every replica makes the same decision: when applying. The second copy is
answered from the first one's record and changes nothing. Mutation C shows what happens without
it: a write took effect twice.

**The state machine must be deterministic.** Applying an entry may depend only on the entry and
the current state, never on the local clock or randomness, or replicas diverge. That's why the
leader's time travels *inside* commands. Session expiry, for the same reason, is itself a
replicated command (`ExpireSessions(now, ttl)`) that the leader proposes.

**Losing the leadership.** A leader that steps down with writes still waiting answers them
`NotLeader(maybe_applied = true)`. They are in its log and may still commit under the next
leader. The client retries there; dedup makes that safe; and if it gives up, it reports the write
as of unknown outcome.

## Reads: ReadIndex, not a local read

A leader may not simply read its local data. It might have been deposed without knowing, and a
new leader may already have committed newer writes. ReadIndex (Phase 4):
1. record the commit index;
2. confirm leadership with a heartbeat round to a majority;
3. wait until that index is applied;
4. then read locally.

Reading locally without the heartbeat round is a **lease read**. It's safe only if a deposed
leader always notices before anyone else wins an election, which depends on bounded clock drift.
Mutation A is that bug. The simulator only caught it once nodes could **stall** (timers running
4–8× slow, like a long GC pause) while cut off from their peers but not their clients. With
perfect clocks, check-quorum always deposes the old leader in time, and the bug is invisible.

## The applied index lives with the data

Each entry is applied in **one batch that also records its index**. After a crash, data and
applied index agree, and Raft replays exactly the entries after it. Applies need no fsync, because
the log is already durable and a lost apply is simply redone.

There's one subtlety: compacting the log deletes entries, so their applies must be on disk
first. They are, because the compaction is written by a synced batch to the *same* engine
(same WAL), and Phase 2's prefix property makes every earlier batch durable with it.

Mutation D splits the applied index into its own batch. A crash can then record an entry as
applied when its data was lost, or the reverse; the simulation catches a write taking effect
twice.

**Snapshots.** `KvStateMachine.snapshotData()` serializes the replicated key ranges (client data
and sessions). A follower that needs compacted entries gets them by `InstallSnapshot`, and
`restore` replaces its replicated ranges in one batch. If a crash lands between storing the
snapshot and applying it, startup notices (applied index below the stored snapshot) and finishes
the job.

## Threading: one mailbox

`RaftNode` is not thread-safe, but client requests arrive on gRPC threads, peer messages on
transport threads, and ticks on timers. Every one of them is a task in **one mailbox**, run one at
a time on the `Scheduler` (an actor). The same code runs unchanged in the single-threaded
simulator.

## Clients

`KvClient` holds seed addresses and a guess of the leader:
- A `NotLeader` with an address makes it go straight there, without backing off.
- A node that doesn't answer, or knows no leader (an election is on), makes it move to the next
  seed, with backoff.

One lesson from the simulator: **the per-attempt timeout must be well below the call's
deadline**. With both at 2 s, a single lost message used up the whole call. The simulated clients
use 400 ms attempts within 2 s. The production defaults are 2 s attempts within 5 s.

## Verification (the Phase 5 gate)

**`ReplicatedKvSimulationTest`** runs 3 or 5 real replicas (Raft, state machine, LSM engines on
simulated disks) with 4 concurrent `KvClient`s, all on one thread from one seed. The faults:
- partitions that also cut clients off;
- the leader cut off from its peers but not its clients;
- power cuts (half of them aimed at the leader) and mid-write disk failures;
- stalled processes, and per-node clock drift.

Every client operation is recorded with its start, end and result. Then the run checks:
1. the history is **linearizable** (`Linearizability`: Wing and Gong's search with Lowe's
   memoization, checked per key);
2. no write took effect twice, every acknowledged write took effect, and no write reported as
   certainly failed did;
3. after the faults stop, every client makes progress and all replicas hold identical data.

The run also measures failover.

- **Gate:** 500 seeds pass in about 2 minutes. They checked 469,599 operations for
  linearizability: 304,531 acknowledged writes, 163,269 reads and 1,799 writes of unknown outcome.
  The faults: 2,398 power cuts (1,134 of them on the leader), 969 mid-write disk failures and
  2,897 partitions. No violation occurred.
- **Failover, simulated:** a new leader at p50 360 ms (p99 1.8 s); the next successful client
  write at p50 496 ms (p99 2.8 s). That's with 10 ms ticks, so elections start after 100–200 ms,
  and clocks drift between nodes.
- **Failover, real processes:** `ReplicatedClusterTest` runs three JVMs over gRPC and kills the
  leader outright. Writes succeeded again after about 1.1 s. The test then restarts that node and
  kills another, so the restarted node must have caught up for the cluster to keep working.
- **The checker's own tests:** `LinearizabilityTest` covers known-good and known-bad histories,
  unknown outcomes, and a 2,000-operation concurrent history.
- **Mutation checks:**

  | Bug put in on purpose | Caught? |
  |---|---|
  | A: leader reads locally (lease read) without ReadIndex | seed 7 (not linearizable), but only once stalls and peer-only partitions existed; 100 seeds missed it before |
  | B: acknowledge a write when proposed, not when applied | seed 1 (acknowledged write never took effect) |
  | C: no dedup at apply time | seed 1 (a write took effect twice) |
  | D: applied index written apart from the data | seed 32 (a write took effect twice) |

## Not yet

- Reads from followers, and lease reads (with a clock-drift bound).
- Streaming or chunked snapshots (state is materialized in memory).
- Batching and pipelining proposals.
- Flow control on the Raft stream.
- Membership changes at runtime: the Raft library supports them, and the placement driver will
  drive them in Phase 6.
- One group per node: sharding into many groups is Phase 6.
- TLS and authentication.

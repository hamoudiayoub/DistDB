# DistDB: the ten phases explained

DistDB is a distributed SQL database built from scratch in Java 25. Data is split into ranges of
keys, each range is copied to three machines and kept identical by Raft, transactions span
ranges, and any node answers SQL from `psql`. It is built bottom-up in ten phases. Each phase
ends at a **gate**, a test that proves it works, and leaves a design note in `docs/design/`.

This document explains what each phase does, how it works, and what to learn from it.
**Phases 1–5 are built** (with their measured results); **phases 6–10 are the plan.**

```
 SQL (8, 9)      psql ─▶ pgwire ─▶ parser ─▶ planner ─▶ distributed executor
 transactions (7)          Percolator: timestamps, locks, two-phase commit
 routing (6)               range cache ─▶ which range, which leader?
 replication (5, 6)        one Raft group per range, many ranges per node
 consensus (4)             Raft: a replicated log
 server (3)                gRPC, deadlines, retries, idempotency
 storage (2)               LSM engine on each node's disk
 foundations (1)           determinism seams, key encoding, build
```

---

## Phase 1: Foundations

**Goal.** Set up the project so that everything built later can be tested deterministically,
and give the database a way to turn any value into bytes that sort correctly.

**What it builds.**
- A Gradle multi-module build: one module per layer (`distdb-common`, `distdb-env`,
  `distdb-storage`, `distdb-raft`, `distdb-rpc`, `distdb-kv`, `distdb-pd`, `distdb-client`,
  `distdb-txn`, `distdb-sql`, `distdb-pgwire`, `distdb-server`, `distdb-sim`, `distdb-bench`).
- Shared build settings in `build-logic`: Java 25, warnings as errors, Spotless formatting
  (google-java-format), JaCoCo coverage, and GitHub Actions CI.
- **The determinism seams** (`distdb-env`): the only ways core code may reach the outside world.

  | Seam | Real implementation | Simulated implementation |
  |---|---|---|
  | `Clock` | `System.nanoTime`, wall clock | virtual time the simulator advances |
  | `RandomSource` | seeded from entropy | one seed for the whole run, split per component |
  | `Transport` | gRPC (Phase 5) | in-memory network with losses, delays, partitions |
  | `FileSystem` | `FileChannel` with real fsync | in-memory files that lose or tear unsynced data |
  | `Scheduler` | virtual threads and a timer | a single-threaded event loop |

- **ArchUnit rules** that fail the build if a module depends upward, or if core code calls
  `System.currentTimeMillis`, `new Random()`, `Thread.sleep`, sockets or `Files` directly.
- **Order-preserving key encoding** (`KeyWriter`, `KeyReader`): encodes nulls, booleans, longs,
  doubles, byte strings and strings so that comparing the bytes gives the same order as comparing
  the values, in ascending or descending order.

**How the key encoding works.**
- Each value starts with a type tag (null `05`, false `10`, true `11`, long `20`, double `30`,
  bytes `40`, string `41`), so values of different types still sort consistently.
- A long is stored big-endian with its sign bit flipped, so negative numbers sort before
  positive ones.
- A double uses the IEEE-754 trick: flip the sign bit for positives, flip every bit for
  negatives.
- Strings and byte strings escape `00` as `00 FF` and end with `00 01`. A shorter string then
  sorts first, and no encoding is a prefix of another.
- For descending order, every byte is inverted.
- This is what lets a SQL table's rows, and its indexes, be stored in key order (Phase 8).

**Why it matters.** Distributed bugs show up once in a million runs. If every source of
nondeterminism sits behind a seam, a whole cluster can run in one thread from one seed, and any
failure can be replayed exactly. This single decision is what makes phases 2–5 testable.

**Concepts to learn.** Dependency inversion and fakes; monotonic vs wall clocks; splittable random
generators; byte-comparable encodings; the durability contract of fsync; enforced layering;
property-based testing.

**Gate.** Property tests (jqwik) prove that byte order equals value order for every type, in both
directions. *Done.*

---

## Phase 2: LSM storage engine

**Goal.** Give every node a fast, crash-safe, ordered key-value store: the only storage interface
everything above uses (`KvEngine`: `write(batch, sync)`, `get`, `scan`, `snapshot`).

**What it builds** (`distdb-storage`).
- **Write-ahead log (WAL):** every batch is appended to a log first, as a checksummed record.
- **Memtable:** a sorted skip list in memory. When full, it is frozen, and a new one takes over.
- **SSTables:** immutable sorted files with:
  - 4 KB prefix-compressed blocks, with restart points;
  - a bloom filter (about 1% false positives at 10 bits per key);
  - an index of each block's last key;
  - a checksummed footer.
- **Leveled compaction:** level 0 holds freshly flushed files, which may overlap. Levels 1–6 hold
  non-overlapping files, each level about 10 times larger than the one above. Compaction merges
  files down the levels and drops overwritten and deleted data.
- **Manifest:** a log of which files are live, switched atomically through a `CURRENT` file.
- **Snapshots** and the **MVCC key format** (`MvccKey`: key + descending timestamp, newest
  version first), ready for Phase 7.
- **`SimFileSystem`:** a simulated disk. On a crash, it loses, keeps or tears unsynced data
  page by page, and it can fail any chosen write.

**How it works.**
- **Sequence numbers.** Every write gets an increasing sequence number, and internal keys sort by
  user key, then newest first. A reader picks a sequence number and ignores anything newer.
  That's what gives readers that don't block writers, atomic batches, and snapshots that are just
  a number.
- **Reads** check the memtable, then the frozen memtable, then level 0 newest first, then one
  file per deeper level. Bloom filters skip most files.
- **Crash safety** comes from a strict fsync order:
  - a new file's directory entry is synced before the file is used;
  - the set of live files changes only through a synced manifest edit;
  - a log is deleted only after its data is recorded in a table;
  - the old log is synced before a new one starts.
- **Recovery** replays the manifest, then the logs, and stops at the first damaged record. It
  always restores a *prefix* of the writes.

**Concepts to learn.** B-tree vs LSM; read, write and space amplification; WAL framing and torn
writes; bloom filter math; k-way merging; compaction strategies; tombstones; crash consistency
and fsync ordering; model-based testing; fault injection.

**Gate.** A model test compares the engine with a Java `TreeMap` over **2 million random
operations**. A crash test runs **20,000 simulated power cuts** (500 seeds); after each, the
recovered data must be exactly a prefix of the writes, including every synced one. *Passed.*

**The bug it found was in the simulator.** The simulated disk zeroed bytes that had already been
synced, which made it look as if the engine lost data. Lesson: the checker needs tests too.

---

## Phase 3: Single-node KV server

**Goal.** Put the engine behind a network API that is safe to use over a network that loses
messages.

**What it builds.**
- **The API** (`kv.proto`): one gRPC method, `Kv/Call`, carrying a union of Get, Put, Delete and
  Scan, like CockroachDB's `Batch`. Deadlines, retries and errors each then have one code path.
- **Deadlines:** a deadline is set once per call and crosses the network as time remaining. The
  server skips work whose deadline has already passed.
- **Typed errors** that tell the client what to do: `ServerBusy` (retry later), `InvalidArgument`
  (never retry), `StorageFailure`, `StaleRequest`, and from Phase 5 `NotLeader`.
- **The client** (`KvClient`): retries with exponential backoff and full jitter, within the
  deadline.
- **The `distdb` binary:** `distdb node` serves; `distdb kv put/get/delete/scan` is a
  command-line client.

**The core problem: unknown outcomes.** When a request gets no answer, it may have been lost on
the way, or applied with only the reply lost. Retrying a write can then apply it twice, which can
break linearizability even for plain puts. The fix:
- **Request ids:** each write carries a random client id and a sequence number, and a retry
  reuses them.
- **Client sessions:** the server records each write's answer in the *same atomic batch* as the
  write, and answers retries from that record.
- **A floor:** answers the client no longer needs are deleted, and late duplicates below the
  floor are rejected.
- **Expiry:** idle sessions expire after an hour, which is safe because client deadlines are
  capped at 5 minutes.
- **Honest outcomes:** the client reports a failed write as "certainly not applied" only with
  proof; otherwise it says the outcome is unknown. The CLI uses exit code 3 for the first and 4
  for the second.

**Concepts to learn.** RPC design; HTTP/2 and gRPC; deadline propagation; retry classification;
backoff with jitter; idempotency keys and dedup tables; at-least-once delivery plus idempotence;
load shedding; crash-only design; paging with resume keys.

**Gate.** The node runs as its own OS process and the CLI as another, talking over TCP. The node
is then killed with `kill -9`, restarted, and every acknowledged write is still there. *Passed.*

**Bugs found.** The server claimed "nothing changed" after a storage error that might have
written to disk (fixed with `maybe_applied`). A timeout while still connecting was reported as
"maybe applied", though nothing had been sent (fixed with a gRPC stream tracer).

---

## Phase 4: Raft

**Goal.** Keep several copies of a log identical across machines that crash and lose messages, so
that a committed command is never lost or reordered.

**What it builds.**
- **`RaftNode`** (`distdb-raft`): Raft as a *pure state machine*, with no I/O inside. The host
  calls `tick()`, `step(message)` and `propose(command)`, then repeatedly takes a `Ready` and
  carries it out in order: persist (fsync), send, apply, then `advance`.
- **`RaftStore`** (`distdb-kv`): stores the Raft log, term and vote in the LSM engine, one synced
  batch per `Ready`.
- **A deterministic simulator** (`distdb-sim`): `SimNetwork`, `RaftSimulation` and `RaftChecker`.

**What the algorithm does, and why each rule exists.**
- **Elections.** Randomized timeouts make split votes rare. A node votes once per term, and the
  vote is saved before the reply is sent. Otherwise two leaders could be elected in one term.
- **The election restriction.** A node votes only for a candidate whose log is at least as up to
  date as its own. Otherwise a node missing committed entries could become leader and erase them.
- **Replication.** Each AppendEntries carries the index and term of the entry before the new
  ones, and a follower accepts only if its log matches there. So two logs that agree on one entry
  agree on everything before it.
- **Fast backtracking.** On a rejection, the follower reports its conflicting term, so the
  leader skips a whole term per round trip.
- **The commit rule (Figure 8 of the paper).** A leader commits by counting replicas only for
  entries of its *own* term. It appends a no-op when elected so older entries commit promptly.
- **Persist before send.** A vote or an acknowledgement is a promise, and must be on disk before
  anyone hears it.
- **Snapshots.** Old log entries are replaced by the state machine's state, and a follower that
  is far behind receives that state with InstallSnapshot.
- **Pre-vote and check-quorum.** A node cut off from the group can't push the term up and depose
  a healthy leader. A leader that loses its majority steps down.
- **Membership changes.** Servers are added or removed one at a time; new ones join as learners
  and are promoted once caught up. A new leader must commit an entry of its own term before
  changing membership.
- **ReadIndex.** Linearizable reads without writing to the log: the leader confirms with a
  heartbeat round that it still leads, then serves the read at its commit index.

**How it's tested.**
- One unit test per rule, including the paper's tricky cases.
- The simulator runs a whole group in one thread: real Raft nodes and LSM engines on simulated
  disks, over a network that loses, duplicates and delays messages. It cuts power, fails disks
  mid-write, and adds or removes members.
- The checker enforces the **four safety properties**: one leader per term; log matching; every
  committed entry in every later leader's log; and every replica applying the same commands in
  order.
- Mutation checks plant classic bugs to prove the tests catch them.

**Gate.** **5,000 varied seeds** pass: 1.1 million entries committed, 26,735 power cuts, 9,536
disk failures, 38,281 partitions and 20,104 membership changes, with zero violations. *Passed.*

**Lessons.** Random simulation alone missed two planted bugs (the Figure 8 rule, and a follower
committing entries it hadn't checked). Targeted unit tests and per-seed variation of settings
("buggify") cover them.

---

## Phase 5: Replicated KV service

**Goal.** Put Raft inside the KV server, so the service survives losing any minority of nodes and
stays **linearizable**: no client can tell it isn't one machine.

**What it builds.**
- **`ReplicatedKv`:** one replica.
  - Writes are proposed to the Raft log and answered once applied.
  - Reads go through ReadIndex.
  - Followers answer `NotLeader` with the leader's address.
  - All of its work runs through one serial mailbox (an actor), since `RaftNode` isn't
    thread-safe.
- **`KvStateMachine`:** applies committed entries on every replica. Deduplication moved here, at
  apply time, so every replica makes the same decision.
  - The applied index is saved in the same batch as the data.
  - Time travels *inside* log entries, so session expiry is the same everywhere.
- **`GrpcTransport`:** Raft messages between nodes, over a long-lived gRPC stream per peer.
- **`KvClient` across several nodes:** it follows redirects and moves on from dead nodes. The CLI
  accepts `--server a,b,c`.
- **`distdb node --id N --peers 1=…,2=…,3=…`:** a real cluster.
- **A linearizability checker** (`Linearizability`): Wing and Gong's search with Lowe's
  memoization, as in Porcupine. It is checked per key, and writes of unknown outcome may count as
  having happened or not.

**How it's tested.** `ReplicatedKvSimulation` runs 3 or 5 real replicas with 4 concurrent clients,
under partitions (including ones that cut clients off), power cuts aimed at the leader, disk
failures, stalled processes, and clocks that drift. Every operation is recorded. Afterwards:
- the history must be linearizable;
- no write may take effect twice;
- every acknowledged write must have taken effect;
- all replicas must hold identical data.

`ReplicatedClusterTest` runs three real processes and kills the leader outright.

**Gate.** **500 seeds:** 469,599 operations, all linearizable, through 2,398 power cuts, 969
disk failures and 2,897 partitions.
- Failover in the simulator: a new leader in 360 ms (median).
- With three real processes: writes resumed **1.1 s** after the leader was killed.

*Passed.* (This phase is not committed yet.)

**Lessons.**
- A leader reading locally (a lease read) is safe only if clocks behave. The simulator caught
  that bug only after it learned to stall a node's clock.
- A per-attempt timeout equal to the call's deadline lets one lost message waste the whole call.

---

## Phase 6: Multi-Raft and sharding *(planned)*

**Goal.** Scale out. Cut the key space into **ranges** `[start, end)`, each its own Raft group of
three replicas, with thousands of ranges per node. Ranges split as they grow and move to balance
load.

**What it will build.**
- **Range descriptors:** range id, start and end key, replicas, and an **epoch** made of two
  counters, one bumped on split or merge and one on membership change. Every request carries the
  epoch it was routed with; a stale one gets `EpochMismatch`. A request for a key that isn't here
  gets `RangeNotFound`.
- **A multi-Raft host:** one node runs many `RaftNode`s that share one engine and one transport.
  Messages carry a range id, and messages for many ranges to the same peer are batched into one
  RPC per tick. Raft state, the applied index and client sessions become per range. Each state
  machine sees only its key span, and snapshots cover only that span.
- **The placement driver (PD):** three processes forming their own Raft group (reusing the
  Phase 4 library). It holds the range directory and current leaders, and tracks node liveness
  from heartbeats. Its scheduler emits operations: add replica, remove replica, transfer leader,
  split.
- **Routing:** each client keeps a range cache (key → range → leader). On a miss, `NotLeader` or
  `EpochMismatch`, it asks the PD and retries.
- **Splits:** when a range is too big, its leader picks a middle key and proposes a split command.
  Every replica applies it at the same log index, creating the right-hand range with the same
  members. Making this atomic across crashes is one of the hardest parts of the project.
- **Rebalancing:** move a replica by adding a learner, letting it catch up, promoting it, then
  removing the old one. This needs leadership transfer (a `TimeoutNow` message in Raft).

**Concepts to learn.** Range vs hash partitioning; hot spots; metadata management; cache
invalidation by epochs; operating thousands of state machines per node; atomic splits; placement
and failure detection. *Read:* Kleppmann's DDIA chapters 5–6, and the TiKV multi-Raft documents.

**Gate.** Data keeps flowing while ranges split and move under faults, and the linearizability
checker still passes. **Milestone:** a sharded, replicated KV store.

---

## Phase 7: Distributed transactions *(planned)*

**Goal.** ACID transactions with **snapshot isolation** across any number of ranges, following
Google's Percolator (as TiDB does).

**What it will build.**
- **Timestamp oracle (TSO)** in the PD leader. It hands out strictly increasing 64-bit timestamps
  (physical milliseconds plus a logical counter) and persists a high-water mark a few seconds
  ahead through Raft, so a new PD leader never reissues one. Clients fetch timestamps in batches.
- **Three column families per key,** built on the Phase 2 MVCC keys:
  - **data** `key@start_ts` → value;
  - **lock** `key` → (primary key, start_ts, TTL);
  - **write** `key@commit_ts` → start_ts.
- **The commit protocol:**
  1. **Begin:** take a `start_ts`. Reads see the latest write with `commit_ts ≤ start_ts`.
     Writes are buffered on the client.
  2. **Prewrite:** pick one key as **primary**. For every key, fail if there is a newer write
     (write-write conflict) or another lock; otherwise write data and lock.
  3. **Commit:** take a `commit_ts`, then write the primary's write record and remove its lock,
     atomically. **That single write is the commit point.**
  4. Commit the secondary keys asynchronously.
- **Lock resolution:** a reader that meets a lock follows it to the primary. If the primary
  committed, the reader rolls the lock forward. If the lock's TTL expired uncommitted, it rolls
  the transaction back. No coordinator log is needed.
- **Garbage collection:** the PD tracks a safe point (the oldest timestamp any transaction may
  read), and older versions are removed.

**Concepts to learn.** ACID; isolation levels and their anomalies (dirty read, lost update, read
skew, write skew); MVCC; two-phase commit and why it blocks; why the primary key removes the
need for a coordinator; timestamps vs hybrid logical clocks. *Read:* the Percolator paper, and
DDIA chapters 7 and 9.

**Gate.**
- **The bank test:** concurrent transfers between accounts under faults never change the total,
  across thousands of seeds.
- **Anomaly tests:** snapshot isolation allows exactly the anomalies it should (write skew) and
  forbids the rest.

---

## Phase 8: SQL layer *(planned)*

**Goal.** Turn SQL into transactional key-value operations, so `psql` or JDBC can connect to any
node and run real queries.

**What it will build.**
- **Table-to-key mapping**, using the Phase 1 encoding so key order equals SQL order:
  - a row is stored at `/t/{table}/pk/{primary key}` → its other columns;
  - a secondary index entry at `/t/{table}/i/{index}/{columns}/{pk}`;
  - a unique index entry at `/t/{table}/i/{index}/{columns}` → the primary key.

  A row and its index entries are written in one transaction, so they never disagree.
- **Catalog:** table and index definitions stored as keys, cached on each node with a version
  number.
- **The pipeline:**
  - **parser:** hand-written recursive descent, Pratt parsing for expressions, an AST of sealed
    interfaces and records;
  - **binder:** names and types;
  - **planner:** logical, then physical plans;
  - **executor:** Volcano-style iterators, running on the gateway node.
- **SQL surface:** `CREATE TABLE`/`INDEX`, `INSERT`, `SELECT` with joins, `GROUP BY`,
  `ORDER BY`, `LIMIT`, `UPDATE`, `DELETE`, `BEGIN`/`COMMIT`/`ROLLBACK`.
- **pgwire:** the PostgreSQL wire protocol v3 (startup, simple query, then extended query for
  prepared statements), with one virtual thread per connection.

**Concepts to learn.** Relational algebra; mapping relations onto ordered key-value storage;
parsing techniques; name resolution; logical vs physical plans; join algorithms (nested loop,
hash, index lookup); aggregation and sorting; wire protocols. *Read:* the CMU 15-445 lectures on
query processing, and the CockroachDB paper.

**Gate.** `psql` connects to any node and runs joins and transactions. Results match PostgreSQL
on sqllogictest files. **Milestone:** a distributed SQL database.

---

## Phase 9: Distributed execution and the optimizer *(planned)*

**Goal.** Make queries fast by moving computation to the data, and by choosing good plans.

**What it will build.**
- **Rule-based rewrites:** predicate pushdown, projection pruning, constant folding.
- **Coprocessor pushdown:** filters, projections, partial aggregates and `LIMIT` run on each range
  leader, next to the data. Only results cross the network, and the gateway merges them (final
  aggregation, merge sort).
- **Statistics:** row counts, histograms, distinct counts (HyperLogLog).
- **A cost-based optimizer:** full scan vs index scan, join algorithm and join order (dynamic
  programming), with costs that include how many ranges and nodes a plan touches.
- **`EXPLAIN ANALYZE`:** shows which nodes and ranges each operator touched, and how long it took.
- *Stretch:* a distributed hash join with a shuffle (exchange) operator.

**Concepts to learn.** Query optimization; selectivity estimation; cost models; join ordering
(Selinger); partial and two-phase aggregation; data locality; instrumentation. *Read:* the TiDB
paper.

**Gate.** Pushdown shows a **measured** reduction in network bytes and latency on aggregation
queries.

---

## Phase 10: Hardening and benchmarks *(planned)*

**Goal.** Prove that the real binaries, on real networks and disks, survive what the simulator
models, and measure performance honestly.

**What it will build.**
- **A Docker Compose cluster and chaos suite:** kill nodes, partition with iptables, add latency
  with Toxiproxy, skew clocks. Then check invariants: the bank test, and the linearizability
  checker on recorded histories.
- **Observability:**
  - Prometheus metrics: Raft proposals, commit latency, elections, leader changes, ranges per
    node, compaction bytes, transaction conflicts and retries;
  - structured logs tagged with node, range and transaction ids;
  - a `/debug` page per node.
- **Benchmarks:**
  - YCSB-style workloads on the engine and the KV layer;
  - a TPC-C-style SQL workload (new-order, payment);
  - throughput and p99 latency at 1, 3 and 5 nodes;
  - recovery time after a leader crash;
  - before/after numbers for each optimization: Raft batching and pipelining, group commit, TSO
    batching, coprocessor pushdown.

  The benchmarks run on local Docker, then briefly on 3 to 5 cloud VMs.
- **The README:** an architecture diagram, a five-minute quick start (`docker compose up`, then
  `psql`), and benchmark charts with hardware details.

**Concepts to learn.** The gap between simulation and real systems; the Jepsen method; metrics
and percentiles; coordinated omission; profiling (JFR); Little's law; batching, pipelining and
group commit. *Read:* the Jepsen analyses of etcd, CockroachDB and TiDB.

**Gate.** The chaos suite passes, and the README has benchmark charts.

---

## How the phases fit together

- **Phases 1–2** make one node correct and testable.
- **Phases 3–5** make one replicated group correct over a real network.
- **Phase 6** multiplies that group into thousands.
- **Phase 7** coordinates many groups atomically.
- **Phases 8–9** put SQL on top.
- **Phase 10** proves it on real machines.

Each layer trusts only the guarantees of the one below, and each guarantee has a test that was
checked, with planted bugs, to actually fail when the guarantee breaks.

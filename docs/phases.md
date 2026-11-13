# DistDB: the ten phases explained

DistDB is a distributed SQL database built from scratch in Java 25. Data is split into ranges of
keys, each range is copied to three machines and kept identical by Raft, transactions span
ranges, and any node answers SQL from `psql`. It is built bottom-up in ten phases. Each phase
ends at a **gate**, a test that proves it works, and leaves a design note in `docs/design/`.

This document explains what each phase does, how it works, and what to learn from it.
**Phases 1–8 are built** (with their measured results); **phases 9–10 are the plan.**

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

## Phase 6: Multi-Raft and sharding

**Goal.** Scale out. Cut the key space into **ranges** `[start, end)`, each its own Raft group of
three replicas, many ranges per node. Ranges split as they grow and move to balance the load,
while clients keep reading and writing without noticing anything but a little latency.

**What it builds.**
- **Range descriptors:** range id, span, voters, learners, and an **epoch** of two counters:
  `version` (bumped by a split) and `conf_ver` (the log index of the last membership change).
  Requests carry the range id and version they were routed with; a node answers `RangeNotFound`
  or `EpochMismatch` when the client's picture is stale.
- **`Store`, a multi-Raft host:** one node runs many `RaftNode`s in one mailbox, over one LSM
  engine. Each range has its own Raft log, applied index and client sessions, and its state
  machine (`RangeStateMachine`) touches only its own span. Raft messages for many ranges to the
  same peer leave as one frame per round.
- **Splits through the log.** The leader proposes `Split(key, new id, expected version)`. Every
  replica applies it at the same index, in one atomic batch: the left range narrows, the right
  range is created with a copy of the client sessions and a fresh Raft state starting at index 5.
- **Replica lifecycle.** A message for an unknown range creates an uninitialized replica (how a
  node joins a range). A removed replica is never told by Raft, so members answer its messages
  with a `Removed` notice, and it destroys itself, keeping its term and vote and a tombstone.
- **Leadership transfer** in the Raft library (`TimeoutNow`), so a replica that must leave hands
  its leadership over first.
- **The placement driver (PD):** three processes running a Phase 5 `ReplicatedKv` group. It
  allocates range ids (conditional puts on a replicated counter), learns the directory and store
  liveness from heartbeats, and schedules operators: repair, add or remove replicas, split, move
  replicas for balance, transfer leaders.
- **Routing:** `KvClient` keeps a range cache (key → range → leader), asks the PD on a miss, and
  drops an entry on `RangeNotFound` or `EpochMismatch`. A scan walks the ranges in order.
- **Commands:** `distdb pd`, `distdb node --pd ... --founders ...`, `distdb ranges`, and
  `distdb kv --pd ...`.

**How it works, in one example.** Range 1 `[−∞, +∞)` lives on nodes 1, 2, 3. It grows past the
PD's limit, so the PD sends its leader `Split(r1, new id 7)`. The leader picks the middle key `m`
and proposes the split. Every replica applies it at, say, index 412: r1 becomes `[−∞, m)` at
version 2, and r7 `[m, +∞)` appears on all three nodes, with r1's sessions copied. r1's leader
campaigns for r7 at once. Later the PD sees node 4 empty and moves r7's replica from node 1 to node
4: add node 4 as a learner, which catches up from a snapshot; promote it; remove node 1 (handing
over the leadership first if node 1 led). Node 1 is not told; the next time it campaigns for r7, a
member answers `Removed`, and node 1 deletes its copy. Meanwhile a client that still routes key
`q` to r1 gets `EpochMismatch`, asks the PD, and goes to r7.

**Concepts to learn.** Range vs hash partitioning; hot spots; metadata that is a cache, and
epochs to tell it is stale; why splits must go through the log and be atomic; why a node may only
create or keep a replica it is a member of; why destroying a replica must keep its vote; why a
snapshot is "state as of an index"; leader leases vs leadership transfer; failure detection and
placement. *Read:* Kleppmann's DDIA chapters 5–6, the TiKV deep-dive on multi-Raft, CockroachDB's
range splits and replica GC.

**How it's tested.**
- **`ShardedKvSimulation` (the gate):** a 3-node PD and 4 or 5 data nodes, real stores on simulated
  disks, real routed clients. Faults: partitions that also cut clients and PD nodes, power cuts
  (also of the PD leader), disk failures, stalled clocks, peer-only partitions. On top, the run
  forces random splits and random replica moves. Afterwards: the history is linearizable per key;
  no write took effect twice (in any range); acknowledged writes took effect and "certainly failed"
  ones did not; every client made progress after the faults; the ranges tile the key space; every
  member of every range has the same descriptor and data; no node ever held overlapping replicas
  (checked every 100 ms during the run).
- **Unit tests:** `RangeStateMachineTest` (each split rule), `StoreTest` (routing, a split across
  three stores, a retry after a split, a move with removal and re-add, a leader asked to remove
  itself), and Raft tests for leadership transfer and for a learner that lost its log.
- **`ShardedClusterTest`:** seven real processes; the PD splits and spreads the ranges, a data
  node is killed, and every acknowledged write reads back.

**Gate.** **500 seeds:** 426,071 operations, all linearizable, while ranges split 5,144 times and
changed membership 123,400 times (31,559 replicas destroyed), through 2,641 power cuts (617 of
the PD leader), 1,280 disk failures and 2,699 partitions.
- With seven real processes, the PD split the first range into four and moved replicas onto the
  fourth node within seconds. Killing a data node lost no acknowledged write.
- Of ten bugs planted on purpose, five were caught by the simulation and four more by targeted
  unit tests. The last is a check that cannot fail today, and is kept as a safeguard.

*Passed.* (This phase is not committed yet.)

**The hardest bug.** A node removed from a range and added back had lost its log, and it voted
for a candidate that was still using a configuration from before the removal. That elected a
leader without entries the node had once acknowledged: two different committed entries at one
index. The fix: a replica created again does not vote until its log is as long as it was.

**Lessons.**
- A snapshot is the state at some index, with that index's span; a snapshot from before a split
  can never be installed next to the ranges the split created.
- A host that destroys replicas breaks an assumption Raft makes (followers never lose
  acknowledged entries). The host must make up for it, or teach Raft the new case.
- The PD is a cache with opinions. Nothing it says is trusted for safety; ranges check every
  operator against their own state.

---

## Phase 7: Distributed transactions

**Goal.** ACID transactions with **snapshot isolation** across any number of ranges, following
Google's Percolator (as TiDB does): a transfer debits one account and credits another, in two
ranges, and either both happen or neither does, whatever crashes.

**What it builds.**
- **A timestamp oracle (TSO)** in the PD leader: `physical_ms << 18 | logical`, strictly
  increasing, handed out in batches. The leader issues only below a limit it owns in the PD's
  replicated state (moved forward by conditional puts), and each batch first re-reads that limit
  linearizably, so a deposed leader can't issue a timestamp below one already issued.
- **Three column families per key**, inside every range's span, on Phase 2's `MvccKey`:
  data `key@start_ts`, lock `key`, write `key@commit_ts`. Splits, snapshots and replica removal
  carry them like any other data.
- **Percolator commands**, applied through each range's Raft log, all idempotent (no request ids):
  `Prewrite`, `Commit`, `Rollback`, `CheckTxnStatus`, and for GC `SetTxnFloor`, `ScanLocks`, `Gc`.
- **`TxnClient` and `Txn`** (`distdb-txn`): begin, get, scan, put, delete, commit, with retries for
  conflicts. The commit prewrites every key, takes a commit timestamp, commits the primary (the
  commit point), then the secondaries in the background.
- **Lock resolution:** a reader that meets a lock asks the lock's primary how the transaction
  ended, and rolls the lock forward or back. An expired lock is rolled back by whoever asks.
- **Garbage collection** by a GC worker on the PD leader: set a floor on every range (no
  prewrites below it), resolve every lock below it, then publish the safe point; ranges drop the
  versions nobody can read.

**How it works, in one example.** Transfer 10 from account A (range 1) to B (range 2). Begin:
`start_ts = 100`. Read A = 50 and B = 70 at 100. Commit: prewrite A (the primary) with 40 and B
with 80, each a lock plus data at 100. Take `commit_ts = 105`. Commit A: write record
`A@105 → 100`, lock removed. **The transfer is now committed**, even if the client dies this
instant. A reader at 110 that finds B's lock asks A's range: committed at 105. So it writes
`B@105 → 100` itself and reads 80. Had the client died before committing A, the reader would have
found A's lock expired, rolled A back (leaving a ROLLBACK record so a late prewrite is refused),
then rolled B back: no money moved.

**Concepts to learn.** ACID; isolation levels and their anomalies (dirty read, lost update, read
skew, write skew); MVCC; two-phase commit and why it blocks; why Percolator's primary key removes
the coordinator; why rollback records exist; timestamps vs hybrid logical clocks; why GC must
resolve locks first. *Read:* the Percolator paper, and DDIA chapters 7 and 9.

**How it's tested.**
- **`BankSimulationTest` (the gate):** Phase 6's cluster and faults (partitions, power cuts of
  data nodes and the PD leader, disk failures, stalls, forced splits and moves), with bank
  clients: transfers between random accounts, and audits (a read-only transaction reading every
  account). One transaction in twenty has its client killed after its prewrites, or right after
  its commit point. GC runs with a lifetime of 2 to 5 seconds. Every audit, and a final one, must
  find exactly the money the bank opened with, no account negative; replicas must converge and
  every client must make progress after the faults.
- **`TxnAnomalyTest`:** no dirty reads, no lost updates, no read skew, transactions see everything
  that committed before they began, a reader finishes a transaction whose client died after its
  commit point, and **write skew is allowed** (two transactions keep `x + y >= 1` on their own
  snapshots and break it together).
- **`PercolatorTest`:** each command's rules on one engine.

**Gate.** **2,000 seeds:** 144,429 transfers committed and 50,701 audits balanced, while
2,142 clients were killed mid-commit, ranges split 13,601 times and changed membership 369,687
times, and GC ran 16,843 rounds; through 10,496 power cuts (2,481 of the PD leader), 4,590 disk
failures and 10,556 partitions. No audit was ever off by a unit.
- Of seven bugs planted on purpose, the simulation caught all seven (two only after more than a
  hundred seeds), and the unit tests four of them.
- The runs found three bugs in Phase 6's replica lifecycle (stale stamps and orphaned replicas),
  not in the transaction code.

*Passed.* (This phase is not committed yet.)

**Lessons.**
- The primary key's write record *is* the transaction's outcome. Everything else (secondaries,
  readers' roll-forwards, rollback records) follows from protecting that one fact.
- GC can destroy information a protocol depends on: the primary's commit record. Hence the floor,
  then lock resolution, then the safe point.
- A version number on a message must describe when the message was made, not when it was sent.

---

## Phase 8: SQL layer

**Goal.** Turn SQL into transactional key-value operations, so that `psql` or JDBC can connect to
any node and run real queries, with PostgreSQL's answers.

**What it builds.**
- **Tables as keys**, in Phase 1's order-preserving encoding, so key order is SQL order:
  - a row is stored at `/{table}/1/{primary key}` → its other columns as `(column id, value)`;
  - a secondary index entry at `/{table}/{index}/{columns}/{primary key}`;
  - a unique index entry at `/{table}/{index}/{columns}` → the primary key.

  A row and its entries are written by one transaction. Uniqueness costs nothing extra: two
  inserts of one key both write it, and Phase 7 lets only the first commit. NUMERIC keys sort by
  value (sign, exponent, digits); a table without a primary key gets a hidden `rowid` from the
  TSO.
- **The catalog** in reserved keys (`/1/...`): descriptors, names, an id counter and a version.
  Each transaction reads the version in its own snapshot and reuses the node's cached descriptors
  when it matches.
- **Schema changes that cannot lose a writer:** `CREATE INDEX` adds the index WRITE_ONLY (writers
  maintain it, readers ignore it), then **fences** (every range's transaction floor goes to that
  commit, and every older lock is resolved, so no transaction with the old schema can commit), then
  backfills and publishes. `DROP INDEX` and `DROP TABLE` remove the definition, fence, then delete
  the keys.
- **The pipeline:**
  - **parser:** hand-written recursive descent, Pratt parsing with PostgreSQL's precedence, an AST
    of sealed records;
  - **binder:** scopes (correlated subqueries become outer references), PostgreSQL's types
    (`1.5` is NUMERIC, `'x'` is unknown until its context types it), aggregation checks;
  - **planner:** predicate pushdown; access paths (point get, index or key range, full scan);
    joins (hash, lookup, nested loop). No statistics yet, so it uses rules of thumb;
  - **executor:** Volcano iterators on the gateway node, all join kinds, hash aggregation, sort,
    set operations, `EXPLAIN`.
- **SQL surface:** `CREATE`/`DROP TABLE` and `INDEX` (`UNIQUE`, `PRIMARY KEY`, `NOT NULL`,
  `DEFAULT`), `INSERT` (multi-row, `SELECT`, `ON CONFLICT DO NOTHING`, `RETURNING`), `UPDATE`,
  `DELETE`, `SELECT` with every join, `GROUP BY`/`HAVING`, `DISTINCT`, `ORDER BY`, `LIMIT`/`OFFSET`,
  subqueries, CTEs, `VALUES`, `UNION`/`INTERSECT`/`EXCEPT`; `BEGIN`/`COMMIT`/`ROLLBACK`.
  Types: BOOLEAN, BIGINT, DOUBLE PRECISION, NUMERIC (with PostgreSQL's scale rules), TEXT/VARCHAR.
- **pgwire:** PostgreSQL's protocol v3, one virtual thread per connection: startup (TLS declined,
  trust authentication), simple query (psql), extended query (JDBC: prepared statements, text and
  binary values, portals with a row limit), error recovery to `Sync`.
- **`distdb node --sql HOST:PORT`** serves SQL from any data node.

**How it works, in one example.** `UPDATE accounts SET balance = balance - 10 WHERE id = 7`
arrives at node 2. The session begins a transaction (a TSO timestamp) and reads the catalog
version (one get; the descriptors come from the cache). The binder resolves `balance` and `id`,
and the planner turns `id = 7` into a point get of `/100/1/7`. The executor reads that row at the
snapshot and computes the new row. It writes the row (and any index entry whose key changed) into
the transaction's buffer, and commits: Percolator's prewrite, commit timestamp, primary commit.
The client gets `UPDATE 1`. Had another transaction updated row 7 since the snapshot, the prewrite
would have refused, and the session would have run the statement again (autocommit), or returned
40001 (in `BEGIN`).

**Concepts to learn.**
- Relational algebra, and mapping relations onto ordered key-value storage.
- Parsing: recursive descent and Pratt parsing.
- Name resolution, scopes and correlation; PostgreSQL's type resolution.
- Logical vs physical plans; access paths; join algorithms (nested loop, hash, index lookup);
  aggregation and sorting.
- Why a schema change is write skew under snapshot isolation (F1's online schema changes), and
  wire protocols.

*Read:* the CMU 15-445 lectures on query processing, the CockroachDB paper, and the F1 schema
change paper.

**How it's tested.**
- **sqllogictest files, with PostgreSQL as the oracle:** 11 hand-written files, plus 400 random
  queries from a fuzzer (filters, all joins, grouping, subqueries, set operations, NULLs). The
  expected results were recorded from a real PostgreSQL 18; DistDB must match them value for value
  (and SQLSTATE for SQLSTATE), on one node (`SqlLogicTest`) and on the simulated sharded cluster
  (`SqlLogicSimulationTest`).
- **`SqlBankSimulationTest` (the gate):** a bank in SQL on Phase 6's faulty cluster. Up to eight
  sessions on up to three gateways run multi-statement transfers, single-statement transfers, owner
  changes, audits and index audits. An admin session builds, drops and rebuilds a secondary index
  under that load (and tries a unique index that must fail). Each session is a fiber of the
  simulation, so their statements interleave deterministically. Checks: every audit holds the
  opening total; an index lookup always agrees with a full scan; afterwards every public index has
  exactly one entry per row; only the errors faults explain; replicas converge.
- **`SqlClusterTest`:** three PD JVMs and three data nodes with `--sql`. JDBC on three different
  nodes creates, joins, indexes and conflicts (40001); then `psql` runs a script.
- **`PgServerTest`:** PostgreSQL's JDBC driver against the wire protocol.
- **`SessionTest`:** plan choices, prepared statements, isolation across sessions, the fence.
- **`RowCodecTest`:** key order equals SQL order, as properties.

**Gate.** **1,000 seeds** pass (about 2.5 seconds per seed):
- **Money:** 15,345 transfers committed, 3,568 accounts opened and 2,488 closed. All 8,571
  audits found the opening total, and all 6,332 index audits agreed with a full scan.
- **Schema changes under load:** 1,303 index builds and 303 drops. Faults cut 392 builds short,
  and each was cleaned up.
- **Faults:** 10,571 power cuts, 10,463 partitions and 5,605 disk failures, with 11,749 splits and
  621,110 membership changes.
- **Errors sessions saw:** 6,889 serialization failures (conflicts and fences), 331 unknown
  outcomes and 3,872 timeouts. None of them ever cost a unit of money or an index entry.

The sqllogictest files hold 162 statements and 619 queries, and DistDB gives PostgreSQL 18's
answers to all of them, on one node and on the cluster. `psql` and JDBC work against real
processes.
- Of nine bugs planted on purpose, every one was caught: three by the bank simulation, seven by the
  unit tests and sqllogictest, one only by a targeted test (`FenceTest`).
- Two planted bugs first survived, which showed a gap in the workload (it never inserted) and a
  race too narrow for random runs.


*Passed.* (This phase is not committed yet.)

**Lessons.**
- A schema read is a read: under snapshot isolation, a writer with an old schema and a schema
  change are write skew. The transaction floor bounds old schemas, as leases do in F1.
- A reference implementation is a better oracle than expected results written by hand: it found
  three of my assumptions in the first run.
- Blocking code fits in a deterministic simulator as coroutines, never as nested event loops.

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

# DistDB

A distributed SQL database in Java 25, built from scratch as a learning project. Data is split into
ranges, and each range is replicated three ways with Raft. Percolator-style transactions give
snapshot isolation across ranges, and any node speaks the PostgreSQL wire protocol. (DistDB is a
placeholder name.)

## Status

| Phase | What                                                         | State   |
|-------|--------------------------------------------------------------|---------|
| 1     | Foundations: build, CI, determinism seams, key encoding      | done    |
| 2     | LSM storage engine with MVCC keys, crash-tested              | done    |
| 3     | Single-node KV server over gRPC, CLI client, safe retries    | done    |
| 4     | Raft library and the deterministic simulator                 | done    |
| 5     | Replicated KV service, linearizable, checked under faults    | done    |
| 6     | Multi-Raft and sharding: ranges, placement driver, splits    | done    |
| 7     | Percolator transactions, snapshot isolation                  | done    |
| 8     | SQL: parser, planner, executor, PostgreSQL wire protocol     | done    |
| 9     | Distributed execution and the cost-based optimizer           | done    |
| 10    | Hardening and benchmarks                                     | next    |

Phase 2's gate, run at full scale:

```bash
./gradlew :distdb-storage:test -Ddistdb.storage.modelOps=2000000 -Ddistdb.storage.crashSeeds=500
```

Phase 3's gate is `MultiProcessGateTest`: node and CLI as separate processes, then `kill -9` and
restart.

Phase 4's gate is `RaftSimulationTest`. Each seed runs a whole Raft group in one thread over a
lossy, partitioned network, with power cuts, failing disks and membership changes, and checks the
four safety properties, linearizable reads, and recovery once the faults stop. CI runs 100 seeds;
the gate is 5,000. `-Ddistdb.sim.raftSeed=S` replays one seed exactly:

```bash
./gradlew :distdb-sim:test --tests '*RaftSimulationTest' -Ddistdb.sim.raftSeeds=5000
```

Phase 5's gate is `ReplicatedKvSimulationTest`. Real replicas and real clients run under
partitions, power cuts, disk failures and stalled clocks. Every client operation is recorded, and
the history must be linearizable. No write may take effect twice or contradict what its client was
told, and replicas must converge. It also measures failover. `ReplicatedClusterTest` does the same
with three real processes over gRPC, killing the leader. CI runs 30 seeds; the gate is 500:

```bash
./gradlew :distdb-sim:test --tests '*ReplicatedKvSimulationTest' -Ddistdb.sim.kvSeeds=500
```

Phase 6's gate is `ShardedKvSimulationTest`. A placement driver of three nodes and four or five
data nodes run under the same faults (and power cuts of the PD leader), while the PD splits and
rebalances ranges and the run forces more splits and moves at random. The history must stay
linearizable across every split and move, no write may take effect twice in any range, and at the
end the ranges must tile the key space with identical replicas. `ShardedClusterTest` does it with
seven real processes. CI runs 20 seeds; the gate is 500:

```bash
./gradlew :distdb-sim:test --tests '*ShardedKvSimulationTest' -Ddistdb.sim.shardSeeds=500
```

Phase 7's gate is `BankSimulationTest`: the same sharded cluster, faults, splits and moves, running
bank transfers in transactions, while clients are killed mid-commit and GC drops old versions.
Every audit (one snapshot of every account) must hold exactly the money the bank opened with.
`TxnAnomalyTest` checks snapshot isolation anomaly by anomaly: no dirty reads, lost updates or
read skew; write skew allowed. CI runs 20 seeds; the gate is 2,000:

```bash
./gradlew :distdb-sim:test --tests '*BankSimulationTest' -Ddistdb.sim.bankSeeds=2000
```

Phase 8's gate has two halves. **Results match PostgreSQL:** the sqllogictest files in
`distdb-sql/src/testFixtures/resources/slt` (hand-written ones plus 400 random queries) hold the
results a real PostgreSQL 18 gave, and DistDB must give the same on one node and on the simulated
cluster (`SqlLogicTest`, `SqlLogicSimulationTest`). **Correct under faults:** `SqlBankSimulationTest`
runs a bank in SQL on the faulty sharded cluster, with a secondary index built and dropped under
load; every audit must balance and every index must match its table. `SqlClusterTest` runs real
processes with JDBC and `psql`. CI runs 10 seeds; the gate is 1,000:

```bash
./gradlew :distdb-sim:test --tests '*SqlBankSimulationTest' -Ddistdb.sim.sqlSeeds=1000
```

To record the expected results again from your own PostgreSQL (a scratch database: it drops and
recreates schema `public`), and regenerate the random queries:

```bash
./gradlew :distdb-sql:test --tests '*SqlLogicTest*' -Ddistdb.slt.fuzz=400 "-Ddistdb.slt.complete=jdbc:postgresql://localhost:5432/scratch?user=postgres"
```

Phase 9's gate is `PushdownGateTest`: a measured cut in network bytes and latency on aggregation
queries. A 20,000-row table spread over 40 ranges and 5 nodes, on the simulator with a wire model
(1–5 ms latency, 1 Gbit/s links, 1 µs per key read). Each query runs with pushdown off and on, and
must answer the same. Aggregations must move at least ten times fewer bytes and answer at least
twice as fast; they moved about 1% of the bytes and answered about 30 times sooner (numbers in
[`docs/design/0010-distributed-execution.md`](docs/design/0010-distributed-execution.md)):

```bash
./gradlew :distdb-sim:test --tests '*PushdownGateTest' -i
```

## Try it

```bash
./gradlew :distdb-server:installDist
```

A sharded cluster: a placement driver (PD) of three nodes, then data nodes. One terminal per
process. `--split-keys 50` makes the PD split ranges early, so you can watch it happen:

```bash
distdb-server/build/install/distdb/bin/distdb pd --data-dir /tmp/pd1 --id 1 --peers 1=127.0.0.1:7601,2=127.0.0.1:7602,3=127.0.0.1:7603 --split-keys 50
```
```bash
distdb-server/build/install/distdb/bin/distdb pd --data-dir /tmp/pd2 --id 2 --peers 1=127.0.0.1:7601,2=127.0.0.1:7602,3=127.0.0.1:7603 --split-keys 50
```
```bash
distdb-server/build/install/distdb/bin/distdb pd --data-dir /tmp/pd3 --id 3 --peers 1=127.0.0.1:7601,2=127.0.0.1:7602,3=127.0.0.1:7603 --split-keys 50
```

Nodes 1 to 3 found the first range; node 4 starts empty, and the PD moves replicas onto it:

```bash
distdb-server/build/install/distdb/bin/distdb node --data-dir /tmp/n1 --id 1 --listen 127.0.0.1:7701 --pd 127.0.0.1:7601,127.0.0.1:7602,127.0.0.1:7603 --founders 1,2,3
```
```bash
distdb-server/build/install/distdb/bin/distdb node --data-dir /tmp/n2 --id 2 --listen 127.0.0.1:7702 --pd 127.0.0.1:7601,127.0.0.1:7602,127.0.0.1:7603 --founders 1,2,3
```
```bash
distdb-server/build/install/distdb/bin/distdb node --data-dir /tmp/n3 --id 3 --listen 127.0.0.1:7703 --pd 127.0.0.1:7601,127.0.0.1:7602,127.0.0.1:7603 --founders 1,2,3
```
```bash
distdb-server/build/install/distdb/bin/distdb node --data-dir /tmp/n4 --id 4 --listen 127.0.0.1:7704 --pd 127.0.0.1:7601,127.0.0.1:7602,127.0.0.1:7603
```

The client asks the PD where each key lives. Write some keys, then list the ranges:

```bash
distdb-server/build/install/distdb/bin/distdb kv --pd 127.0.0.1:7601,127.0.0.1:7602,127.0.0.1:7603 put greeting "hello world"
```
```bash
distdb-server/build/install/distdb/bin/distdb ranges --pd 127.0.0.1:7601,127.0.0.1:7602,127.0.0.1:7603
```

SQL: start any data node with `--sql 127.0.0.1:5433` as well (node 1, say), then connect with
`psql` (or any PostgreSQL driver; there is no password):

```bash
psql -h 127.0.0.1 -p 5433 -U distdb -d distdb
```

```sql
CREATE TABLE accounts (id INT PRIMARY KEY, owner TEXT NOT NULL, balance INT NOT NULL);
INSERT INTO accounts VALUES (1, 'ann', 100), (2, 'bob', 50);
BEGIN;
UPDATE accounts SET balance = balance - 30 WHERE id = 1;
UPDATE accounts SET balance = balance + 30 WHERE id = 2;
COMMIT;
CREATE INDEX accounts_owner ON accounts (owner);
EXPLAIN SELECT * FROM accounts WHERE owner = 'bob';
```

Filters and aggregates run on the ranges, next to the data, and the gateway merges what they send
(Phase 9). `ANALYZE` gathers the statistics the planner costs plans with, and `EXPLAIN ANALYZE`
shows what each operator did: rows, time, ranges, nodes, requests and bytes:

```sql
ANALYZE accounts;
EXPLAIN ANALYZE SELECT owner, count(*), sum(balance) FROM accounts GROUP BY owner;
SET distdb_pushdown = off;  -- every row to the gateway, as in Phase 8
EXPLAIN ANALYZE SELECT owner, count(*), sum(balance) FROM accounts GROUP BY owner;
```

Every node with `--sql` sees the same tables. Kill any one node, PD or data, and the cluster keeps serving. A single replicated group without a
PD still works as in Phase 5: `distdb node --data-dir DIR --id N --peers 1=HOST:PORT,...`, used
with `distdb kv --server ...`; and without `--peers` a node is a group of one on `127.0.0.1:7700`.

There is no authentication, so keep nodes on localhost. The `distdb` script runs the JDK in
`JAVA_HOME`, which must be 25. The CLI's exit status tells a failed write (3) from one that may have
been applied (4).

## Build

You need JDK 25. Gradle finds it through toolchains, and the Gradle daemon is pinned to 25 in
`gradle/gradle-daemon-jvm.properties`, so `JAVA_HOME` can point anywhere for the build. Keep the
checkout **outside OneDrive and other synced folders**: sync clients turn build outputs into
placeholder files and break Gradle.

```bash
./gradlew build          # compile, check formatting, run all tests
./gradlew spotlessApply  # format the code (google-java-format)
```

## Layout

Modules follow the layers, and dependencies point only downward. An ArchUnit test in
`distdb-sim` enforces this, and also bans direct use of clocks, randomness, threads, sockets and
files in the core modules.

```
distdb-common   byte-comparable key encoding, ids
distdb-env      Clock, RandomSource, Transport, FileSystem, Scheduler + real implementations
distdb-storage  LSM engine                                  (Phase 2)
distdb-raft     Raft as a pure state machine, no I/O        (Phase 4)
distdb-rpc      protos, KvChannel/KvHandler, PdClient, gRPC (Phases 3, 5, 6)
distdb-kv       Store (many ranges), ReplicatedKv, RaftStore (Phases 3-6)
distdb-pd       placement driver and timestamp oracle       (Phases 6, 7)
distdb-client   KvClient: retries, redirects, range cache   (Phases 3, 5, 6)
distdb-txn      TxnClient, Txn, GC worker (Percolator)      (Phase 7)
distdb-sql      parser, binder, planner, executor, catalog  (Phases 8-9)
distdb-pgwire   PostgreSQL wire protocol                    (Phase 8)
distdb-server   the distdb binary: node, pd, kv, ranges     (Phases 3, 6, 8, 10)
distdb-sim      simulations: Raft, KV, sharding, bank, SQL bank (Phases 4-8)
distdb-bench    benchmarks                                  (Phase 10)
docs/design/    one design note per component
docs/journal.md learning journal
```

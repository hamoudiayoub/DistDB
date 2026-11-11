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
| 6     | Multi-Raft and sharding: ranges, placement driver, splits    | next    |
| 7–10  | Transactions, SQL, distributed execution, hardening          | planned |

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

## Try it

```bash
./gradlew :distdb-server:installDist
```

A three-node cluster, one terminal per node:

```bash
distdb-server/build/install/distdb/bin/distdb node --data-dir /tmp/d1 --id 1 --peers 1=127.0.0.1:7701,2=127.0.0.1:7702,3=127.0.0.1:7703
```
```bash
distdb-server/build/install/distdb/bin/distdb node --data-dir /tmp/d2 --id 2 --peers 1=127.0.0.1:7701,2=127.0.0.1:7702,3=127.0.0.1:7703
```
```bash
distdb-server/build/install/distdb/bin/distdb node --data-dir /tmp/d3 --id 3 --peers 1=127.0.0.1:7701,2=127.0.0.1:7702,3=127.0.0.1:7703
```

Then talk to it through any node; the client follows redirects to the leader. Kill the leader's
terminal and keep writing: after a short pause, another node takes over.

```bash
distdb-server/build/install/distdb/bin/distdb kv --server 127.0.0.1:7701,127.0.0.1:7702,127.0.0.1:7703 put greeting "hello world"
```
```bash
distdb-server/build/install/distdb/bin/distdb kv --server 127.0.0.1:7701,127.0.0.1:7702,127.0.0.1:7703 get greeting
```

Without `--peers`, `distdb node --data-dir DIR` is a group of one on `127.0.0.1:7700`. There is no
authentication, so keep nodes on localhost. The `distdb` script runs the JDK in `JAVA_HOME`, which
must be 25. The CLI's exit status tells a failed write (3) from one that may have been applied (4).

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
distdb-rpc      protos, KvChannel/KvHandler, gRPC transports (Phases 3, 5)
distdb-kv       ReplicatedKv, KvStateMachine, RaftStore     (Phases 3-5)
distdb-pd       placement driver and TSO                    (Phase 6)
distdb-client   KvClient: deadlines, retries, redirects     (Phases 3, 5)
distdb-txn      Percolator transactions                     (Phase 7)
distdb-sql      parser, planner, executor                   (Phases 8-9)
distdb-pgwire   PostgreSQL wire protocol                    (Phase 8)
distdb-server   the distdb binary: node, kv                 (Phases 3, 10)
distdb-sim      simulations, safety and linearizability checks (Phases 4-5)
distdb-bench    benchmarks                                  (Phase 10)
docs/design/    one design note per component
docs/journal.md learning journal
```

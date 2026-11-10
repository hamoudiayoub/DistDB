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
| 4     | Raft library and the simulator                               | next    |
| 5–10  | Replication, sharding, txns, SQL, hardening                  | planned |

Phase 2's gate, run at full scale:

```bash
./gradlew :distdb-storage:test -Ddistdb.storage.modelOps=2000000 -Ddistdb.storage.crashSeeds=500
```

Phase 3's gate is `MultiProcessGateTest` (node and CLI as separate processes, then `kill -9` and
restart). Its retry simulation takes more seeds with `-Ddistdb.sim.kvSeeds=N`, and replays one
with `-Ddistdb.sim.kvSeed=S`:

```bash
./gradlew :distdb-server:test :distdb-sim:test -Ddistdb.sim.kvSeeds=2000
```

## Try it

```bash
./gradlew :distdb-server:installDist
distdb-server/build/install/distdb/bin/distdb node --data-dir /tmp/distdb
```

In another terminal:

```bash
distdb-server/build/install/distdb/bin/distdb kv put greeting "hello world"
distdb-server/build/install/distdb/bin/distdb kv get greeting
distdb-server/build/install/distdb/bin/distdb kv scan
distdb-server/build/install/distdb/bin/distdb kv --help
```

The node listens on `127.0.0.1:7700` by default (`--listen` to change it). It has no
authentication, so keep it on localhost. The `distdb` script runs the JDK in `JAVA_HOME`, which
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
distdb-raft     Raft as a pure state machine                (Phase 4)
distdb-rpc      kv.proto, KvChannel/KvHandler, gRPC         (Phase 3)
distdb-kv       KvService: dedup, limits; replicas later    (Phases 3, 5)
distdb-pd       placement driver and TSO                    (Phase 6)
distdb-client   KvClient: deadlines, retries; routing later (Phases 3, 6)
distdb-txn      Percolator transactions                     (Phase 7)
distdb-sql      parser, planner, executor                   (Phases 8-9)
distdb-pgwire   PostgreSQL wire protocol                    (Phase 8)
distdb-server   the distdb binary: node, kv                 (Phases 3, 10)
distdb-sim      simulations, architecture tests
distdb-bench    benchmarks                                  (Phase 10)
docs/design/    one design note per component
docs/journal.md learning journal
```

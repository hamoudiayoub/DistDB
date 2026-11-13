# 0011 — Hardening, observability, benchmarks, and the shell

> Written while building Phase 10. Phases 1–9 were verified in a deterministic simulator. This phase
> asks the opposite question: do the real binaries, on a real network, real disks and a real clock,
> survive what the simulator models? It also measures performance honestly, and it gives the
> database a command line worth using. The parts to defend without notes are the chaos suite's
> checkers, coordinated omission, and group commit.

## Problem

The simulator is the strongest test here, but it tests a model:

- its network loses and delays messages, but it is not TCP, gRPC or Docker's bridge;
- its disk fails, but it is not a filesystem;
- its clocks drift, but processes share one machine and one kernel.

A real system can fail where the model is wrong. The Jepsen method answers that: run the real
system, inject real faults, record what clients saw, and check the history against the model of
correctness.

Performance claims need the same discipline. A benchmark that waits for each response before
sending the next request hides the queueing a real client would see (coordinated omission). A
number without its hardware means nothing.

## What gets built

### `distdb sql`: the shell

An interactive SQL client that speaks PostgreSQL's wire protocol (simple query), so it works
against any DistDB node and against PostgreSQL itself.

- **Line editing (JLine):** history saved across sessions, multi-line statements until `;`,
  syntax highlighting as you type, and completion of keywords, tables, columns and meta commands.
- **The prompt** shows the node and the transaction state, like psql: `distdb=>` when idle,
  `distdb*>` inside a transaction, `distdb!>` after a failed statement.
- **Results** come as tables with box-drawing borders. Numbers are right-aligned, NULLs dimmed,
  long values cut to the terminal's width. Each result has a footer with the row count and the
  time. Too wide a result switches to the expanded (one column per line) layout.
- **Plans:** `EXPLAIN` and `EXPLAIN ANALYZE` draw as colored trees. Operators are bold; the work
  done on the ranges and the measured numbers are highlighted.
- **Meta commands:**
  - `\dt`, `\d TABLE` (columns and indexes), `\di`;
  - `\ranges [TABLE]` and `\nodes` (the cluster as the placement driver sees it);
  - `\timing`, `\x`, `\i FILE`, `\conninfo`, `\pushdown on|off`, `\?`, `\q`.
- **Scripting:** `-c SQL`, `-f FILE` and piped input print plain output: no colors and no borders
  that a script would have to strip. `NO_COLOR` is honored.

The catalog the meta commands read comes from new SQL statements, so psql can use them too:

- `SHOW TABLES`, `SHOW COLUMNS FROM t`, `SHOW INDEXES FROM t`, `SHOW STATISTICS FOR t`;
- `SHOW RANGES [FROM TABLE t]` and `SHOW NODES`, answered from the placement driver.

### `distdb demo`

A whole cluster in one process, for a first look with no setup: three PD nodes and three data
nodes on localhost, in a temporary directory, with the shell attached.

### Observability

Every PD and data node gets an HTTP port (`--http HOST:PORT`):

- **`/metrics`:** Prometheus' text format. It covers Raft proposals, elections and leader changes;
  commit (apply) latency as a histogram; replicas and leaders per node; compaction bytes, WAL
  syncs and group-commit sizes; transaction conflicts, lock resolutions and retries; SQL
  statements and their latency.
- **`/debug`:** a human page: the node, its ranges and their Raft state, and the metrics in
  plain words.
- **`/debug/clock`:** chaos-only. It shifts the node's wall clock by an offset (see "Clock skew").

Logs become structured, one event per line with `key=value` fields, and carry the node, range and
transaction ids where they apply. A log line can then be filtered by range across every node's
log.

### Group commit

Today each synced write fsyncs the write-ahead log on its own, under the write lock: N concurrent
writers pay N fsyncs. With group commit:

1. A writer queues its batch.
2. The first writer in the queue becomes the leader. It appends every queued batch to the log,
   fsyncs once, applies them in order, and wakes the others.

The rule that makes it correct: no write is visible, and no writer returns, before the fsync that
covers it.

### Switches for before/after numbers

`--tune raft.batch=off,raft.pipeline=off,tso.batch=off,wal.group-commit=off` turns each
optimization off:

- `raft.batch=off`: one entry per AppendEntries;
- `raft.pipeline=off`: one AppendEntries in flight per follower;
- `tso.batch=off`: one PD request per timestamp;
- `wal.group-commit=off`: one fsync per synced write.

Pushdown already has `SET distdb_pushdown = off`.

### Benchmarks (`distdb bench`)

- **YCSB-style workloads** (A: 50% reads and 50% updates; B: 95/5; C: reads only), with Zipfian
  key choice.
  - **On the engine:** in-process, with synced writes.
  - **On the KV layer:** against a real local cluster of processes, at 1, 3 and 5 data nodes.
- **TPC-C-style SQL:** new-order and payment transactions over JDBC. The schema is TPC-C's, with
  the scale cut down to fit a laptop.
- **Recovery:** kill -9 a range's leader under load, and measure the time from the kill to the
  first successful write, and to throughput back at 90%.
- **Ablations:** each optimization off, then on.
- **How latency is measured:**
  - Open-loop: requests start on a fixed schedule, and latency counts from the scheduled start,
    so a stall is charged to every request it delays. That avoids coordinated omission.
  - Closed-loop runs, which wait for each response before the next request, give maximum
    throughput.
  - Percentiles come from a log-bucketed histogram (1% precision).

Results go to `docs/bench/*.json`. Charts are rendered from them as SVG and shown in the README,
with the hardware.

### Docker Compose cluster and the chaos suite

- **The cluster:** `docker compose up` starts three PD nodes and five data nodes. Three of the
  data nodes serve SQL, published on localhost ports.
- **The chaos suite** (`distdb chaos`) runs on the host. It drives clients through those ports
  and injects faults through Docker:
  - **process faults:** kill -9 and restart; pause and resume (SIGSTOP), which looks like a GC
    pause or a hung VM;
  - **partitions:** `iptables` rules inside the containers. A fault isolates one node, or cuts the
    cluster into majority and minority;
  - **latency:** `tc netem` delay and jitter on a node's interface;
  - **clock skew:** containers share the host's kernel clock, so skew is injected through each
    process's clock seam instead (`/debug/clock`), as Jepsen uses libfaketime. The TSO's physical
    time and every wall-clock read are shifted; monotonic time is not.
- **The checkers:**
  - **Bank:** transfers between accounts through all three gateways, with audits during the run
    and a final one after healing. Money must be conserved and every audit must balance.
  - **Register:** reads, writes and compare-and-sets on a few single-row registers through
    different gateways. Each operation is recorded with its invocation and completion times and
    its outcome: ok, failed, or unknown. The history is checked for linearizability with the
    Phase 5 checker. Single-key operations under Percolator with one TSO must be linearizable: an
    operation's timestamp is taken after it starts, so it is above every commit acknowledged
    before.

## What this phase cannot do here

- **Cloud VMs:** the plan said "briefly on 3 to 5 cloud VMs". This machine has no cloud account,
  and creating one is the user's call. Every number here is from one laptop, and the README says
  so.
- **Real power loss:** a killed container keeps the kernel's page cache, so a write that was never
  fsynced still survives. The simulator's disk (Phase 2) remains the test of fsync discipline,
  including for group commit.
- **"1, 3 and 5 nodes" on one machine** measures coordination overhead, not scale-out: the nodes
  share the same CPUs and disk.

## Verification (the Phase 10 gate)

**The chaos suite passed.** It ran in Docker (`distdb-bench chaos --duration 180 --seed 1`):
three PD containers and five data-node containers, driven from the host through the three SQL
gateways.
- **Faults (10 in 180 s):**
  - kill -9 of three data nodes (n4, n3, n1);
  - two kill -9s of the PD leader, each right after its two followers' clocks were set 4.2 s and
    6.6 s behind;
  - n5 cut off by iptables;
  - pd3 paused;
  - 150 ± 50 ms of latency on two pairs of nodes;
  - a 2.6 s skew on pd1.
- **Bank:** 1,111 transfers committed (453 attempts failed on conflicts or faults, as they may).
  All 203 audits during the run, and the final audit after healing, found exactly 20 accounts
  holding 2,000.
- **Registers:** 1,052 writes, 1,035 reads and 1 write with an unknown outcome make a
  2,096-operation history. It is linearizable.

**The benchmarks** are in the README, with charts, and in `docs/bench/*.json`. The machine: one
laptop, Intel Core i5-13420H (8 cores, 12 threads), 7.7 GB RAM, Micron 512 GB NVMe SSD,
Windows 11, Java 25. The headline facts:
- **The engine:** group commit takes fsynced YCSB-A from 1,382 to 15,581 operations a second at
  16 threads (11×), and p99 from 195 ms to 4 ms.
- **Writes are fsync-bound.** A write is fsynced in the Raft log and again when applied, about
  2 ms each on NTFS, on every replica. KV writes reach about 340–540 a second whatever the
  cluster size.
- **Reads:** 5,696 a second on one node, 4,267 on three, 2,186 on five. On one machine, more
  nodes means more coordination on the same CPUs, not more capacity.
- **Latency at a fixed 200 ops/s** (YCSB-A, open loop): p50 13–17 ms, p99 54–67 ms.
- **TPC-C** at reduced scale: 92 new-orders a minute, with every consistency condition holding.
  Each key is prewritten with its own Raft write; batching prewrites by range is the next
  optimization.
- **Recovery** after kill -9 of the busiest leader: below half throughput for 0.9 s, 90% after
  1.1 s. The slowest write took 4.8 s: requests already sent to the dead node wait for their
  deadline.
- **Ablations** (on ÷ off, 3 nodes):
  - TSO batching 10.3×;
  - pushdown 1.8× in latency, and 99.5% fewer bytes;
  - group commit 1.25×;
  - Raft pipelining 1.14×;
  - Raft batching 0.98×: no gain, since the disk, not the messages, is the bottleneck.

**Planted bugs:** all five were caught.

| | planted bug | caught by |
|---|---|---|
| B1 | group commit lets writers return before the fsync that covers them | `GroupCommitTest` (a crash loses returned writes) and `LsmCrashTest` |
| B2 | the shell's lexer ends a statement at a `;` inside a string | `SqlLexerTest` |
| B3 | the Prometheus histogram's buckets are not cumulative | `MetricsAndLogTest` |
| B4 | the timestamp oracle forgets its predecessor's limit when it takes over | the chaos suite (`--only skew-failover`): audits found 2,003 instead of 2,000, and a register read went back in time |
| B5 | `--tune raft.pipeline=off` changes nothing | `ObservabilityTest` |

B4 is the one only the real cluster could catch here. It needs a PD failover onto a node whose
wall clock is behind, and the containers' clocks only differ through the injected skew. The new
leader handed out timestamps below ones already used. Timestamp order then no longer matched the
real order of events, which snapshot isolation relies on: a read went back in time, and three
units of money appeared from nowhere.

**Tests added:**
- the shell: layouts, lexer, script mode, transaction status, errors;
- `SHOW` statements;
- group commit, including a crash test;
- tuning, the HTTP endpoint and the log format;
- the latency histograms and the Zipfian chooser.

## What remains

- The second fsync per write (at apply). The Raft log alone is enough for durability.
- Prewrites batched by range, one request per range instead of per key.
- A client that fails over at once when its connection to a leader breaks, instead of waiting for
  the deadline.
- Runs on separate machines, and on Linux, where fsync costs a fraction of what it costs here.

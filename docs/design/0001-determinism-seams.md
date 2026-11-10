# 0001 — Determinism seams

> Draft written while building Phase 1. Rewrite it in your own words once you have read the
> FoundationDB testing paper; the point of these notes is that you can defend every line.

## Problem

Distributed bugs come from rare interleavings of messages, timers, crashes and disk behaviour. A
bug seen once in a million runs is unfixable unless the run can be replayed exactly. Replay needs
every source of nondeterminism to be under the test's control.

## Decision

Everything nondeterministic goes behind an interface in `distdb-env`, from the first commit:

| Seam           | Real                         | Simulated (Phase 4, `distdb-sim`)               |
|----------------|------------------------------|-------------------------------------------------|
| `Clock`        | `SystemClock`                | virtual time advanced by the simulator          |
| `RandomSource` | `SplittableRandomSource`     | the same class, seeded with the run's seed      |
| `Transport`    | gRPC (with Raft, Phase 5)    | in-memory queues with drops, delays, partitions |
| `KvChannel`, `KvHandler` | gRPC unary calls (Phase 3, `distdb-rpc`) | a lossy in-memory channel (`KvRetrySimulationTest`) |
| `FileSystem`   | `LocalFileSystem`            | in-memory files that lose unsynced writes       |
| `Scheduler`    | `ExecutorScheduler`          | single-threaded event loop                      |

An ArchUnit rule (`distdb-sim/src/test/java/io/distdb/sim/ArchitectureRules.java`) fails the
build if core packages (storage, raft, kv, pd, client, txn, sql) call `System.nanoTime`,
`currentTimeMillis`, `Thread.sleep`, `Instant.now`, start threads, or use `Random`, sockets,
`FileChannel` or `Files` directly. `ArchitectureRulesTest` runs the rule against deliberate
violations so it can't pass vacuously while the core modules are still empty.

## Invariants

- Core code reaches the world only through the five seams.
- A `RandomSource` belongs to one component; `split()` hands out child streams. Splitting is
  deterministic, so one seed drives the whole cluster, and adding a component doesn't shift the
  other components' streams.
- Durability follows the `FileSystem` contract only: unsynced bytes may vanish or tear; creates,
  renames and deletes are durable only after `syncDirectory`. The simulated FS will enforce
  exactly this contract, no more.

## Choices worth defending

- **Transport is one-way and lossy** (`send` plus a handler), not request/response. That matches
  what a network really does, which is also what the simulator injects, and Raft wants exactly
  this.
- **Revised in Phase 3: KV requests got their own seam.** The first plan put request/response on
  top of `Transport`. Instead, `KvChannel` (client side, one attempt per call) and `KvHandler`
  (server side) sit in `distdb-rpc`. In production they are plain gRPC unary calls, so deadline
  propagation, per-call status and connection handling come from gRPC rather than being rebuilt
  over a byte pipe. The parts that must be deterministic still are: retries, backoff and request
  ids live in `KvClient` and use `Scheduler` and `RandomSource`, and the server logic lives in
  `KvService`. The simulator implements `KvChannel` itself (see `KvRetrySimulationTest`). The cost:
  two network seams instead of one, so the Phase 4 simulator needs to model both. See
  [0004](0004-kv-server.md).
- **`Scheduler` makes no ordering promise in production.** The simulator runs tasks one at a
  time; if core code silently relied on that, it would pass in simulation and race in production.
  So code that needs serialization (one Raft group) must queue its tasks itself.
- **Directory fsync is a no-op on Windows.** Windows can't open a directory as a channel, and
  NTFS journals metadata. On Linux it's a real `fsync` of the directory.

## Alternatives rejected

- Mocking time and randomness in individual tests: this doesn't compose into whole-cluster replay.
- Running real processes and injecting faults at the OS level only (Jepsen style): useful, and
  planned for Phase 10, but not replayable.

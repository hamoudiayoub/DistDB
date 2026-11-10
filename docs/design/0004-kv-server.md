# 0004 — Single-node KV server, RPC and retries

> Draft written while building Phase 3. Rewrite it in your own words; the parts to be able to
> defend without notes are the idempotency rules and "unknown outcome".

## Problem

Put the storage engine behind a network API that a client in another process can use, and make
that API safe to use over a network that loses things. Concretely:

- Get, Put, Delete and Scan over gRPC, plus a command-line client.
- **Every call has a deadline**, and the server doesn't do work nobody is waiting for.
- **A client can retry anything without corrupting data.** A write that's retried must take effect
  at most once, even if the server crashed and restarted between the attempts.
- **The client says honestly what happened.** A write that failed after the request may have
  arrived is not reported as "failed".
- The client logic and the server logic run unchanged in the simulator.

## The shape

```
 distdb kv put k v ─┐                                 ┌─ distdb node
                    │ KvClient (distdb-client)        │  GrpcKvServer (distdb-rpc)
                    │   deadline, retries, backoff,   │    gRPC deadline → Clock deadline
                    │   request ids, outcome          │  KvService (distdb-kv)
                    │ KvChannel ── GrpcKvChannel ═════╪═══> KvHandler
                    │   one attempt, a timeout        │    validate, dedup, apply, answer
                    └                                 │  LsmEngine (Phase 2)
```

| Piece | Where | Deterministic? |
|---|---|---|
| `kv.proto`: `Kv/Call(KvRequest) → KvResponse` | `distdb-rpc/src/main/proto` | data only |
| `KvChannel` / `KvHandler`: the request/response seam | `io.distdb.rpc` | interfaces |
| `GrpcKvChannel` / `GrpcKvServer` | `io.distdb.rpc.grpc` | no: real network |
| `KvClient`: the retry loop | `io.distdb.client` | yes (ArchUnit-checked) |
| `KvService`, `ClientSessions`, `KeySpace` | `io.distdb.kv` | yes (ArchUnit-checked) |
| `distdb node`, `distdb kv` | `io.distdb.server` | no: `main()` |

**One RPC method with a union of operations** (CockroachDB's `Batch` does the same), not one
method per operation. Deadlines, retries, request ids and errors then each have exactly one code
path on each side, and the simulator's channel has one method to fake.

**Why a separate request/response seam and not `Transport`?** See the Phase 3 revision in
[0001](0001-determinism-seams.md). In short: gRPC already does deadlines, status codes and
connection management for request/response, and Raft (one-way, batched) is a different shape of
traffic.

## Deadlines

- The client turns a timeout into a **deadline** on its own clock once, at the start of a call.
  All attempts and backoff waits fit inside it.
- Each attempt waits at most `min(time left, attemptTimeout)` (2 s), so one slow attempt doesn't
  use up the whole budget.
- The deadline crosses the network as **time remaining** (gRPC's `grpc-timeout` header). The
  server adds it to its own clock. Absolute times would be wrong, because clocks on two machines
  disagree.
- The server rejects work whose deadline has already passed (`DeadlineExceeded`), and a long scan
  checks the deadline as it goes.

## Typed errors drive the client

| Error | Meaning | Client does |
|---|---|---|
| `ServerBusy` | over `maxInflightRequests`; nothing done | back off, retry |
| `DeadlineExceeded` | the deadline passed before the work started | retry if time is left |
| `InvalidArgument` | the request can never succeed | fail at once |
| `StorageFailure` | the engine failed; the node is restarting | back off, retry |
| `StaleRequest` | a late copy of a write the client has finished with | never seen by a correct client |
| *no answer* (`RpcException`) | timeout, connection refused or broken | retry |

Retries use **exponential backoff with full jitter**: wait a random time in `[0, ceiling]`, with
the ceiling doubling from 20 ms up to 1 s. Without jitter, clients that failed together retry
together and fail together again.

## Idempotency: the request id and client sessions

Every write carries `RequestId(clientId, seq)`. `clientId` is 16 random bytes picked when the
client starts; `seq` counts that client's writes. **A retry reuses the id.** Reads carry nothing,
because repeating a read changes nothing.

Why even blind puts need this: A sends `put(x,1)`, the server applies it, the reply is lost. B's
`put(x,2)` succeeds. A retries, and without dedup `x` ends at 1. If C read `x = 1` before B's
write, no single order of the three operations explains what everyone saw: that's a
linearizability violation, caused only by a retry.

The server keeps a **session per client** in the engine (`ClientSessions`, after section 6.3 of
Ongaro's thesis):

```
0x01 's' clientId            → floor | lastActiveMillis
0x01 'r' clientId seq(BE)    → the response sent for that write
```

The rules, each of which the simulation catches being broken (see Verification):

1. **The write and its result go in the same `WriteBatch`**, which is atomic and synced. After a
   crash both exist or neither does, so a retry after a restart is still recognized. Writing the
   result separately lets a crash keep the record without the data, and the retry is then
   answered "OK" for a write that never happened.
2. **A retry is answered from the record**, never applied again.
3. **The floor.** Each write also says the lowest seq its client is still waiting for
   (`first_incomplete_seq`). Results below it are deleted, and anything below it that arrives
   later is a stray duplicate and gets `StaleRequest`. Without this check, a late duplicate whose
   result was already deleted would be applied a second time. The floor never goes down.
4. **Sessions expire** after an hour idle. That is safe only because clients stop retrying at
   their deadline, and `ClientOptions` caps deadlines at 5 minutes, far below the TTL. This is a
   contract between client and server, written down in both.

The client side has a subtle rule too: taking a new seq and marking it incomplete must be atomic
(`seqLock`). Otherwise a concurrent write could compute a floor above it, and the server would
reject it as stale.

Checking for a result and recording one happen under one write lock, so writes are serialized.
Reads take no lock.

## Unknown outcomes

A failed write is in one of two states, and the client reports which one
(`KvException.outcomeUnknown()`, CLI exit 4 versus 3):

- **Certainly not applied:** every attempt was answered with an error, or provably never sent.
- **Maybe applied:** at least one attempt might have reached the disk.

Two details make "certainly" true rather than hopeful:

- **`maybeDelivered` on transport failures.** `GrpcKvChannel` attaches a `ClientStreamTracer` to
  each call. It sees `outboundMessage` only once the request is handed to a real connection. A call
  that fails before then (connection refused, or deadline hit while still connecting) was never
  sent. The first version matched `ConnectException` in the cause chain instead, and reported
  "maybe applied" for a call that timed out while connecting.
- **`StorageFailure.maybe_applied`.** If `engine.write` throws, the batch can still be on disk
  (the append succeeded and the fsync failed, say), and it reappears after the restart. The server
  says so, and the client counts it as maybe applied.

## Other decisions

- **Key space split** (`KeySpace`): client data under `0x02`, the node's own records under `0x01`.
  A client scan can't see sessions, and a client key can't collide with them.
- **Scans are paged:** at most `limit` entries (default 1000, max 10 000) or about 1 MiB per page,
  but always at least one entry. A page ends with `resume_key = lastKey + 0x00`. Each page is a
  consistent snapshot; the pages together aren't (MVCC timestamps fix that in Phase 7). Scan
  bounds may be one byte longer than the key limit, or a scan could never continue past a
  maximum-length key.
- **Every write is synced before it is acknowledged.** Throughput is one fsync per write; group
  commit is the known fix.
- **Crash-only on storage errors.** After any `StorageException` the service refuses everything,
  and `distdb node` halts the process (exit 70). Restart is the recovery path, and it's the
  well-tested one.
- **Overload is refused, not queued:** beyond 1024 requests in flight, new ones get `ServerBusy`.
- **One node per data directory**, enforced with an OS file lock on `DIR/LOCK`. The engine lives
  in `DIR/engine`.
- **Listen on localhost by default.** There's no authentication or TLS.

## Verification (the Phase 3 gate)

- **`MultiProcessGateTest` (the gate):** starts `distdb node` as its own OS process, runs the CLI
  as a separate process per command, and also drives 200 concurrent writes from a library client.
  Then it kills the node outright (like `kill -9`), restarts it on the same directory, and checks
  that every acknowledged write survived. It also checks that a second node can't open a directory
  that is in use.
- **`KvRetrySimulationTest`:** 4 clients, 100 seeds, all on one thread. The network loses 10% of
  requests and 10% of replies, duplicates 5%, and delays some by seconds; one call in ten has a
  short deadline. The power is cut every few seconds, and a third of process lives get a disk that
  fails mid-operation. A wrapper around the engine counts how often each request id actually took
  effect, including failed writes found on disk after the restart. The property: **at most once
  for every write, exactly once for every acknowledged write, zero for every write reported as
  certainly failed.** 100 seeds give about 10 000 acknowledged writes, 1 400 power cuts and 1 800
  retries answered from the record, and the run fails if any kind of fault never happened. The
  same seed always gives the same totals. **Mutation checks:** disabling the record lookup, the
  stale check, the same-batch rule, or the client's "maybe delivered" bookkeeping each makes it
  fail on seed 1 or 7.
- **`KvServiceTest`** (dedup, floors, GC, expiry, crash-then-retry, paging, limits, busy,
  fail-stop), **`KvClientTest`** (backoff ceilings and jitter, request-id reuse, floors, outcome
  classification, scan paging), **`GrpcKvTest`** (real sockets: deadline propagation, timeout
  after delivery, refused, timed out while connecting, server bug, 500 concurrent calls), and
  **`CliTest`** (byte notation round-trips any bytes, usage errors, exit codes).

## Not yet (deliberately)

- Group commit, so concurrent writers share an fsync.
- Session registration (the Raft thesis' full scheme). A client silent for longer than the TTL
  that then retries can be applied twice. The deadline cap makes that impossible for a correct
  client.
- TLS, authentication, metrics, a debug page.
- Multi-key atomic batches in the API, and consistent multi-page scans.

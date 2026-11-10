# Learning journal

One entry per bug that took more than an hour: symptom, root cause, what it taught you.

---

## 2026-11-08 — Gradle fails inside OneDrive ("not a regular file")

- **Symptom:** `:build-logic:compilePluginsBlocks` failed with "Unable to delete directory", then
  with "Cannot snapshot ... not a regular file".
- **Cause:** the project lived under `OneDrive\Desktop`. OneDrive turns freshly written build
  outputs into cloud placeholder files (NTFS reparse points), and Java NIO doesn't report those
  as regular files.
- **Fix:** keep the repository outside OneDrive.
- **Lesson:** the file system's real behaviour is part of your environment. That is exactly why
  `FileSystem` is a seam with a written contract.

---

## 2026-11-08 — Crash test "lost" synced writes; the bug was in the simulator

- **Symptom:** `LsmCrashTest` seed 1, cycle 22: recovery came back at sequence 2062, but batches
  up to 2067 had been acknowledged with `sync = true`.
- **Cause:** not the engine. `SimFileSystem` tears unsynced pages at crash time, and its "zero this
  sector" choice also zeroed bytes that had been synced. A log's synced end is rarely
  page-aligned, so the page being appended to holds both synced and unsynced bytes. Real disks
  write a sector atomically: it holds either its old or its new contents, and both contain the
  synced bytes. Zeros can only appear in space that was never synced. The simulator's own test
  missed this because its synced data happened to be exactly one page long.
- **Fix:** a torn sector now keeps synced, unchanged bytes. A new regression test
  (`syncedBytesSharingAPageWithUnsyncedOnesSurvive`) syncs a non-aligned prefix. It fails against
  the old code, checked by putting the bug back.
- **Lesson:** a fault injector that is *stricter than the contract* produces false alarms; one that
  is *looser* hides real bugs. The simulator needs its own tests against the contract, with
  unaligned sizes. And when a test fails, ask which side of the test is wrong before "fixing"
  the code under test.

---

## 2026-11-10 — "Certainly not applied" was a guess, twice

- **Symptom 1:** while designing `KvRetrySimulationTest`, I had to decide what counts as a write
  "taking effect" when `engine.write` throws. That exposed a lie in the protocol: the server
  answered `StorageFailure`, documented as "this attempt changed nothing". But a failed write can
  be on disk (the log append worked and the fsync failed), and recovery brings it back. A client
  that got only `StorageFailure` answers would report the write as certainly failed.
- **Fix 1:** `StorageFailure.maybe_applied`, set when the failure struck during this request's
  `engine.write`. The simulation checks it: the engine wrapper records each failed write's last
  sequence number, and after the restart, recovery's `lastSequence()` says whether the batch
  survived. Phase 2's prefix property makes that all or nothing.
- **Symptom 2:** `CliTest` expected a put to a node that isn't running to exit 3 ("failed"), and
  got 4 ("may have been applied"). On a cold JVM, the 500 ms deadline ran out while gRPC was still
  connecting, so the attempt ended as a timeout, and timeouts counted as "maybe delivered". The
  channel only recognized "never sent" from a `ConnectException` in the cause chain.
- **Fix 2:** a per-call `ClientStreamTracer`. gRPC calls `outboundMessage` only once the request
  is handed to a real connection, so a call without it was never sent, whatever the error. A test
  against a TCP listener that never speaks HTTP/2 pins this down.
- **Lesson:** "unknown outcome" is the default, and every "certainly not" needs a proof. Each
  proof (an error the server returns before touching the disk, a request that never left) has to
  be checked, or the client reports false certainty, and that's worse than reporting ignorance.

---

## 2026-11-10 — The protobuf plugin broke Spotless in one module

- **Symptom:** `:distdb-rpc:spotlessJava` failed to configure: "Cannot set the value of task
  property 'taskService' ... loaded with ... project-distdb-rpc ... using a provider ... loaded
  with ... project-distdb-bench".
- **Cause:** `distdb-rpc` applied the protobuf plugin with a version in its own `plugins {}`
  block. That gives the module its own buildscript classloader, so the Spotless classes it sees
  differ from those of the build service registered by its siblings.
- **Fix:** load every third-party plugin through `build-logic` (the protobuf plugin is now a
  `build-logic` dependency, applied by a `distdb.protobuf-conventions` plugin), so all modules
  share one classloader.
- **Lesson:** in a multi-module build, apply plugins from one place.

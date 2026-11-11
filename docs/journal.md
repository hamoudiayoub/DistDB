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

---

## 2026-11-11 — Leader and follower ping-ponged empty AppendEntries forever

- **Symptom:** 13 of 17 Raft unit tests failed with the test cluster's "messages never stop"
  guard, right after the first election.
- **Cause:** after any acknowledgement, the leader called `replicateTo`, which sent an
  AppendEntries even when it had no new entries. The follower acknowledged that one too, and the
  leader answered again, forever.
- **Fix:** an empty AppendEntries has exactly two legitimate uses: probing for the match point,
  and carrying a new commit index (after a commit, or when a heartbeat answer shows a follower is
  behind). `replicateTo` now takes `allowEmpty`, and each call site says which case it is.
- **Lesson:** in a message-driven protocol, "answer every message with a message" loops. Each send
  needs a reason that eventually runs out. The test harness's message budget turned a hang into a
  clear failure.

---

## 2026-11-11 — Random simulation missed two real Raft bugs

- **Symptom:** to prove the simulator can catch bugs, I put eight classic Raft mistakes back in,
  one at a time. Five failed within the first 45 seeds. Two survived 500 seeds each:
  - committing earlier-term entries by counting replicas (the Figure 8 rule);
  - a follower committing up to its own last index instead of the last entry the leader just
    verified.
- **Cause:** both need scenarios random faults almost never build.
  - Figure 8 needs about three elections with crashes at exact moments, while a new leader's
    no-op is still unreplicated.
  - The stale-suffix case is rare here because the conflict-term hint jumps straight to the
    divergence point, and a conflict truncates the whole suffix.
  Fixed knobs (64 entries per message, always 3 voters) made both rarer still.
- **Fix:** knobs now vary per seed (3 or 5 voters, 1 to 64 entries per message, 1 to 32 in
  flight, gentle or harsh faults: FoundationDB's "buggify"). Even so, both bugs need targeted
  unit tests, which now exist and catch them:
  - `aLeaderCommitsOnlyEntriesOfItsOwnTermByCounting`;
  - `aFollowerCommitsOnlyWhatTheLeaderHasJustVerified`.
- **Lesson:** "thousands of seeds pass" only means something once you've checked which bugs the
  seeds can catch. Mutation testing turns that question into a table. Simulation finds the bugs
  you didn't think of; targeted tests are for the ones you did.

---

## 2026-11-11 — Persist, then send: shown, not just stated

- **What:** the simulator's host loop was changed (as an experiment) to send a Ready's messages
  before persisting it. Seed 45 then failed leader completeness. A follower acknowledged entries,
  its disk failed before writing them, and it came back without them. The leader had already
  counted it towards a majority, and a later leader was elected without a committed entry.
- **Lesson:** the Ready ordering rule isn't style. An acknowledgement or a vote is a promise, and
  a promise must be on disk before anyone hears it.

---

## 2026-11-11 — The simulator's liveness check was wrong, not Raft

- **Symptom:** seed 268 failed liveness: a node "never applied" the entry proposed after the
  faults stopped, yet its commit index equalled the leader's.
- **Cause:** that node caught up by installing a snapshot that already included the entry, so it
  never applied it individually. The check looked for the entry being applied, not for the state
  machine having reached its index.
- **Fix:** record the marker's index when any node applies it, and require every member's state
  machine to have reached that index.
- **Lesson:** same as the Phase 2 simulator bug. Checkers are code too, and when one fails, ask
  first which side is wrong.

---

## 2026-11-11 — Builds collided with the IDE's own Gradle build

- **Symptom:** builds failed at random inside `build-logic` ("Unable to delete directory",
  "is not a directory", "Could not add file ... to ZIP").
- **Cause:** VS Code's Java extension runs its own Gradle daemon. When files change, it rebuilds
  the `build-logic` included build at the same moment, in the same `build-logic/build` directory.
- **Workaround:** a wrapper that retries the build, clearing `build-logic/build` (pure output)
  when the collision happens. The real fix is to stop the IDE building at the same time, or to
  give it its own build directory.

---

## 2026-11-11 — A lease-read bug the simulator could not see, until clocks could stall

- **Symptom:** to test the Phase 5 gate, I made the leader serve reads from its local state
  without ReadIndex's heartbeat round (a lease read with no lease). The linearizability checker
  passed 100 seeds, and then 100 more with per-node clock drift.
- **Cause:** a stale read needs a deposed leader that still believes it leads while a new leader
  commits a write. In the simulator, check-quorum always deposed the old leader within its own 10
  to 20 ticks, before the others (who wait at least as long, then run pre-vote) could elect
  anyone. Client timing narrowed the window further: the competing write first timed out at the
  old leader. With clocks that run at the same rate, the lease is safe, which is exactly the
  assumption lease reads rely on.
- **Fix (to the simulator):** two faults that real systems have:
  - stalled processes, whose timers run 4–8 times slow for a while, like a long GC pause;
  - a partition that cuts the leader off from its peers but not from its clients.

  The checker then caught the bug on seed 7, with a read that returned a value already
  overwritten by a completed write. The correct code (ReadIndex) passes, because it never trusts
  time.
- **Lesson:** a simulator only finds bugs in the failure modes it models. "Clocks are perfect" is
  a hidden assumption, and the most dangerous kind, since some correctness arguments (leases)
  depend on it. When a planted bug survives, ask what the world is missing, not whether the bug
  is real.

---

## 2026-11-11 — One lost message ate a client's whole deadline

- **Symptom:** the failover benchmark failed liveness: after the faults stopped, one client did
  nothing for 3 seconds while the cluster was healthy and the other client worked.
- **Cause:** found by tracing that client's attempts. Its per-attempt timeout (2 s) equalled its
  deadline (2 s). One request to a follower was lost (2% loss), so that single attempt waited out
  the entire deadline. That happened twice in a row, which covered the quiet period. A first
  suspicion turned out wrong: an exception swallowed inside a future callback, which would also
  have silenced the client. The harness now records those, so it can't happen unnoticed.
- **Fix:** attempts much shorter than the deadline (400 ms within 2 s in the simulator; the
  production defaults are 2 s within 5 s).
- **Lesson:** retrying needs time left to retry in. The ratio of attempt timeout to deadline is a
  real design parameter, not a default to leave alone.

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

---

## 2026-11-11 — Learners that could never be installed: snapshots from before a split

- **Symptom:** the first sharded simulation runs ended with learners that never caught up. The
  final check found replicas that stayed uninitialized for seconds, with their range's leader
  healthy and sending.
- **Cause:** a leader sends the snapshot from its last log compaction, and with compaction every
  1000 entries that snapshot could be the range's very first one, from before several splits. Its
  span then covered keys that other ranges own now, and on the learner's node those ranges had
  replicas. The store refuses a snapshot that overlaps another replica, rightly: installing it
  would have overwritten their data. So the snapshot was refused forever.
- **Fix:** a range compacts its log as soon as it applies a split, so the snapshot it sends always
  carries the current span. Mutation S7 puts the bug back.
- **Lesson:** a snapshot is not "the state", it is "the state at some index", and everything about
  it (here, the span) is as of that index. Anything that checks a snapshot against the present has
  to account for how old it is.

---

## 2026-11-11 — A range created on a node that was no longer a member

- **Symptom:** in the same runs, a node held a replica of a range whose descriptor did not list it.
  Nobody ever sent it anything, and its span blocked other ranges' snapshots there.
- **Cause:** a node removed from a range, and later added back, receives the log it missed: its
  removal, whatever happened while it was out, and the re-add. It applies all of it in order. When
  that included a split, the node created the right-hand range with the members of that moment,
  which did not include it. Since the new range does not count it as a member, it
  never campaigns and never hears from anyone, so no `Removed` notice ever reaches it.
- **Fix:** a split creates the new range on a node only if the node is one of its members (the
  same rule CockroachDB uses); otherwise it deletes that part of the data. Mutation S9.
- **Lesson:** every "create" needs to ask "should this exist here?", not just "can I build it?".

---

## 2026-11-11 — Raft assumed followers never lose their logs; a store broke that on purpose

- **Symptom:** seed 36 of 200: learners still uninitialized at the end, but this time nothing
  overlapped them. The leader reported their match index as 25 and 80, though their logs were
  empty.
- **Cause:** a node removed from a range keeps an orphan replica (nobody tells it). When the PD
  added it back as a learner, the leader probed it and the orphan acknowledged entries up to 25.
  Then a `Removed` notice arrived from a member that had not yet heard of the re-add (in answer to
  an earlier pre-vote of the orphan), and the store destroyed the replica. The leader's match
  index for it stayed at 25, so the leader probed just above 25 forever, and the empty replica
  refused each probe. Raft never lowers a match index, because a correct follower never loses an
  acknowledged entry.
- **Fix, twice:**
  - in Raft, a rejection that shows the follower's log is now shorter than its match index lowers
    the match index, so the leader catches it up again (a new unit test removes a learner's log);
  - in the store, a replica that has heard from a leader or candidate whose configuration is at
    least as new as a `Removed` notice ignores the notice: it is being added back.

  Neither is a safety issue. A learner's acknowledgements never count, and a voter can't be
  destroyed this way: it would need a configuration in its log at least as new as the one that
  removed it.
- **Lesson:** the assumptions of a library become the host's obligations. Destroying replicas is
  outside Raft's model, so the host has to restore what Raft expects, or teach Raft the new case.

---

## 2026-11-11 — The simulator recorded the wrong request ids

- **Symptom:** the first sharded run failed at once: "acknowledged write null never took effect".
- **Cause:** the Phase 5 harness learned each write's request id from its first attempt, which
  `KvClient` sent synchronously. With range routing, the first attempt waits for the PD's answer,
  so the id was not known yet when the harness read it.
- **Fix:** the harness derives the id itself: a client numbers its writes 1, 2, 3, ... in the
  order they are issued.
- **Lesson:** a harness that leans on timing details of the code under test breaks when they
  change. Derive what you can from the contract.

---

## 2026-11-11 — The simulators could hide any exception as a "disk failure"

- **Symptom:** none yet, which was the problem. While chasing a real-process failure, I noticed
  that any exception thrown inside a store's mailbox goes through the "storage failed" path, and
  both simulators counted that as a disk fault and restarted the node. A logic bug (an
  `IllegalStateException` in a split, say) would have looked like bad luck with disks, and the run
  would have passed. In a real process the same exception halts the node.
- **Fix:** the simulators now fail the run unless the failure is a `StorageException` on a disk
  that the simulator really made fail. No run since has stopped on such an exception, so nothing
  had been hidden.
- **Lesson:** a fault injector must check that each failure it absorbs is one it caused.
  Otherwise "the system survived a fault" and "the system crashed" look the same.

---

## 2026-11-11 — Seven JVMs on one machine, and 10 ms ticks

- **Symptom:** `ShardedClusterTest` failed once in a full build: a write got `NotLeader` for
  range 1 for its whole 60 s deadline. Run alone, it passed, but the first writes took 18 to 20 s.
- **Cause:** two things. Eight cold JVMs (three PD nodes, four data nodes, the test) start on one
  machine; the PD answered nothing for the first seconds, and stores learn each other's addresses
  only from the PD. And stores ticked every 10 ms, so a range calls an election after 100 to
  200 ms without hearing its leader. On a machine also running the simulations, heartbeats were
  late more often than that, and elections followed one another.
- **Fix:** stores now default to 50 ms ticks (elections after 0.5 to 1 s). A store ticks every
  range it holds, so ticks also cost CPU per range; TiKV and CockroachDB use far longer ones. The
  test now waits for the cluster to form before timing writes, with generous deadlines. Once
  formed: 200 writes in 3.7 s, four ranges after 6.8 s.
- **Lesson:** timeouts are a bet on the slowest machine you will run on, not the average one.

---

## 2026-11-11 — Two committed entries at one index: a destroyed replica voted again

- **Symptom:** the first 500-seed gate run failed at seed 231. At the end, two replicas of range
  r3 held different descriptors, and the leader could not commit past index 10. A per-range check
  added for the hunt (every replica must apply the same entry at each index) then pinpointed it:
  index 8 was a membership change on three replicas and a no-op on a fourth.
- **Cause:** with the trace of r3:
  1. Index 8 promoted n4, and committed with n3's acknowledgement.
  2. Index 10 removed n3. A member's `Removed` notice later made n3 destroy its replica; its log
     was gone, and its term and vote stayed.
  3. The PD added n3 back as a learner, and the leader sent it the range's initial snapshot,
     whose membership still says voters {1, 2, 3}.
  4. n2 had stalled at index 7, so it still used the old configuration {1, 2, 3}. It campaigned,
     and n3, with a log ending at 5, found n2's log complete enough and voted for it. Two of
     {1, 2, 3} is a majority, so n2 led and committed something else at index 8.

  The design note's argument said that once a removal is committed, the removed node's copy of
  the log no longer matters. That holds only if the node never votes again with less than it
  had. A candidate on an older configuration still counts it.
- **Fix:** the tombstone records how long the log was when the replica was destroyed. A replica
  created again neither votes nor campaigns until its log is that long; then it holds every entry
  it ever acknowledged, because it caught up from a leader. (CockroachDB gives a re-added replica
  a new id instead, so old configurations do not count it.) The 500-seed gate then passed.
- **Lesson:** "delete the data, keep the vote" was not enough: a vote is only safe if it is cast
  with at least the log behind every promise made before. Also, the end-of-run checks only saw a
  symptom; checking the core property continuously found the moment it broke. That check stays in
  the gate.

---

## 2026-11-12 — A message stamped with the configuration that removed its receiver

- **Symptom:** while checking planted transaction bugs, two runs failed for an unrelated reason:
  a learner that never initialized. A 100-seed bank run then reproduced it without any planted
  bug (seed 33). The node held an orphan replica of the old range, with a stale, wider span, that
  blocked the learner's snapshot. The orphan kept campaigning, the members kept answering
  `Removed`, and it ignored them.
- **Cause:** Phase 6's guard against stale notices. A replica ignores a `Removed` notice if a
  leader whose configuration is at least as new has sent to it, because that means it is being
  added back. The store stamped every outgoing message with the sender's configuration index *at
  the moment the batch was sent*. A leader had queued a heartbeat to n4, then appended the change
  that removed n4 (index 69), then sent the batch: n4 received a message stamped 69. Its own
  removal had become its proof of membership.
- **Fix:** a message carries the configuration index only if that configuration counts its
  receiver, and 0 otherwise. Planted back as mutation S11.
- **Also found in the same hunt:** an orphan whose old configuration's members no longer host the
  range at all gets no answer from anyone. A replica that has heard from no leader for 3 seconds
  now asks the PD who the range's members are, and probes them; they answer `Removed` under the
  usual rule (the PD only says whom to ask).
- **And once more (the 2,000-seed gate, seed 823):** an orphaned *learner* was never probed.
  Learners never campaign, so it kept believing in its old leader, and the probe fired only for
  replicas with no leader. The probe now fires for any replica that no leader has sent to for 3
  seconds, whatever it believes.
- **Lesson:** a timestamp or version attached to a message must describe the moment the message
  was made, not the moment it left. Batching moved the two apart.

---

## 2026-11-12 — The bank's convergence check raced with GC

- **Symptom:** bank seed 8 failed the final check: one replica of a range held one more version
  of an account than the other two.
- **Cause:** not a bug. That replica was one entry behind, and the entry was a GC command: the GC
  worker kept raising the safe point during the settle period, and each new safe point is a log
  entry on every range. The comparison needs a quiet cluster.
- **Fix:** the run stops the GC workers along with the clients, before it compares replicas.
- **Lesson:** "converged" is only defined once nothing more is being written. Every background job
  that writes (GC, session expiry, rebalancing) has to be stopped before the check.

---

## 2026-11-12 — Transactions need the oracle twice, and partitions show it

- **Symptom:** the first bank runs committed few transfers during the fault period: clients sat
  idle for seconds at a time, with "no timestamp in time".
- **Cause:** not a bug. Every transaction takes two timestamps, and each needs a linearizable
  read in the PD group to prove the PD node still owns the timestamp range. A partition that cuts
  a client, or the PD leader, off from a PD majority stops every transaction of that client. A
  single-key write, in contrast, only needs the PD when its range cache misses.
- **Lesson:** the timestamp oracle is a single point of *availability* (never of safety). Systems
  built on Percolator work around it: batching (done here), caching a lease, or hybrid logical
  clocks instead of a central oracle (CockroachDB's choice).

---

## 2026-11-12 — A schema change is write skew: an index lost a writer's row

- **Symptom:** none yet, by design: the hazard was found on paper, then planted to see the gate
  catch it. With `CREATE INDEX` as one transaction (add the index, scan the table, write the
  entries), the SQL bank simulation failed at seed 21: "for owner o1 the index found 3 rows, a full
  scan 4".
- **Cause:** a writer that started before the index existed read the old schema and inserted a
  row without an entry; the index's transaction read the table before that row committed. They
  wrote different keys, so snapshot isolation let both commit. The writer read the schema and wrote
  rows; the schema change read rows and wrote the schema: write skew.
- **Fix:** F1's idea with Phase 7's tool. The index is first added WRITE_ONLY (writers maintain
  it, readers ignore it) and committed at `C1`. Then a **fence**: every range's floor goes to
  `C1` and every lock below it is resolved, so no transaction that started with the old schema can
  still commit. Only then the backfill, and the index goes PUBLIC. `TxnClient.fence` is the GC
  worker's floor-and-resolve walk, now shared.
- **Lesson:** under snapshot isolation, "read A, write B" against "read B, write A" is a race, even
  when one side is the schema. Leases (F1, CockroachDB) bound how long an old schema lives; DistDB
  already had a bound, the transaction floor, and only had to use it.

---

## 2026-11-12 — A rolled-back CREATE TABLE poisoned the catalog cache

- **Symptom:** `transaction.test` failed: after `BEGIN; CREATE TABLE made_in_txn ...; ROLLBACK`,
  the next transaction's `CREATE TABLE kept` succeeded, but its `INSERT INTO kept` said the
  relation did not exist.
- **Cause:** catalog versions were a counter, and the cache was keyed by version. The rolled-back
  transaction had read its own uncommitted version `v+1` and cached its snapshot (with the table
  that never was). The next schema change also produced `v+1`, and found the stale snapshot.
- **Fix:** a version is now the start timestamp of the transaction that wrote it (unique, and
  increasing in commit order since two schema changes conflict on the version key), and a
  transaction never caches a version it wrote itself.
- **Lesson:** a cache key must name one state. "Version 5" named two different catalogs, one of
  which was never committed.

---

## 2026-11-12 — PostgreSQL as the oracle: three differences it caught at once

Recording the sqllogictest files from a real PostgreSQL 18 and running them on DistDB found, in the
first run:

- **NUMERIC lost its scale on disk:** `7` stored in a `NUMERIC(6,2)` column read back as `7`,
  not `7.00`. Row values reused the key encoding, which strips trailing zeros so that `1.0` and
  `1.00` are one key. Values now keep the scale; keys still don't.
- **`position('l' IN 'hello')` was a syntax error:** the first argument was parsed as a full
  expression, so `IN` became the IN operator. It now stops below IN's binding power.
- **`SELECT 1 UNION SELECT 'x'` gave the wrong error:** the select list typed `'x'` as text at
  once, so the union failed with 42804 (types do not match). PostgreSQL keeps it unknown until the
  union types it as integer, and fails with 22P02 (not a number). Untyped literals now stay
  UNKNOWN until the query's output is final.
- **Lesson:** writing expected results by hand would have encoded my own assumptions. A reference
  implementation finds the assumptions.

---

## 2026-11-12 — `scan(limit 0)` meant "no limit" in one layer and "nothing" in another

- **Symptom:** every sqllogictest file passed on the in-memory store, and the first one failed on
  the simulated cluster: `CREATE TABLE t` succeeded, then `INSERT INTO t` said "relation does not
  exist".
- **Cause:** the catalog loads every descriptor with `scan(start, end, 0)`. The SQL layer's
  `KvTxn` contract says 0 means no limit; Phase 7's `Txn.scan` returned the first 0 entries.
- **Fix:** the adapter translates 0 to "unlimited".
- **Lesson:** a sentinel value is a contract between two layers, and only one of them had read
  it. The in-memory store implemented the SQL layer's version of the contract, so only the real
  cluster could find this.

---

## 2026-11-12 — Three lost messages in a row on a fault-free cluster

- **Symptom:** on the simulated cluster with no faults injected, one scan in `aggregate.test`
  failed with "deadline exceeded after 3 attempts: no answer", on every run.
- **Cause:** not a bug. The simulated network drops 2% of messages even when the test injects no
  faults. This scan's request was dropped, then both retries' replies (traced at the leader: it
  answered each in 3 ms). A one-in-a-hundred-thousand event, reproduced exactly because the run is
  deterministic.
- **Fix:** Phase 7 reports such a failure as FAILED: "nothing answered in time; no write happened".
  The SQL layer now marks it retryable, so an autocommit statement runs again, as for 40001.
- **Lesson:** "safe to retry" is a property of what happened (nothing was written), not of the
  error's code. The error carries it as a flag.

---

## 2026-11-12 — Waiting by running the world starved the sessions

- **Symptom:** the first SQL bank runs did almost nothing: 12 transfers in 3 seeds, for 198 power
  cuts.
- **Cause:** a design flaw in the simulation. Each session's statement waited by running the
  simulated world, and the world included other sessions' statements, which then ran *nested
  inside* the first one's wait. A session whose answer had come could not return until every
  statement started on top of it finished, and they kept starting new ones.
- **Fix:** `SimFibers`. Each session is a thread that runs only while the simulation's thread waits
  for it, so one thread runs at a time and the run stays deterministic. A session that waits for a
  future hands control back. When the future completes, a simulation task hands control to the
  session again. Sessions now interleave at every wait, with no nesting.
- **Lesson:** blocking code can live inside a deterministic simulator, but only as coroutines,
  never as nested event loops. With one stack for many logical threads, the last one to start
  decides when the others finish.

---

## 2026-11-12 — A unique index under construction already enforces uniqueness

- **Symptom:** SQL bank seed 28 failed: an owner change got 23505 from `accounts_owner_unique`,
  an index the admin session was building at that moment (and which was bound to fail, as owners
  repeat).
- **Cause:** not a bug. A WRITE_ONLY unique index is maintained by writers, and maintaining a unique
  entry means checking it is free. PostgreSQL behaves the same way: an unfinished `CREATE UNIQUE
  INDEX CONCURRENTLY` keeps enforcing uniqueness on new writes.
- **Fix:** the simulation accepts 23505 from that index (and from no other).
- **Lesson:** the states of a schema change are visible to every writer. "Writers maintain it" for
  a unique index includes "writers may be refused".

---

## 2026-11-12 — A planted bug survived because the workload never did that

- **Symptom:** planting "inserts skip WRITE_ONLY indexes" (P2) broke nothing: unit tests and 60
  SQL bank seeds all passed. So did planting "the fence raises floors but resolves no locks"
  (P10).
- **Cause:** for P2, a gap in the workload: the bank only *updated* rows, and updates maintain
  every index correctly. No insert ever ran while an index was being built. For P10, a narrow race:
  an old transaction must lock its keys before the floor reaches its range, then commit after the
  backfill's snapshot, and sixty seeds never lined that up.
- **Fix:** the bank now opens and closes empty accounts (inserts and deletes), and seed 8 caught
  P2. `FenceTest` now states the fence's contract directly: a transaction that prewrote before the
  fence must not commit after it. It caught P10 at once.
- **Lesson:** a planted bug that survives says what the tests never do. Two remedies: widen the
  workload when the gap is an operation it skips; write a targeted test when the gap is timing
  that random runs rarely line up.

---

## 2026-11-12 — Projection pruning broke a correlated EXISTS

- **Symptom:** with pushdown on, `subquery.test` line 32 answered wrongly. The query was a
  correlated `EXISTS` whose subquery compares to a column of the outer table. The remote scan sent
  that column as NULL, so the subquery never matched.
- **Cause:** pruning keeps the columns the plan above reads, using `columnsRead`, which walks an
  expression's column references. A conjunct that cannot run on the ranges (it holds a correlated
  subquery) stays in a Filter on the gateway, and its subquery reads the row through outer
  references. `columnsRead` does not descend into subqueries, so those reads were invisible, and
  the columns were pruned.
- **Fix:** when a conjunct kept on the gateway holds a correlated subquery, the remote scan keeps
  every column. Planted bug Q5 (pruning ignores what the gateway's filter reads) now checks it.
- **Lesson:** NULLing a column is silent: nothing fails until something reads it. A "what does
  this read" analysis has to see through every way of reading, outer references included. And an
  oracle with a few correlated queries found what my own tests did not.

---

## 2026-11-12 — A latency gate on a network with no bandwidth

- **Symptom:** a first look at measuring the gate showed three problems with the simulator as it
  was.
  - Its network had latency (1–5 ms) but no bandwidth: a 3 MB response arrived as fast as a 30-byte
    one, so bytes could not cost time.
  - `Cluster.await` advanced time in 10 ms steps, so latencies were rounded up to the next 10 ms
    at every wait. A plain scan waits about 120 times per query, so it would have been charged up
    to about a second of rounding.
  - The fault-free cluster still lost 2% of client messages, and a 40-range fan-out sends 80, so
    most fan-outs would have waited for a retry timeout. The latency would have measured timeouts.
- **Fix:** a wire model, `ShardedKvSimulation.Wire`, used only by the measurement cluster:
  - a bandwidth per node, with each inbound link carrying one message at a time;
  - a read cost per key on the range;
  - no losses.

  `await` now runs one task at a time and stops at the task that completes the future, so the
  clock reads the exact completion time. Every other run keeps `Wire.NONE`, and their seeds replay
  unchanged.
- **Lesson:** a measurement is only as good as the model of what it measures. The decomposition
  also matters. In this model, the 30× latency gain comes from round trips: 119 sequential pages
  become one parallel wave. The transfer of 3 MB at 1 Gbit/s is only 24 ms of the 1.39 s. Bytes
  are a separate win, which grows as the network gets slower or busier.

---

## 2026-11-12 — A cost-based planner changed plans two tests had pinned

- **Symptom:** after statistics and costs, `SessionTest` expected `remote scan branch b filter
  name = 'south'` and got `index scan branch b on branch_name_key`. Later, `SqlClusterTest`
  expected an index scan on a four-row table and got one filtered scan on the table's range.
- **Cause:** not a bug. `name` is UNIQUE, so `name = 'south'` matches at most one row: one get
  (cost about 2) beats scanning the table on its ranges (about 3). The rule-based planner never
  compared the two.
- **Fix:** both tests now check what they were written for. The first still checks that each
  side's conjunct sits below the join, on its own side, and expects the index lookup. The second
  checks that an index built through one node is used and maintained through another. It uses a
  unique index, which a lookup is estimated to reach in one row, and expects a duplicate inserted
  through another node to fail with 23505.
- **Lesson:** once a planner chooses by cost, a plan test tests the costs too. Pin the property
  the test is about (where a predicate runs), and expect the operator only where the costs are
  clear.

---

## 2026-11-12 — Two planted bugs no test could see: an offset and a join order

- **Symptom:** two planted bugs passed everything: the unit tests, sqllogictest in memory and on
  two clusters, the gate, and 30 SQL bank seeds.
  - Q7: a pushed limit forgets the offset.
  - Q8: the join-order search keeps the costliest plan.
- **Cause:**
  - **Q7:** a limit is pushed only when no sort sits above it. Every `LIMIT ... OFFSET` in the
    oracle files has an `ORDER BY`, because PostgreSQL leaves the order undefined without one. So
    the oracle never reached the pushed limit.
  - **Q8:** a bad join order still gives the right answer, and the join-order test checked only
    the answer.
- **Fix:** two `SessionTest` cases.
  - One compares a pushed `LIMIT 3 OFFSET 4` with pushdown off, and with known rows: our scans
    return rows in key order, so the result is defined here.
  - One asserts the plan of a three-table join written from its biggest table: a lookup join into
    the big table, and no scan of it.
- **Lesson:** an oracle checks answers, not choices. A decision made only for speed needs a test
  that pins the decision. And a reference implementation cannot check what it leaves undefined.

---

## 2026-11-13 — Group commit only groups if the leader lets go of the lock

- **The trap (caught while designing, before any code ran):** the obvious sketch keeps the write
  lock while the group's leader writes and fsyncs the log. Concurrent writers would then wait on
  the lock instead of joining the queue. When the leader finished, the next writer would find an
  empty queue and lead a group of one: no grouping at all.
- **Why:** a writer can only join the next group if it can queue while the current group is being
  written. That means the leader must release the lock during the I/O. Releasing it opens a
  second problem: `close()` takes the same lock, and could close the log under the leader.
- **What was built:** LevelDB's design.
  - Writers queue on a condition.
  - The head of the queue writes every queued batch with the lock released.
  - A separate log lock orders the leader's I/O against `close()`.
  - The leader then applies the group in order, publishes the new sequence, and wakes the others.

  On this laptop's NVMe drive (YCSB-A with fsynced writes, 4 threads) it took the engine from 898
  to 4,125 operations a second.
- **Lesson:** batching needs a place to wait that is not the critical section. The rule that keeps
  it correct fits in one line: nothing is visible, and nobody returns, before the fsync that
  covers it. `GroupCommitTest` crashes the simulated disk after concurrent writers return, and
  checks that every returned write survives.

---

## 2026-11-13 — A write costs two fsyncs, and on Windows an fsync costs 2 ms

- **Symptom:** a single put on a one-node cluster took 5.8 ms at the median on localhost. The
  server's own commit-latency histogram said 4.3 ms of that was inside the node.
- **Cause:** the node's metrics showed 2,606 fsyncs for 1,301 writes. Each write is fsynced
  twice: once as a Raft log entry (it must be durable before it counts toward the quorum), and
  once when the state machine applies it. NTFS's `FlushFileBuffers` on this laptop takes about
  2 ms.
- **Not fixed (yet):** the second fsync is not needed for safety. The log is durable, so applied
  state can be rebuilt by replaying it. Dropping it requires storing the applied index with the
  data, and replaying past it on restart. That changes the recovery contract of Phases 5 and 6,
  which the simulator would have to re-check. Recorded as the top item for later.
- **Lesson:** count the fsyncs before tuning anything. On real disks they set the floor for
  write latency. A metric that is cheap to add (fsyncs per write) explained the number at once.

---

## 2026-11-13 — Why every latency number here comes from an open loop

- **The trap:** coordinated omission. A closed-loop client sends its next request only when the
  last one answers. During a stall, a leader crash say, each client has one request outstanding,
  and every request it would have sent meanwhile is never sent, so never measured. The benchmark
  slows down with the system and records a handful of slow requests instead of hundreds. Its p99
  then looks fine across an outage.
- **What was built:** `Runner.open`, an open loop. Requests start on a fixed schedule whatever
  happens, and each one's latency counts from when it was supposed to start, so a stall is charged
  to every request it delayed. Closed loops remain, for finding maximum throughput only. The
  latency charts and the recovery timeline are all open-loop.
- **Lesson:** a latency percentile is only meaningful together with the load model that produced
  it. Ask "measured from when?" before trusting any p99.

---

## 2026-11-13 — The shell showed every operator at 100% of the time

- **Symptom:** the first `EXPLAIN ANALYZE` tree in the shell drew a full time bar on every
  operator.
- **Cause:** each operator's time includes its inputs' time: a project over a merge over a
  remote scan reports 26 ms three times. Bars of inclusive time all reach the top.
- **Fix:** each bar shows the operator's own time, its time minus its inputs', as a share of the
  statement's total, so the bars answer "where did the time go?".
- **Lesson:** inclusive and exclusive time answer different questions. Profilers show both for
  this reason, and a display has to say which one it draws.

---

## 2026-11-13 — An open loop above capacity measures the queue, not the system

- **Symptom:** the first full benchmark run reported a 12.2 s "unavailability" after killing a
  leader, and KV p50 latencies of 2–3 s at "1,000 ops/s".
- **Cause:** both open loops offered more than the cluster could take. The recovery run sent 800
  writes a second, and the latency run's 50% writes made 500 a second, against a capacity of
  about 340 writes a second on this disk. Requests queued from the first second. In the recovery
  timeline, the five seconds before the kill were already almost all empty 100 ms buckets, so the
  "outage" was the backlog.
- **Fix:** rates below capacity: 150 writes a second for recovery, 200 operations a second for
  latency. Then recovery came out at 0.9 s below half throughput, about Raft's election timeout,
  and p50 at 13–17 ms. The recovery metric also uses a 500 ms moving window, because batched
  writes complete in bursts.
- **Lesson:** before reading an open-loop result, check that the system kept up before the event
  you are measuring. Measure capacity first (closed loop), then latency well below it.

---

## 2026-11-13 — Deleting obsolete files deleted a table being born

- **Symptom:** a full build failed once in `LsmEngineTest.concurrentUseOnTheRealFileSystem`: a
  scan threw `NoSuchFileException` for `000025.sst`, a table in the version it held.
- **Cause:** `deleteObsoleteFiles` took the live set (tables of live versions, plus pending
  outputs) and then listed the directory. A flush on another thread could allocate table 25,
  create it and install it between the two steps: the listing saw the file, the live set did not
  name it, and it was deleted. Group commit, which lets more threads run during a write, made the
  window easier to hit.
- **Fix:** list the directory first, then take the live set. A table in the listing exists, so
  its number was pending before the file was created, and it stays pending until a live version
  holds it.
- **Lesson:** a "what is in use" snapshot only protects what existed when it was taken. Take it
  after the listing it filters, the way LevelDB holds its mutex across both.

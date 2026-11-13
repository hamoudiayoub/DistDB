package io.distdb.kv;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.distdb.common.NodeId;
import io.distdb.env.RandomSource;
import io.distdb.env.Scheduler;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.Transport;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.raft.RaftConfig;
import io.distdb.raft.RaftNode;
import io.distdb.rpc.KvHandler;
import io.distdb.rpc.proto.DeleteRequest;
import io.distdb.rpc.proto.Error;
import io.distdb.rpc.proto.GetRequest;
import io.distdb.rpc.proto.KeyValue;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.PutRequest;
import io.distdb.rpc.proto.RequestId;
import io.distdb.rpc.proto.ScanRequest;
import io.distdb.rpc.proto.ScanResponse;
import io.distdb.storage.EngineSnapshot;
import io.distdb.storage.KvEngine;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import io.distdb.storage.StorageException;
import io.distdb.storage.WriteBatch;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * {@link ReplicatedKv} groups on simulated disks and an in-memory network, all on one simulated
 * thread. Most tests use a group of one (the single-node server), the rest a group of three.
 */
class ReplicatedKvTest {

  private static final Path DIR = Path.of("/node");
  private static final EngineOptions SMALL =
      new EngineOptions(16 * 1024, 1024, 10, 32 * 1024, 4, 12, 128 * 1024, 10);
  private static final byte[] ALICE = id(1);
  private static final byte[] BOB = id(2);

  // ---------------------------------------------------------------------------------------------
  // Basic operations (a group of one)

  @Test
  void putGetDeleteRoundTrip() {
    Group g = Group.of(1);
    assertThat(g.get("k")).isEmpty();
    assertThat(g.put(ALICE, 1, 1, "k", "v1").hasPut()).isTrue();
    assertThat(g.get("k")).contains("v1");
    g.put(ALICE, 2, 2, "k", "v2");
    assertThat(g.get("k")).contains("v2");
    assertThat(g.delete(ALICE, 3, 3, "k").hasDelete()).isTrue();
    assertThat(g.get("k")).isEmpty();
  }

  @Test
  void scanPagesFollowResumeKeysToTheEnd() {
    Group g = Group.of(1);
    for (int i = 0; i < 2500; i++) {
      g.put(ALICE, i + 1, i + 1, key(i), "v" + i);
    }
    List<String> seen = new ArrayList<>();
    byte[] start = new byte[0];
    int pages = 0;
    while (true) {
      ScanResponse page = g.scan(start, null, 0).getScan();
      pages++;
      page.getEntriesList().forEach(kv -> seen.add(kv.getKey().toStringUtf8()));
      if (!page.hasResumeKey()) {
        break;
      }
      assertThat(page.getEntriesCount()).isEqualTo(1000);
      start = page.getResumeKey().toByteArray();
    }
    assertThat(pages).isEqualTo(3);
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 2500; i++) {
      expected.add(key(i));
    }
    assertThat(seen).isEqualTo(expected);
  }

  @Test
  void scanRespectsBoundsAndLimit() {
    Group g = Group.of(1);
    for (int i = 0; i < 20; i++) {
      g.put(ALICE, i + 1, i + 1, key(i), "v");
    }
    ScanResponse page = g.scan(utf8(key(5)), utf8(key(9)), 0).getScan();
    assertThat(keys(page)).containsExactly(key(5), key(6), key(7), key(8));
    assertThat(page.hasResumeKey()).isFalse();
    page = g.scan(utf8(key(5)), utf8(key(9)), 2).getScan();
    assertThat(keys(page)).containsExactly(key(5), key(6));
    assertThat(page.getResumeKey().toByteArray()).isEqualTo(utf8(key(6) + "\0"));
    assertThat(g.scan(utf8(key(9)), utf8(key(5)), 0).getScan().getEntriesCount()).isZero();
  }

  @Test
  void scanPageStopsAtItsByteBudgetButAlwaysMakesProgress() {
    KvServiceOptions d = KvServiceOptions.defaults();
    Group g =
        Group.of(
            1,
            new KvServiceOptions(
                d.maxKeyBytes(),
                d.maxValueBytes(),
                d.defaultScanLimit(),
                d.maxScanLimit(),
                100,
                d.maxInflightRequests(),
                d.sessionTtl(),
                d.sessionSweepInterval()),
            1000);
    g.put(ALICE, 1, 1, "a", "x".repeat(500));
    g.put(ALICE, 2, 2, "b", "x".repeat(33));
    g.put(ALICE, 3, 3, "c", "x".repeat(33));
    g.put(ALICE, 4, 4, "d", "x".repeat(33));
    g.put(ALICE, 5, 5, "e", "x".repeat(33));
    List<List<String>> pages = new ArrayList<>();
    byte[] start = new byte[0];
    while (true) {
      ScanResponse page = g.scan(start, null, 0).getScan();
      pages.add(keys(page));
      if (!page.hasResumeKey()) {
        break;
      }
      start = page.getResumeKey().toByteArray();
    }
    assertThat(pages).containsExactly(List.of("a"), List.of("b", "c", "d"), List.of("e"));
  }

  @Test
  void resumeKeyAfterTheLongestAllowedKeyIsAccepted() {
    Group g = Group.of(1);
    String longest = "k".repeat(KvServiceOptions.defaults().maxKeyBytes());
    g.put(ALICE, 1, 1, longest, "1");
    g.put(ALICE, 2, 2, "z", "2");
    ScanResponse first = g.scan(new byte[0], null, 1).getScan();
    assertThat(keys(first)).containsExactly(longest);
    ScanResponse second = g.scan(first.getResumeKey().toByteArray(), null, 1).getScan();
    assertThat(keys(second)).containsExactly("z");
  }

  @Test
  void theNodesOwnRecordsAreInvisibleToClients() {
    Group g = Group.of(1);
    g.put(ALICE, 1, 1, "k", "v");
    assertThat(keys(g.scan(new byte[0], null, 0).getScan())).containsExactly("k");
    assertThat(g.get(ClientSessions.sessionKey(ALICE, new byte[0]))).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // Idempotency, now decided at apply time on every replica

  @Test
  void aRetriedPutIsAnsweredNotAppliedAgain() {
    Group g = Group.of(1);
    KvResponse first = g.put(ALICE, 1, 1, "k", "alice");
    g.put(BOB, 1, 1, "k", "bob");
    assertThat(g.put(ALICE, 1, 1, "k", "alice")).isEqualTo(first);
    assertThat(g.get("k")).contains("bob");
  }

  @Test
  void aRetriedDeleteIsAnsweredNotAppliedAgain() {
    Group g = Group.of(1);
    g.put(ALICE, 1, 1, "k", "v");
    g.delete(BOB, 1, 1, "k");
    g.put(ALICE, 2, 2, "k", "again");
    assertThat(g.delete(BOB, 1, 1, "k").hasDelete()).isTrue();
    assertThat(g.get("k")).contains("again");
  }

  @Test
  void aLateCopyOfAFinishedWriteIsRejected() {
    Group g = Group.of(1);
    g.put(ALICE, 1, 1, "k", "old");
    g.put(ALICE, 2, 2, "k", "new");
    assertThat(errorOf(g.put(ALICE, 1, 1, "k", "old")).hasStaleRequest()).isTrue();
    assertThat(g.get("k")).contains("new");
  }

  @Test
  void resultsTheClientNoLongerNeedsAreDeleted() {
    Group g = Group.of(1);
    for (int seq = 1; seq <= 10; seq++) {
      g.put(ALICE, seq, seq, "k" + seq, "v");
    }
    assertThat(g.storedResults(1, ALICE)).containsExactly(10L);
    g.put(BOB, 1, 1, "a", "v");
    g.put(BOB, 2, 1, "b", "v");
    g.put(BOB, 3, 1, "c", "v");
    assertThat(g.storedResults(1, BOB)).containsExactly(1L, 2L, 3L);
    g.put(BOB, 4, 3, "d", "v");
    assertThat(g.storedResults(1, BOB)).containsExactly(3L, 4L);
  }

  @Test
  void theFloorNeverGoesDown() {
    Group g = Group.of(1);
    g.put(ALICE, 5, 5, "k", "five");
    g.put(ALICE, 6, 1, "k", "six");
    assertThat(errorOf(g.put(ALICE, 4, 4, "k", "four")).hasStaleRequest()).isTrue();
    assertThat(g.get("k")).contains("six");
  }

  @Test
  void aRetryAfterACrashAndRestartIsStillRecognized() {
    Group g = Group.of(1);
    KvResponse first = g.put(ALICE, 1, 1, "k", "alice");
    g.put(BOB, 1, 1, "k", "bob");
    g.crash(1);
    g.restart(1);
    g.awaitLeader();
    assertThat(g.put(ALICE, 1, 1, "k", "alice")).isEqualTo(first);
    assertThat(g.get("k")).contains("bob");
  }

  @Test
  void afterACrashTheDataAndTheAppliedIndexAgree() {
    Group g = Group.of(1);
    for (int i = 0; i < 300; i++) {
      g.put(ALICE, i + 1, i + 1, key(i), "v" + i);
    }
    g.crash(1); // unsynced applies may be lost; the log has them, so they are replayed
    g.restart(1);
    g.awaitLeader();
    for (int i = 0; i < 300; i += 37) {
      assertThat(g.get(key(i))).contains("v" + i);
    }
    RaftNode.Status s = g.node(1).kv.raftStatus();
    assertThat(s.applied()).isEqualTo(s.commit());
    assertThat(g.engineApplied(1)).isEqualTo(s.applied());
  }

  @Test
  void idleSessionsExpireByLogTimeAndActiveOnesStay() {
    KvServiceOptions d = KvServiceOptions.defaults();
    KvServiceOptions shortTtl =
        new KvServiceOptions(
            d.maxKeyBytes(),
            d.maxValueBytes(),
            d.defaultScanLimit(),
            d.maxScanLimit(),
            d.scanPageBytes(),
            d.maxInflightRequests(),
            Duration.ofSeconds(10),
            Duration.ofSeconds(1));
    Group g = Group.of(1, shortTtl, 1000);
    g.put(ALICE, 1, 1, "a", "v");
    g.runFor(Duration.ofSeconds(6));
    g.put(BOB, 1, 1, "b", "v");
    assertThat(g.sessionCount(1)).isEqualTo(2);
    g.runFor(Duration.ofSeconds(6)); // ALICE idle 12 s, BOB 6 s
    assertThat(g.sessionCount(1)).isEqualTo(1);
    assertThat(g.storedResults(1, ALICE)).isEmpty();
    g.runFor(Duration.ofSeconds(6));
    assertThat(g.sessionCount(1)).isZero();
    assertThat(g.get("a")).contains("v");
    assertThat(g.get("b")).contains("v");
  }

  // ---------------------------------------------------------------------------------------------
  // Rejections

  @Test
  void malformedRequestsAreRejectedAsInvalid() {
    Group g = Group.of(1);
    KvRequest put = putRequest(ALICE, 1, 1, "k", "v");
    g.assertInvalid(put.toBuilder().clearRequestId().build());
    g.assertInvalid(
        put.toBuilder()
            .setRequestId(
                put.getRequestId().toBuilder().setClientId(ByteString.copyFrom(id(1), 0, 8)))
            .build());
    g.assertInvalid(put.toBuilder().setRequestId(put.getRequestId().toBuilder().setSeq(0)).build());
    g.assertInvalid(putRequest(ALICE, 3, 4, "k", "v"));
    int maxKey = KvServiceOptions.defaults().maxKeyBytes();
    int maxValue = KvServiceOptions.defaults().maxValueBytes();
    g.assertInvalid(putRequest(ALICE, 1, 1, "k".repeat(maxKey + 1), "v"));
    g.assertInvalid(putRequest(ALICE, 1, 1, "k", "v".repeat(maxValue + 1)));
    g.assertInvalid(KvRequest.getDefaultInstance());
    g.assertInvalid(getRequest(utf8("k".repeat(maxKey + 1))));
    assertThat(g.get("k")).isEmpty();
  }

  @Test
  void workIsNotStartedOnceTheDeadlineHasPassed() {
    Group g = Group.of(1);
    KvResponse r = g.call(g.node(1), putRequest(ALICE, 1, 1, "k", "v"), g.clock.nanoTime());
    assertThat(errorOf(r).hasDeadlineExceeded()).isTrue();
    assertThat(g.get("k")).isEmpty();
    assertThat(g.put(ALICE, 1, 1, "k", "v").hasPut()).isTrue();
  }

  @Test
  void requestsBeyondTheInflightLimitAreRefusedAsBusy() {
    Group g = Group.of(1, KvServiceOptions.defaults().withMaxInflightRequests(1), 1000);
    ReplicatedKv kv = g.node(1).kv;
    CompletableFuture<KvResponse> first =
        kv.handle(putRequest(ALICE, 1, 1, "k", "v"), KvHandler.NO_DEADLINE);
    CompletableFuture<KvResponse> second = kv.handle(getRequest(utf8("k")), KvHandler.NO_DEADLINE);
    assertThat(second.join().getError().hasServerBusy()).isTrue();
    g.await(first);
    assertThat(first.join().hasPut()).isTrue();
    assertThat(g.get("k")).contains("v");
  }

  @Test
  void aStorageFailureStopsTheReplicaAndIsReportedOnce() {
    Group g = Group.of(1);
    g.put(ALICE, 1, 1, "warm", "up");
    g.node(1).failing.engaged = true;
    Error first = errorOf(g.put(ALICE, 2, 2, "k", "v"));
    assertThat(first.hasStorageFailure()).isTrue();
    assertThat(first.getStorageFailure().getMaybeApplied())
        .as("it was proposed, and the log write is what failed")
        .isTrue();
    Error second = errorOf(g.put(ALICE, 3, 3, "k", "v"));
    assertThat(second.getStorageFailure().getMaybeApplied()).isFalse();
    assertThat(g.node(1).failures).hasSize(1);
  }

  // ---------------------------------------------------------------------------------------------
  // A group of three

  @Test
  void followersRedirectToTheLeader() {
    Group g = Group.of(3);
    NodeId leader = g.awaitLeader();
    g.put(ALICE, 1, 1, "k", "v");
    for (NodeId id : g.nodes.keySet()) {
      if (!id.equals(leader)) {
        KvResponse r = g.call(g.nodes.get(id), getRequest(utf8("k")), KvHandler.NO_DEADLINE);
        assertThat(errorOf(r).getNotLeader().getLeaderAddress()).isEqualTo(g.peers.get(leader));
        assertThat(errorOf(r).getNotLeader().getLeaderId()).isEqualTo(leader.value());
      }
    }
  }

  @Test
  void aWriteIsAnsweredOnlyOnceAMajorityHasIt() {
    Group g = Group.of(3);
    NodeId leader = g.awaitLeader();
    g.put(ALICE, 1, 1, "warm", "up");
    g.cut = (a, b) -> !a.equals(b); // nobody hears anybody
    CompletableFuture<KvResponse> w =
        g.nodes.get(leader).kv.handle(putRequest(ALICE, 2, 2, "k", "v"), KvHandler.NO_DEADLINE);
    g.runFor(Duration.ofMillis(80)); // less than an election timeout
    assertThat(w).isNotDone();
    g.cut = (a, b) -> false;
    g.await(w);
    assertThat(w.join().hasPut()).isTrue();
  }

  @Test
  void aLeaderCutOffFromItsGroupRedirectsTheWritesItHolds() {
    Group g = Group.of(3);
    NodeId leader = g.awaitLeader();
    g.put(ALICE, 1, 1, "warm", "up");
    g.cut = (a, b) -> a.equals(leader) != b.equals(leader);
    CompletableFuture<KvResponse> w =
        g.nodes.get(leader).kv.handle(putRequest(ALICE, 2, 2, "k", "v"), KvHandler.NO_DEADLINE);
    g.await(w); // check-quorum makes it step down within an election timeout or two
    Error e = errorOf(w.join());
    assertThat(e.hasNotLeader()).isTrue();
    assertThat(e.getNotLeader().getMaybeApplied())
        .as("it was in the old leader's log, and might yet commit")
        .isTrue();
  }

  @Test
  void aLaggingReplicaCatchesUpFromASnapshot() {
    Group g = Group.of(3, KvServiceOptions.defaults(), 20);
    NodeId leader = g.awaitLeader();
    NodeId lagging = g.nodes.keySet().stream().filter(n -> !n.equals(leader)).findFirst().get();
    g.crash(lagging.value());
    for (int i = 0; i < 120; i++) {
      g.put(ALICE, i + 1, i + 1, key(i), "v" + i);
    }
    g.restart(lagging.value());
    g.runFor(Duration.ofSeconds(2));
    assertThat(g.snapshotsSent()).isPositive();
    assertThat(g.userData(lagging.value())).isEqualTo(g.userData(leader.value())).hasSize(120);
  }

  @Test
  void everyReplicaAppliesEveryWriteExactlyOnce() {
    Group g = Group.of(3);
    g.awaitLeader();
    for (int i = 0; i < 50; i++) {
      g.put(ALICE, i + 1, i + 1, key(i % 7), "v" + i);
    }
    g.runFor(Duration.ofMillis(500));
    for (NodeId id : g.nodes.keySet()) {
      Map<KvStateMachine.RequestKey, Integer> effects = g.effects.get(id);
      assertThat(effects).hasSize(50);
      assertThat(effects.values()).allMatch(n -> n == 1);
    }
    assertThat(g.userData(1)).isEqualTo(g.userData(2)).isEqualTo(g.userData(3));
  }

  // ---------------------------------------------------------------------------------------------
  // The test group

  /** A group of replicas on one simulated clock, network and set of disks. */
  private static final class Group {
    final SimClock clock = new SimClock();
    final SimScheduler scheduler = new SimScheduler(clock);
    final RandomSource random = SplittableRandomSource.seeded(1);
    final Map<NodeId, Node> nodes = new TreeMap<>(Comparator.comparingInt(NodeId::value));
    final Map<NodeId, String> peers = new HashMap<>();
    final Map<NodeId, Transport.MessageHandler> handlers = new HashMap<>();
    final Map<NodeId, Map<KvStateMachine.RequestKey, Integer>> effects = new HashMap<>();
    final Set<NodeId> down = new HashSet<>();
    final KvServiceOptions options;
    final int compactEvery;
    Cut cut = (a, b) -> false;
    long snapshotMessages;

    @FunctionalInterface
    interface Cut {
      boolean blocked(NodeId from, NodeId to);
    }

    Group(int n, KvServiceOptions options, int compactEvery) {
      this.options = options;
      this.compactEvery = compactEvery;
      for (int i = 1; i <= n; i++) {
        peers.put(new NodeId(i), "node-" + i + ":7700");
      }
      for (int i = 1; i <= n; i++) {
        Node node = new Node(new NodeId(i), new SimFileSystem(random.split()));
        nodes.put(node.id, node);
        boot(node);
      }
    }

    static Group of(int n) {
      return of(n, KvServiceOptions.defaults(), 1000);
    }

    static Group of(int n, KvServiceOptions options, int compactEvery) {
      Group g = new Group(n, options, compactEvery);
      g.awaitLeader();
      return g;
    }

    Node node(int i) {
      return nodes.get(new NodeId(i));
    }

    void boot(Node n) {
      n.gate = new Gate(scheduler);
      n.failing = new FailingEngine(LsmEngine.open(n.fs, n.gate, DIR, SMALL));
      ReplicatedKv.Config cfg =
          new ReplicatedKv.Config(
              n.id, peers, RaftConfig.defaults(n.id), options, Duration.ofMillis(10), compactEvery);
      n.kv =
          new ReplicatedKv(
              cfg,
              n.failing,
              transport(n.id),
              clock,
              n.gate,
              random.split(),
              n.failures::add,
              (replica, a) -> {
                if (a.effect()) {
                  effects
                      .computeIfAbsent(replica, k -> new HashMap<>())
                      .merge(a.request(), 1, Integer::sum);
                }
              });
      down.remove(n.id);
      n.kv.start();
    }

    void crash(int i) {
      Node n = node(i);
      n.kv.close();
      n.gate.alive = false;
      down.add(n.id);
      n.fs.crash();
    }

    void restart(int i) {
      boot(node(i));
    }

    private Transport transport(NodeId self) {
      return new Transport() {
        @Override
        public NodeId localNode() {
          return self;
        }

        @Override
        public void send(NodeId to, byte[] message) {
          if (message.length > 0 && message[0] == 7) {
            snapshotMessages++; // RaftCodec's tag for InstallSnapshot
          }
          scheduler.schedule(
              Duration.ofMillis(1),
              () -> {
                Transport.MessageHandler h = handlers.get(to);
                if (h != null && !down.contains(to) && !cut.blocked(self, to)) {
                  h.onMessage(self, message);
                }
              });
        }

        @Override
        public void setHandler(MessageHandler handler) {
          if (handler == null) {
            handlers.remove(self);
          } else {
            handlers.put(self, handler);
          }
        }
      };
    }

    long snapshotsSent() {
      return snapshotMessages;
    }

    NodeId awaitLeader() {
      for (int i = 0; i < 1000; i++) {
        for (Node n : nodes.values()) {
          if (!down.contains(n.id) && n.kv.raftStatus().role() == RaftNode.Role.LEADER) {
            return n.id;
          }
        }
        scheduler.runFor(Duration.ofMillis(10));
      }
      throw new AssertionError("no leader elected");
    }

    Node leaderNode() {
      return nodes.get(awaitLeader());
    }

    void runFor(Duration d) {
      scheduler.runFor(d);
    }

    void await(CompletableFuture<?> f) {
      long until = clock.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (!f.isDone() && clock.nanoTime() < until) {
        if (!scheduler.runNext()) {
          break;
        }
      }
      assertThat(f).as("answered within 30 s").isDone();
    }

    KvResponse call(Node n, KvRequest r, long deadline) {
      CompletableFuture<KvResponse> f = n.kv.handle(r, deadline);
      await(f);
      return f.join();
    }

    KvResponse leaderCall(KvRequest r) {
      return call(leaderNode(), r, KvHandler.NO_DEADLINE);
    }

    KvResponse put(byte[] client, long seq, long floor, String key, String value) {
      return leaderCall(putRequest(client, seq, floor, key, value));
    }

    KvResponse delete(byte[] client, long seq, long floor, String key) {
      return leaderCall(
          KvRequest.newBuilder()
              .setRequestId(requestId(client, seq))
              .setFirstIncompleteSeq(floor)
              .setDelete(DeleteRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)))
              .build());
    }

    Optional<String> get(String key) {
      return get(utf8(key)).map(v -> new String(v, StandardCharsets.UTF_8));
    }

    Optional<byte[]> get(byte[] key) {
      KvResponse r = leaderCall(getRequest(key));
      assertThat(r.hasGet()).as("response %s", r).isTrue();
      return r.getGet().getFound()
          ? Optional.of(r.getGet().getValue().toByteArray())
          : Optional.empty();
    }

    KvResponse scan(byte[] start, byte[] end, int limit) {
      ScanRequest.Builder scan =
          ScanRequest.newBuilder().setStartKey(ByteString.copyFrom(start)).setLimit(limit);
      if (end != null) {
        scan.setEndKey(ByteString.copyFrom(end));
      }
      KvResponse r = leaderCall(KvRequest.newBuilder().setScan(scan).build());
      assertThat(r.hasScan()).as("response %s", r).isTrue();
      return r;
    }

    void assertInvalid(KvRequest request) {
      assertThat(errorOf(leaderCall(request)).hasInvalidArgument())
          .as("request %s", request)
          .isTrue();
    }

    List<Long> storedResults(int i, byte[] client) {
      List<Long> seqs = new ArrayList<>();
      byte[] prefix = KeySpace.local(KeySpace.RESULT, client);
      byte[] end = Arrays.copyOf(prefix, prefix.length + 9);
      Arrays.fill(end, prefix.length, end.length, (byte) 0xFF);
      scanEngine(i, prefix, end, e -> seqs.add(KeySpace.readUint64(e.key(), prefix.length)));
      return seqs;
    }

    int sessionCount(int i) {
      int[] n = {0};
      scanEngine(
          i,
          KeySpace.local(KeySpace.SESSION),
          KeySpace.local((byte) (KeySpace.SESSION + 1)),
          e -> n[0]++);
      return n[0];
    }

    Map<String, String> userData(int i) {
      Map<String, String> out = new TreeMap<>();
      scanEngine(
          i,
          new byte[] {KeySpace.USER},
          new byte[] {KeySpace.USER + 1},
          e ->
              out.put(
                  new String(KeySpace.fromUser(e.key()), StandardCharsets.UTF_8),
                  new String(e.value(), StandardCharsets.UTF_8)));
      return out;
    }

    long engineApplied(int i) {
      return new KvStateMachine(node(i).failing, 1, options).appliedIndex();
    }

    private void scanEngine(
        int i, byte[] start, byte[] end, java.util.function.Consumer<KvEntry> f) {
      try (KvIterator it = node(i).failing.scan(start, end)) {
        while (it.hasNext()) {
          f.accept(it.next());
        }
      }
    }
  }

  private static final class Node {
    final NodeId id;
    final SimFileSystem fs;
    final List<Throwable> failures = new ArrayList<>();
    Gate gate;
    FailingEngine failing;
    ReplicatedKv kv;

    Node(NodeId id, SimFileSystem fs) {
      this.id = id;
      this.fs = fs;
    }
  }

  /** A process's timers, dropped when it crashes. */
  private static final class Gate implements Scheduler {
    final Scheduler shared;
    boolean alive = true;

    Gate(Scheduler shared) {
      this.shared = shared;
    }

    @Override
    public void execute(Runnable task) {
      shared.execute(() -> {
        if (alive) {
          task.run();
        }
      });
    }

    @Override
    public Cancellable schedule(Duration delay, Runnable task) {
      return shared.schedule(
          delay,
          () -> {
            if (alive) {
              task.run();
            }
          });
    }
  }

  /** An engine whose writes can be made to fail. */
  private static final class FailingEngine implements KvEngine {
    final LsmEngine delegate;
    boolean engaged;

    FailingEngine(LsmEngine delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(WriteBatch batch, boolean sync) {
      if (engaged) {
        throw new StorageException("disk on fire");
      }
      delegate.write(batch, sync);
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
      return delegate.get(key);
    }

    @Override
    public KvIterator scan(byte[] startInclusive, byte[] endExclusive) {
      return delegate.scan(startInclusive, endExclusive);
    }

    @Override
    public EngineSnapshot snapshot() {
      return delegate.snapshot();
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  // ---------------------------------------------------------------------------------------------

  private static Error errorOf(KvResponse response) {
    assertThat(response.hasError()).as("response %s", response).isTrue();
    return response.getError();
  }

  private static KvRequest putRequest(
      byte[] client, long seq, long floor, String key, String value) {
    return KvRequest.newBuilder()
        .setRequestId(requestId(client, seq))
        .setFirstIncompleteSeq(floor)
        .setPut(
            PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value)))
        .build();
  }

  private static KvRequest getRequest(byte[] key) {
    return KvRequest.newBuilder()
        .setGet(GetRequest.newBuilder().setKey(ByteString.copyFrom(key)))
        .build();
  }

  private static RequestId requestId(byte[] client, long seq) {
    return RequestId.newBuilder().setClientId(ByteString.copyFrom(client)).setSeq(seq).build();
  }

  private static List<String> keys(ScanResponse page) {
    Function<KeyValue, String> k = kv -> kv.getKey().toStringUtf8();
    return page.getEntriesList().stream().map(k).toList();
  }

  private static byte[] id(int n) {
    byte[] id = new byte[16];
    id[15] = (byte) n;
    return id;
  }

  private static String key(int i) {
    return String.format("key%05d", i);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}

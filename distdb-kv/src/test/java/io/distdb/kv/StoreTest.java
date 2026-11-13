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
import io.distdb.rpc.proto.GetRequest;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.Operator;
import io.distdb.rpc.proto.PutRequest;
import io.distdb.rpc.proto.RangeDescriptor;
import io.distdb.rpc.proto.RangeHeader;
import io.distdb.rpc.proto.RequestId;
import io.distdb.rpc.proto.SplitOperator;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * Stores of a sharded cluster on simulated disks and an in-memory network, all on one simulated
 * thread, with no placement driver: each test sends operators to a range's leader directly.
 */
class StoreTest {

  private static final Path DIR = Path.of("/node");
  private static final EngineOptions SMALL =
      new EngineOptions(16 * 1024, 1024, 10, 32 * 1024, 4, 12, 128 * 1024, 10);
  private static final byte[] ALICE = new byte[16];

  @Test
  void aStoreAnswersByRangeOrByKeyAndRefusesWhatItDoesNotHold() {
    Cluster c = new Cluster(1, 1);
    assertThat(c.put(1, 1, 1, "k", "v").hasPut()).isTrue();
    assertThat(c.get(c.node(1), 0, 0, "k")).contains("v");
    assertThat(c.get(c.node(1), 1, 1, "k")).contains("v");
    KvResponse stale = c.call(c.node(1), get(1, 2, "k"));
    assertThat(stale.getError().hasEpochMismatch()).isTrue();
    KvResponse elsewhere = c.call(c.node(1), get(9, 1, "k"));
    assertThat(elsewhere.getError().hasRangeNotFound()).isTrue();
  }

  @Test
  void aSplitHappensOnEveryReplicaAndKeysThenBelongToTheirNewRange() {
    Cluster c = new Cluster(3, 3);
    for (int i = 0; i < 10; i++) {
      c.put(1, 1, i + 1, "k" + i, "v" + i);
    }
    c.split(1, "k5", 7);
    for (Node n : c.nodes.values()) {
      assertThat(c.descriptor(n, 1).getEndKey().toStringUtf8()).isEqualTo("k5");
      RangeDescriptor right = c.descriptor(n, 7);
      assertThat(right.getStartKey().toStringUtf8()).isEqualTo("k5");
      assertThat(right.hasEndKey()).isFalse();
    }
    // Routed with the old version, or to the range that no longer holds the key: refused.
    assertThat(c.call(c.leader(1), put(1, 1, 11, "k7", "x")).getError().hasEpochMismatch())
        .isTrue();
    assertThat(c.call(c.leader(1), put(1, 2, 12, "k7", "x")).getError().hasEpochMismatch())
        .isTrue();
    assertThat(c.call(c.leader(7), put(7, 2, 13, "k7", "x")).hasPut()).isTrue();
    assertThat(c.get(c.leader(7), 7, 2, "k7")).contains("x");
    assertThat(c.get(c.leader(1), 1, 2, "k3")).contains("v3");
  }

  @Test
  void aWriteRetriedAfterTheSplitIsAnsweredFromTheCopiedSession() {
    Cluster c = new Cluster(3, 3);
    KvRequest first = put(1, 1, 1, "k7", "v");
    assertThat(c.call(c.leader(1), first).hasPut()).isTrue();
    c.split(1, "k5", 7);
    KvRequest retry = first.toBuilder().setHeader(header(7, 2)).build();
    assertThat(c.call(c.leader(7), retry).hasPut()).isTrue();
    c.run(Duration.ofSeconds(1));
    for (Node n : c.nodes.values()) {
      assertThat(c.effects.getOrDefault(n.id, 0)).as("applied once on %s", n.id).isEqualTo(1);
    }
  }

  @Test
  void aMovedReplicaIsDestroyedByARemovedNoticeAndCanBeAddedBack() {
    Cluster c = new Cluster(4, 3);
    for (int i = 0; i < 5; i++) {
      c.put(1, 1, i + 1, "k" + i, "v" + i);
    }
    NodeId leader = c.leader(1).id;
    NodeId moved = leader.equals(new NodeId(3)) ? new NodeId(2) : new NodeId(3);
    c.change(1, Operator.newBuilder().setAddLearner(4));
    c.change(1, Operator.newBuilder().setPromote(4));
    c.change(1, Operator.newBuilder().setRemove(moved.value()));
    assertThat(c.descriptor(c.node(4), 1).getVotersList()).contains(4);
    assertThat(c.userKeys(c.node(4))).as("node 4 holds the data").hasSize(5);

    // Nobody tells the removed replica; it campaigns, and a member answers REMOVED.
    c.run(Duration.ofSeconds(3));
    Node gone = c.nodes.get(moved);
    assertThat(gone.store.replicas()).isEmpty();
    assertThat(c.userKeys(gone)).as("its data is deleted").isEmpty();
    assertThat(RangeStateMachine.tombstone(gone.engine, 1)).isPresent();
    assertThat(c.destroyed).contains(moved);
    assertThat(new RaftStore(gone.engine, 1).hardState())
        .as("its term and vote are kept, so it never votes twice in a term")
        .hasValueSatisfying(
            hs -> assertThat(hs.term()).isGreaterThan(RangeStateMachine.INITIAL_TERM));

    // Added back later: newer configurations pass the tombstone, and it catches up.
    c.change(1, Operator.newBuilder().setAddLearner(moved.value()));
    c.change(1, Operator.newBuilder().setPromote(moved.value()));
    assertThat(c.descriptor(gone, 1).getVotersList()).contains(moved.value());
    assertThat(c.userKeys(gone)).hasSize(5);
  }

  @Test
  void aLeaderAskedToRemoveItselfHandsOverItsLeadershipFirst() {
    Cluster c = new Cluster(4, 3);
    c.put(1, 1, 1, "k", "v");
    c.change(1, Operator.newBuilder().setAddLearner(4));
    c.change(1, Operator.newBuilder().setPromote(4));
    Node old = c.leader(1);
    c.leader(1)
        .store
        .operate(Operator.newBuilder().setRangeId(1).setRemove(old.id.value()).build());
    c.run(Duration.ofSeconds(1));
    assertThat(c.leader(1).id).as("handed over, not removed").isNotEqualTo(old.id);
    assertThat(c.descriptor(c.leader(1), 1).getVotersList()).contains(old.id.value());
    c.change(1, Operator.newBuilder().setRemove(old.id.value()));
    assertThat(c.descriptor(c.leader(1), 1).getVotersList()).doesNotContain(old.id.value());
    assertThat(c.get(c.leader(1), 1, 1, "k")).contains("v");
  }

  // ---------------------------------------------------------------------------------------------

  private static KvRequest put(long range, long version, long seq, String key, String value) {
    KvRequest.Builder b =
        KvRequest.newBuilder()
            .setRequestId(
                RequestId.newBuilder().setClientId(ByteString.copyFrom(ALICE)).setSeq(seq))
            .setFirstIncompleteSeq(seq)
            .setPut(
                PutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(key))
                    .setValue(ByteString.copyFromUtf8(value)));
    if (range != 0) {
      b.setHeader(header(range, version));
    }
    return b.build();
  }

  private static KvRequest get(long range, long version, String key) {
    KvRequest.Builder b =
        KvRequest.newBuilder().setGet(GetRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)));
    if (range != 0) {
      b.setHeader(header(range, version));
    }
    return b.build();
  }

  private static RangeHeader header(long range, long version) {
    return RangeHeader.newBuilder().setRangeId(range).setVersion(version).build();
  }

  private static final class Node {
    final NodeId id;
    final SimFileSystem fs;
    Gate gate;
    LsmEngine engine;
    Store store;

    Node(NodeId id, SimFileSystem fs) {
      this.id = id;
      this.fs = fs;
    }
  }

  private static final class Cluster {
    final SimClock clock = new SimClock();
    final SimScheduler scheduler = new SimScheduler(clock);
    final RandomSource random = SplittableRandomSource.seeded(1);
    final Map<NodeId, Node> nodes = new TreeMap<>(Comparator.comparingInt(NodeId::value));
    final Map<NodeId, Transport.MessageHandler> handlers = new HashMap<>();
    final Map<NodeId, Integer> effects = new HashMap<>();
    final Set<NodeId> destroyed = new HashSet<>();
    final List<NodeId> founders = new ArrayList<>();

    /** {@code n} stores, the first {@code founders} of which found range 1. */
    Cluster(int n, int founders) {
      for (int i = 1; i <= founders; i++) {
        this.founders.add(new NodeId(i));
      }
      for (int i = 1; i <= n; i++) {
        Node node = new Node(new NodeId(i), new SimFileSystem(random.split()));
        nodes.put(node.id, node);
        boot(node);
      }
      run(Duration.ofSeconds(1));
      leader(1);
    }

    Node node(int i) {
      return nodes.get(new NodeId(i));
    }

    void boot(Node n) {
      n.gate = new Gate(scheduler);
      n.engine = LsmEngine.open(n.fs, n.gate, DIR, SMALL);
      Store.Config cfg =
          new Store.Config(
              n.id,
              "n" + n.id.value(),
              founders,
              RaftConfig.defaults(n.id),
              KvServiceOptions.defaults(),
              Duration.ofMillis(10),
              1000,
              Duration.ofSeconds(1));
      n.store =
          new Store(
              cfg,
              n.engine,
              transport(n.id),
              null,
              clock,
              n.gate,
              random.split(),
              t -> {
                throw new AssertionError("storage failed on " + n.id, t);
              },
              new Store.Observer() {
                @Override
                public void applied(NodeId node, long range, RangeStateMachine.Applied a) {
                  if (a.effect()) {
                    effects.merge(node, 1, Integer::sum);
                  }
                }

                @Override
                public void destroyed(NodeId node, long range) {
                  destroyed.add(node);
                }
              });
      n.store.start();
    }

    void run(Duration d) {
      scheduler.runFor(d);
    }

    /** The leader of {@code range}, waiting up to five seconds for one. */
    Node leader(long range) {
      for (int i = 0; i < 100; i++) {
        Node best = null;
        long bestTerm = -1;
        for (Node n : nodes.values()) {
          for (Store.ReplicaView r : n.store.replicas()) {
            if (r.rangeId() == range
                && r.raft().role() == RaftNode.Role.LEADER
                && r.raft().term() > bestTerm) {
              best = n;
              bestTerm = r.raft().term();
            }
          }
        }
        if (best != null) {
          return best;
        }
        run(Duration.ofMillis(50));
      }
      throw new AssertionError("no leader for r" + range);
    }

    RangeDescriptor descriptor(Node n, long range) {
      return n.store.replicas().stream()
          .filter(r -> r.rangeId() == range)
          .findFirst()
          .map(Store.ReplicaView::descriptor)
          .orElseThrow(() -> new AssertionError(n.id + " has no r" + range));
    }

    List<String> userKeys(Node n) {
      List<String> out = new ArrayList<>();
      try (var it = n.engine.scan(KeySpace.user(new byte[0]), KeySpace.userEnd(null))) {
        while (it.hasNext()) {
          out.add(new String(KeySpace.fromUser(it.next().key()), StandardCharsets.UTF_8));
        }
      }
      return out;
    }

    KvResponse call(Node n, KvRequest r) {
      CompletableFuture<KvResponse> f = n.store.handle(r, KvHandler.NO_DEADLINE);
      for (int i = 0; i < 200 && !f.isDone(); i++) {
        run(Duration.ofMillis(25));
      }
      assertThat(f).as("answered").isDone();
      return f.join();
    }

    KvResponse put(long range, long version, long seq, String key, String value) {
      return call(leader(range), StoreTest.put(range, version, seq, key, value));
    }

    Optional<String> get(Node n, long range, long version, String key) {
      KvResponse r = call(n, StoreTest.get(range, version, key));
      if (!r.hasGet()) {
        return Optional.empty();
      }
      return r.getGet().getFound()
          ? Optional.of(r.getGet().getValue().toStringUtf8())
          : Optional.empty();
    }

    void split(long range, String key, long newId) {
      long version = descriptor(leader(range), range).getVersion();
      leader(range)
          .store
          .operate(
              Operator.newBuilder()
                  .setRangeId(range)
                  .setSplit(
                      SplitOperator.newBuilder()
                          .setSplitKey(ByteString.copyFromUtf8(key))
                          .setNewRangeId(newId)
                          .setExpectedVersion(version))
                  .build());
      run(Duration.ofSeconds(1));
      leader(newId);
    }

    /** Sends {@code op} to the range's leader until its descriptor shows the change. */
    void change(long range, Operator.Builder op) {
      RangeDescriptor before = descriptor(leader(range), range);
      for (int i = 0; i < 40; i++) {
        leader(range).store.operate(op.setRangeId(range).build());
        run(Duration.ofMillis(250));
        if (descriptor(leader(range), range).getConfVer() != before.getConfVer()) {
          run(Duration.ofMillis(500)); // let everyone apply it
          return;
        }
      }
      throw new AssertionError("never applied: " + op);
    }

    private Transport transport(NodeId self) {
      return new Transport() {
        @Override
        public NodeId localNode() {
          return self;
        }

        @Override
        public void send(NodeId to, byte[] message) {
          scheduler.schedule(
              Duration.ofMillis(1),
              () -> {
                Transport.MessageHandler h = handlers.get(to);
                if (h != null) {
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
  }

  /** A scheduler that stops running a node's tasks once it has crashed. */
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
}

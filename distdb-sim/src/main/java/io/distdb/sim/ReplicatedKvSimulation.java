package io.distdb.sim;

import io.distdb.client.ClientOptions;
import io.distdb.client.KvClient;
import io.distdb.client.KvException;
import io.distdb.common.NodeId;
import io.distdb.env.RandomSource;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.kv.KeySpace;
import io.distdb.kv.KvServiceOptions;
import io.distdb.kv.KvStateMachine;
import io.distdb.kv.ReplicatedKv;
import io.distdb.raft.RaftConfig;
import io.distdb.raft.RaftNode;
import io.distdb.rpc.KvChannel;
import io.distdb.rpc.RpcException;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import io.distdb.storage.StorageException;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * The Phase 5 gate: a replicated KV group under faults, judged by what its clients saw.
 *
 * <p>Each node is a real {@link ReplicatedKv} (Raft, the state machine, a real LSM engine on its
 * own {@link SimFileSystem}); Raft traffic crosses a {@link SimNetwork}. Clients are real {@link
 * KvClient}s, talking to the nodes over lossy simulated links that the same partitions cut. The run
 * partitions the network, cuts power to nodes (often the leader, on purpose), and fails disks in
 * the middle of writes. Every client operation is recorded with when it started, when it ended, and
 * what it returned.
 *
 * <p>Afterwards:
 *
 * <ul>
 *   <li>the history must be <b>linearizable</b> ({@link Linearizability});
 *   <li>no write may take effect twice, every acknowledged write must have taken effect, and every
 *       write reported as certainly failed must not have;
 *   <li>once the faults stop, every client must make progress, and all replicas must converge on
 *       identical data.
 * </ul>
 *
 * <p>It also measures failover: from cutting the leader's power to a new leader, and to the next
 * write a client sees succeed. Rerunning a seed replays the identical run.
 */
public final class ReplicatedKvSimulation {

  /**
   * What the run does.
   *
   * @param nodes group size
   * @param clients concurrent clients, each issuing one operation at a time
   * @param keys how many keys they share (few keys, many conflicts)
   * @param faultPeriod how long faults are injected
   * @param quietPeriod how long the healed group has to recover
   * @param maxFaultInterval the longest gap between faults (the shortest is a sixth)
   * @param diskFaults whether disks may fail mid-write
   * @param compactEvery how often replicas compact their logs (small: snapshots get sent)
   * @param clockDrift whether each node's timer runs at its own rate (one tick every 8 to 20 ms
   *     instead of exactly 10), as real clocks and GC pauses make them. With perfect clocks a
   *     leader always notices it lost its majority before a new one is elected, which hides any bug
   *     that relies on that (lease reads, for one).
   */
  public record Options(
      int nodes,
      int clients,
      int keys,
      Duration faultPeriod,
      Duration quietPeriod,
      Duration maxFaultInterval,
      boolean diskFaults,
      int compactEvery,
      boolean clockDrift) {

    /**
     * Knobs drawn from the seed: 3 or 5 nodes, gentle or harsh faults, frequent or rare compaction,
     * clocks that drift.
     */
    public static Options varied(long seed) {
      RandomSource r = SplittableRandomSource.seeded(seed * 0x9E3779B97F4A7C15L + 7);
      int[] compact = {16, 64, 1000};
      return new Options(
          r.nextBoolean() ? 3 : 5,
          4,
          r.nextInt(2, 6),
          Duration.ofSeconds(10),
          Duration.ofSeconds(6),
          r.nextBoolean() ? Duration.ofMillis(700) : Duration.ofSeconds(2),
          true,
          compact[r.nextInt(3)],
          true);
    }

    /** Leader crashes only, nothing else, perfect clocks: for measuring failover. */
    public static Options failoverOnly(int nodes) {
      return new Options(
          nodes,
          2,
          3,
          Duration.ofSeconds(30),
          Duration.ofSeconds(3),
          Duration.ofSeconds(2),
          false,
          1000,
          false);
    }
  }

  /** What happened, so tests can check the faults were real and read the failover times. */
  public record Report(
      long seed,
      int operations,
      long writesAcknowledged,
      long readsAnswered,
      long unknownOutcomes,
      long certainlyFailed,
      long powerCuts,
      long leaderCrashes,
      long diskFailures,
      long partitions,
      List<Long> electionFailoverMillis,
      List<Long> clientFailoverMillis) {}

  private static final Path DIR = Path.of("/node");
  private static final EngineOptions ENGINE =
      new EngineOptions(64 * 1024, 4096, 10, 256 * 1024, 4, 12, 1024 * 1024, 10);

  private final long seed;
  private final Options options;
  private final boolean failoverOnly;
  private final RandomSource random;
  private final SimClock clock = new SimClock();
  private final SimScheduler scheduler = new SimScheduler(clock);
  private final SimNetwork net;
  private final Map<NodeId, Node> nodes = new TreeMap<>(Comparator.comparingInt(NodeId::value));
  private final Map<NodeId, String> peers = new LinkedHashMap<>();
  private final List<SimClient> clients = new ArrayList<>();
  private final List<Linearizability.Op> history = new ArrayList<>();
  private final Map<KvStateMachine.RequestKey, Long> effectAt = new HashMap<>();
  private final Set<KvStateMachine.RequestKey> acknowledged = new HashSet<>();
  private final Set<KvStateMachine.RequestKey> certainlyFailed = new HashSet<>();
  private final List<Long> electionFailover = new ArrayList<>();
  private final List<Long> clientFailover = new ArrayList<>();
  private boolean faulty = true;
  private Throwable violation;
  private boolean finished;
  private long quietStart;
  private long powerCuts;
  private long leaderCrashes;
  private long diskFailures;
  private long partitions;
  private long stalls;
  private long writesAcknowledged;
  private long readsAnswered;
  private long unknownOutcomes;
  private long failoverStart = -1;
  private long failoverTerm;
  private boolean clientFailoverPending;
  private long clientFailoverFrom;

  private ReplicatedKvSimulation(long seed, Options options, boolean failoverOnly) {
    this.seed = seed;
    this.options = Objects.requireNonNull(options);
    this.failoverOnly = failoverOnly;
    this.random = SplittableRandomSource.seeded(seed);
    this.net = new SimNetwork(scheduler, random.split(), SimNetwork.Faults.defaults());
    for (int i = 1; i <= options.nodes(); i++) {
      peers.put(new NodeId(i), "n" + i);
    }
    for (NodeId id : peers.keySet()) {
      nodes.put(id, new Node(id));
    }
    for (int c = 0; c < options.clients(); c++) {
      clients.add(new SimClient(c));
    }
  }

  /** Runs one seed; throws an {@link AssertionError} on any violation. */
  public static Report run(long seed, Options options) {
    return new ReplicatedKvSimulation(seed, options, false).execute();
  }

  /** Crashes the leader again and again, with no other faults, and reports failover times. */
  public static Report measureFailover(long seed, int nodes) {
    return new ReplicatedKvSimulation(seed, Options.failoverOnly(nodes), true).execute();
  }

  private Report execute() {
    nodes.values().forEach(Node::boot);
    clients.forEach(SimClient::next);
    every(
        options.maxFaultInterval().dividedBy(6),
        options.maxFaultInterval(),
        failoverOnly ? this::crashLeader : this::injectFault);
    every(Duration.ofMillis(5), Duration.ofMillis(5), this::watchFailover);
    scheduler.runFor(options.faultPeriod());

    // Heal everything and let the group recover.
    faulty = false;
    net.heal();
    nodes.values().forEach(n -> n.life.stall(1));
    for (Node n : nodes.values()) {
      if (n.up && n.armed) {
        n.crash(); // disarm a pending disk failure
      }
      if (!n.up) {
        n.boot();
      }
    }
    quietStart = clock.nanoTime();
    scheduler.runFor(options.quietPeriod());
    finished = true; // clients stop starting operations
    scheduler.runFor(Duration.ofSeconds(3)); // and the replicas converge

    if (violation != null) {
      throw new AssertionError("a check failed during the run", violation);
    }
    checkLiveness();
    checkConvergence();
    checkEffects();
    Linearizability.Result verdict = Linearizability.check(history);
    if (!verdict.linearizable()) {
      throw new AssertionError(
          "not linearizable on key "
              + verdict.key()
              + ":\n  "
              + String.join("\n  ", verdict.history().stream().map(Object::toString).toList()));
    }
    return new Report(
        seed,
        history.size(),
        writesAcknowledged,
        readsAnswered,
        unknownOutcomes,
        certainlyFailed.size(),
        powerCuts,
        leaderCrashes,
        diskFailures,
        partitions,
        List.copyOf(electionFailover),
        List.copyOf(clientFailover));
  }

  // -----------------------------------------------------------------------------------------------
  // Faults

  private void every(Duration min, Duration max, Runnable action) {
    scheduler.schedule(
        Duration.ofNanos(random.nextLong(min.toNanos(), max.toNanos() + 1)),
        () -> {
          if (!finished) {
            action.run();
            every(min, max, action);
          }
        });
  }

  private void injectFault() {
    if (!faulty) {
      return;
    }
    List<NodeId> everyone = new ArrayList<>(nodes.keySet());
    clients.forEach(c -> everyone.add(c.id));
    int dice = random.nextInt(100);
    if (dice < 20) {
      List<NodeId> side = new ArrayList<>();
      for (NodeId id : everyone) {
        if (random.nextBoolean()) {
          side.add(id);
        }
      }
      net.heal();
      net.partition(side);
      partitions++;
    } else if (dice < 27) {
      net.heal();
      net.isolate(randomNode().id);
      partitions++;
    } else if (dice < 37) {
      // The leader loses its peers but not its clients, and its timers stall, so it goes on
      // believing it leads well after the others have elected someone else.
      Node leader = leader();
      if (leader != null) {
        net.heal();
        net.cut(leader.id, nodes.keySet());
        partitions++;
        stall(leader);
      }
    } else if (dice < 47) {
      net.heal();
    } else if (dice < 52) {
      Node n = randomUpNode();
      if (n != null) {
        stall(n);
      }
    } else if (dice < 70) {
      crashLeader();
    } else if (dice < 85) {
      Node n = randomUpNode();
      if (n != null) {
        powerCuts++;
        n.crash();
        n.restartLater();
      }
    } else if (options.diskFaults()) {
      Node n = randomUpNode();
      if (n != null && !n.armed) {
        n.armed = true;
        n.fs.crashAfter(random.nextInt(1, 60));
      }
    }
  }

  /** Slows a node's timers 4 to 8 times for half a second to three seconds. */
  private void stall(Node n) {
    Life life = n.life;
    life.stall(random.nextInt(4, 9));
    stalls++;
    scheduler.schedule(Duration.ofMillis(random.nextInt(500, 3000)), () -> life.stall(1));
  }

  private void crashLeader() {
    if (!faulty) {
      return;
    }
    Node leader = leader();
    if (leader == null) {
      return;
    }
    powerCuts++;
    leaderCrashes++;
    failoverTerm = leader.kv.raftStatus().term();
    failoverStart = clock.nanoTime();
    clientFailoverFrom = failoverStart;
    clientFailoverPending = true;
    leader.crash();
    leader.restartLater();
  }

  /** Measures the time from a leader's crash to a new leader with a higher term. */
  private void watchFailover() {
    if (failoverStart < 0) {
      return;
    }
    for (Node n : nodes.values()) {
      if (n.up
          && n.kv.raftStatus().role() == RaftNode.Role.LEADER
          && n.kv.raftStatus().term() > failoverTerm) {
        electionFailover.add((clock.nanoTime() - failoverStart) / 1_000_000);
        failoverStart = -1;
        return;
      }
    }
  }

  private Node randomNode() {
    List<Node> all = List.copyOf(nodes.values());
    return all.get(random.nextInt(all.size()));
  }

  private Node randomUpNode() {
    List<Node> up = nodes.values().stream().filter(n -> n.up).toList();
    return up.isEmpty() ? null : up.get(random.nextInt(up.size()));
  }

  private Node leader() {
    Node best = null;
    for (Node n : nodes.values()) {
      if (n.up
          && n.kv.raftStatus().role() == RaftNode.Role.LEADER
          && (best == null || n.kv.raftStatus().term() > best.kv.raftStatus().term())) {
        best = n;
      }
    }
    return best;
  }

  // -----------------------------------------------------------------------------------------------
  // Checks

  private void checkLiveness() {
    for (SimClient c : clients) {
      if (c.lastSuccess < quietStart) {
        StringBuilder state = new StringBuilder();
        for (Node n : nodes.values()) {
          state.append("\n  ").append(n.id).append(n.up ? " " : " down ");
          if (n.up) {
            RaftNode.Status s = n.kv.raftStatus();
            state
                .append(s.role())
                .append(" term ")
                .append(s.term())
                .append(" leader ")
                .append(s.leader())
                .append(" commit ")
                .append(s.commit())
                .append(" applied ")
                .append(s.applied())
                .append(" last ")
                .append(s.lastIndex());
          }
        }
        for (SimClient other : clients) {
          state
              .append("\n  client ")
              .append(other.number)
              .append(" sends to ")
              .append(other.client.leader())
              .append(", last success at ")
              .append(other.lastSuccess / 1_000_000)
              .append(" ms, ops started ")
              .append(other.opsStarted)
              .append(" finished ")
              .append(other.opsDone)
              .append(", last started at ")
              .append(other.lastCall / 1_000_000)
              .append(" ms")
              .append(other.recent.isEmpty() ? "" : "\n    " + String.join("\n    ", other.recent));
        }
        throw new AssertionError(
            "liveness: client "
                + c.number
                + " got nothing done after the faults stopped (at "
                + quietStart / 1_000_000
                + " ms):"
                + state);
      }
    }
  }

  private void checkConvergence() {
    Map<String, String> reference = null;
    NodeId referenceNode = null;
    for (Node n : nodes.values()) {
      Map<String, String> data = n.userData();
      if (reference == null) {
        reference = data;
        referenceNode = n.id;
      } else if (!reference.equals(data)) {
        throw new AssertionError(
            "replicas diverged: "
                + referenceNode
                + " has "
                + reference
                + " but "
                + n.id
                + " has "
                + data);
      }
    }
  }

  private void checkEffects() {
    for (KvStateMachine.RequestKey k : acknowledged) {
      if (!effectAt.containsKey(k)) {
        throw new AssertionError("acknowledged write " + k + " never took effect");
      }
    }
    for (KvStateMachine.RequestKey k : certainlyFailed) {
      if (effectAt.containsKey(k)) {
        throw new AssertionError(
            "write "
                + k
                + " was reported as certainly failed but took effect at index "
                + effectAt.get(k));
      }
    }
  }

  /** Every effect of a request must be the same log entry (replayed after a crash, at most). */
  private void onApplied(NodeId replica, KvStateMachine.Applied a) {
    if (!a.effect()) {
      return;
    }
    Long first = effectAt.putIfAbsent(a.request(), a.index());
    if (first != null && first != a.index()) {
      throw new AssertionError(
          "write "
              + a.request()
              + " took effect twice: at index "
              + first
              + " and, on "
              + replica
              + ", at "
              + a.index());
    }
  }

  // -----------------------------------------------------------------------------------------------
  // A node process

  private final class Node {
    final NodeId id;
    final SimFileSystem fs;
    final RandomSource rnd;
    final Duration tick;
    Life life;
    LsmEngine engine;
    ReplicatedKv kv;
    boolean up;
    boolean armed;

    Node(NodeId id) {
      this.id = id;
      this.fs = new SimFileSystem(random.split());
      this.rnd = random.split();
      // This node's timer rate, fixed for its lifetime like a machine's clock.
      this.tick = Duration.ofMillis(options.clockDrift() ? rnd.nextInt(8, 21) : 10);
    }

    void boot() {
      while (true) {
        life = new Life(scheduler);
        try {
          engine = LsmEngine.open(fs, life, DIR, ENGINE);
          ReplicatedKv.Config cfg =
              new ReplicatedKv.Config(
                  id,
                  peers,
                  RaftConfig.defaults(id),
                  KvServiceOptions.defaults(),
                  tick,
                  options.compactEvery());
          kv =
              new ReplicatedKv(
                  cfg,
                  engine,
                  net.endpoint(id),
                  clock,
                  life,
                  rnd.split(),
                  this::storageFailed,
                  ReplicatedKvSimulation.this::onApplied);
          break;
        } catch (StorageException e) {
          if (!fs.isDead()) {
            throw e;
          }
          life.end();
          fs.crash(); // the disk failed during recovery: power-cycle and try again
        }
      }
      up = true;
      armed = false;
      kv.start();
    }

    /** Crash-only: a storage error kills the process, which restarts from disk. */
    void storageFailed(Throwable t) {
      diskFailures++;
      scheduler.execute(() -> {
        crash();
        restartLater();
      });
    }

    void crash() {
      if (!up) {
        return;
      }
      up = false;
      armed = false;
      kv.close();
      life.end();
      net.endpoint(id).setHandler(null);
      fs.crash();
    }

    void restartLater() {
      scheduler.schedule(
          Duration.ofMillis(random.nextInt(100, 1500)),
          () -> {
            if (!up) {
              boot();
            }
          });
    }

    Map<String, String> userData() {
      Map<String, String> out = new TreeMap<>();
      try (KvIterator it = engine.scan(KeySpace.user(new byte[0]), KeySpace.userEnd(null))) {
        while (it.hasNext()) {
          KvEntry e = it.next();
          out.put(
              new String(KeySpace.fromUser(e.key()), StandardCharsets.UTF_8),
              new String(e.value(), StandardCharsets.UTF_8));
        }
      }
      return out;
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Clients

  private final class SimClient {
    final int number;
    final NodeId id;
    final RandomSource choices;
    final KvClient client;
    KvStateMachine.RequestKey lastWrite;
    long lastSuccess = -1;
    long opsStarted;
    long opsDone;
    long lastCall;
    final java.util.ArrayDeque<String> recent = new java.util.ArrayDeque<>();

    /** Keeps the last few attempts, for failure messages. */
    void trace(String line) {
      recent.addLast(line);
      if (recent.size() > 12) {
        recent.removeFirst();
      }
    }

    int written;

    SimClient(int number) {
      this.number = number;
      this.id = new NodeId(100 + number);
      net.endpoint(id); // so partitions can cut clients off too
      this.choices = random.split();
      List<String> seeds = new ArrayList<>(peers.values());
      this.client =
          new KvClient(
              seeds,
              address -> new SimChannel(this, nodeAt(address)),
              clock,
              scheduler,
              random.split(),
              // Attempts much shorter than the deadline: one lost message must cost a retry,
              // not the whole call.
              new ClientOptions(
                  Duration.ofSeconds(2),
                  Duration.ofMinutes(5),
                  Duration.ofMillis(400),
                  Duration.ofMillis(20),
                  Duration.ofMillis(500)));
    }

    void next() {
      if (finished) {
        return;
      }
      String key = "k" + choices.nextInt(options.keys());
      int dice = choices.nextInt(100);
      long call = clock.nanoTime();
      opsStarted++;
      lastCall = call;
      Linearizability.Kind kind;
      String value = null;
      CompletableFuture<?> op;
      if (dice < 50) {
        kind = Linearizability.Kind.PUT;
        value = "c" + number + "-" + written++;
        op = client.put(utf8(key), utf8(value));
      } else if (dice < 65) {
        kind = Linearizability.Kind.DELETE;
        op = client.delete(utf8(key));
      } else {
        kind = Linearizability.Kind.GET;
        op = client.get(utf8(key));
      }
      KvStateMachine.RequestKey request = kind == Linearizability.Kind.GET ? null : lastWrite;
      String written = value;
      op.whenComplete((result, failure) -> {
        opsDone++;
        try {
          record(kind, key, written, request, call, result, failure);
        } catch (Throwable t) {
          // An exception thrown inside a future callback is swallowed: keep it for the end,
          // or this client would silently stop.
          if (violation == null) {
            violation = t;
          }
          return;
        }
        scheduler.schedule(Duration.ofMillis(choices.nextInt(0, 30)), this::next);
      });
    }

    private void record(
        Linearizability.Kind kind,
        String key,
        String value,
        KvStateMachine.RequestKey request,
        long call,
        Object result,
        Throwable failure) {
      long now = clock.nanoTime();
      boolean write = kind != Linearizability.Kind.GET;
      if (failure == null) {
        lastSuccess = now;
        if (write) {
          writesAcknowledged++;
          acknowledged.add(request);
          if (clientFailoverPending && failoverStart < 0) {
            // The first write to succeed once a new leader exists: the old one is down, so it
            // went through the new one. That is the failover clients see.
            clientFailover.add((now - clientFailoverFrom) / 1_000_000);
            clientFailoverPending = false;
          }
          history.add(new Linearizability.Op(number, key, kind, value, call, now));
        } else {
          readsAnswered++;
          @SuppressWarnings("unchecked")
          Optional<byte[]> v = (Optional<byte[]>) result;
          history.add(
              new Linearizability.Op(
                  number,
                  key,
                  kind,
                  v.map(b -> new String(b, StandardCharsets.UTF_8)).orElse(null),
                  call,
                  now));
        }
        return;
      }
      Throwable cause = failure instanceof CompletionException ce ? ce.getCause() : failure;
      if (!(cause instanceof KvException e) || e.code() != KvException.Code.DEADLINE_EXCEEDED) {
        throw new AssertionError("client " + number + " got an unexpected failure", cause);
      }
      if (!write) {
        return; // an unanswered read changed nothing and observed nothing
      }
      if (e.outcomeUnknown()) {
        unknownOutcomes++;
        history.add(new Linearizability.Op(number, key, kind, value, call, Long.MAX_VALUE));
      } else {
        certainlyFailed.add(request);
      }
    }
  }

  private Node nodeAt(String address) {
    for (Map.Entry<NodeId, String> e : peers.entrySet()) {
      if (e.getValue().equals(address)) {
        return nodes.get(e.getKey());
      }
    }
    throw new IllegalArgumentException("no node at " + address);
  }

  /**
   * One client's link to one node: the request crosses the simulated network, the node handles it
   * with the client's deadline, and the answer crosses back, each way subject to partitions and
   * loss.
   */
  private final class SimChannel implements KvChannel {
    private final SimClient client;
    private final Node server;

    SimChannel(SimClient client, Node server) {
      this.client = client;
      this.server = server;
    }

    @Override
    public CompletableFuture<KvResponse> call(KvRequest request, Duration timeout) {
      if (request.hasRequestId()) {
        client.lastWrite =
            new KvStateMachine.RequestKey(
                request.getRequestId().getClientId(), request.getRequestId().getSeq());
      }
      CompletableFuture<KvResponse> reply = new CompletableFuture<>();
      long sentAt = clock.nanoTime();
      reply.whenComplete((r, t) ->
          client.trace(
              sentAt
                  + " -> "
                  + server.id
                  + " "
                  + request.getOpCase()
                  + ": "
                  + (r != null ? (r.hasError() ? r.getError().getKindCase() : "ok") : t)));
      long deadline = clock.nanoTime() + timeout.toNanos();
      scheduler.schedule(
          timeout,
          () ->
              reply.completeExceptionally(
                  new RpcException(RpcException.Kind.TIMEOUT, "no answer")));
      if (!server.up && random.nextBoolean()) {
        reply.completeExceptionally(
            new RpcException(RpcException.Kind.UNAVAILABLE, false, "connection refused", null));
        return reply;
      }
      if (!link(client.id, server.id)) {
        return reply;
      }
      scheduler.schedule(
          delay(),
          () -> {
            if (!server.up || !net.connected(client.id, server.id)) {
              return;
            }
            server
                .kv
                .handle(request, deadline)
                .whenComplete((response, error) -> {
                  if (response != null && link(server.id, client.id)) {
                    scheduler.schedule(delay(), () -> reply.complete(response));
                  }
                });
          });
      return reply;
    }

    private boolean link(NodeId from, NodeId to) {
      return net.connected(from, to) && random.nextInt(100) >= 2;
    }

    private Duration delay() {
      return Duration.ofMillis(random.nextInt(1, 6));
    }

    @Override
    public void close() {}
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}

package io.distdb.sim;

import com.google.protobuf.ByteString;
import io.distdb.client.ClientOptions;
import io.distdb.client.KvClient;
import io.distdb.client.KvException;
import io.distdb.common.Bytes;
import io.distdb.common.NodeId;
import io.distdb.env.RandomSource;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.kv.KeySpace;
import io.distdb.kv.KvServiceOptions;
import io.distdb.kv.KvStateMachine;
import io.distdb.kv.RangeStateMachine;
import io.distdb.kv.Ranges;
import io.distdb.kv.ReplicatedKv;
import io.distdb.kv.Store;
import io.distdb.pd.PlacementDriver;
import io.distdb.raft.Entry;
import io.distdb.raft.RaftCodec;
import io.distdb.raft.RaftConfig;
import io.distdb.raft.RaftNode;
import io.distdb.raft.Snapshot;
import io.distdb.rpc.KvChannel;
import io.distdb.rpc.KvHandler;
import io.distdb.rpc.PdClient;
import io.distdb.rpc.RpcException;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.RangeDescriptor;
import io.distdb.rpc.proto.RangeInfo;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import io.distdb.storage.StorageException;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import io.distdb.txn.GcWorker;
import io.distdb.txn.Txn;
import io.distdb.txn.TxnClient;
import io.distdb.txn.TxnException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
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
 * The Phase 6 gate: a sharded cluster under faults, while its ranges split and move, judged by what
 * its clients saw.
 *
 * <p>The cluster: a placement driver of three nodes (each a {@link ReplicatedKv} replica with a
 * {@link PlacementDriver} on top), and four or five data nodes, each a real {@link Store} on its
 * own simulated disk. Range 1 starts on nodes 1 to 3; the PD's scheduler spreads it out and splits
 * it as it grows. On top of that the run forces splits at random keys and moves of random replicas
 * to random nodes, so the key space is cut and reshuffled all the time. Clients are real {@link
 * KvClient}s routing through the PD. Faults: partitions (that cut clients and PD nodes too), power
 * cuts of data nodes and of the PD leader, disks failing mid-write, stalled timers, and a node cut
 * off from its peers but not from its clients.
 *
 * <p>Afterwards:
 *
 * <ul>
 *   <li>the history is <b>linearizable</b>, per key, across every split and move;
 *   <li>no write took effect twice (in the same range or in two ranges), every acknowledged write
 *       took effect, and none reported as certainly failed did;
 *   <li>once the faults stop, every client makes progress; then, with the scheduler paused, the
 *       ranges tile the key space, every replica of a range has the same descriptor and the same
 *       data, and no node holds two replicas with overlapping spans.
 * </ul>
 */
public final class ShardedKvSimulation {

  /**
   * What the run does.
   *
   * @param dataNodes how many data nodes
   * @param clients concurrent clients, one operation at a time each
   * @param keys how many keys they share
   * @param splitKeys the PD splits a range with more keys than this
   * @param forcedOps whether the run also forces random splits and moves
   * @param faultPeriod how long faults are injected
   * @param maxFaultInterval the longest gap between faults (the shortest is a sixth)
   * @param compactEvery how often replicas compact their logs
   * @param heartbeat how often stores report to the PD
   */
  public record Options(
      int dataNodes,
      int clients,
      int keys,
      long splitKeys,
      boolean forcedOps,
      Duration faultPeriod,
      Duration maxFaultInterval,
      int compactEvery,
      Duration heartbeat,
      boolean bank,
      Duration gcLifetime) {

    /** Knobs drawn from the seed. */
    public static Options varied(long seed) {
      RandomSource r = SplittableRandomSource.seeded(seed * 0x9E3779B97F4A7C15L + 11);
      int[] compact = {16, 64, 1000};
      long[] split = {4, 8, 16};
      return new Options(
          r.nextBoolean() ? 4 : 5,
          4,
          r.nextInt(16, 49),
          split[r.nextInt(3)],
          r.nextInt(4) != 0,
          Duration.ofSeconds(10),
          r.nextBoolean() ? Duration.ofMillis(700) : Duration.ofSeconds(2),
          compact[r.nextInt(3)],
          Duration.ofMillis(r.nextInt(150, 501)),
          false,
          Duration.ofSeconds(3));
    }

    /**
     * The Phase 7 gate: the same cluster, faults, splits and moves, with bank transfers in
     * transactions instead of single-key operations. {@code keys} is the number of accounts.
     */
    public static Options bank(long seed) {
      Options o = varied(seed);
      RandomSource r = SplittableRandomSource.seeded(seed * 0x2545F4914F6CDD1DL + 3);
      return new Options(
          o.dataNodes(),
          4,
          r.nextInt(4, 21),
          o.splitKeys() * 4,
          o.forcedOps(),
          o.faultPeriod(),
          o.maxFaultInterval(),
          o.compactEvery(),
          o.heartbeat(),
          true,
          Duration.ofMillis(r.nextInt(2000, 5001)));
    }
  }

  /** What happened, so tests can check the faults and the reshuffling were real. */
  public record Report(
      long seed,
      int operations,
      long writesAcknowledged,
      long readsAnswered,
      long unknownOutcomes,
      int ranges,
      long splits,
      long membershipChanges,
      long replicasDestroyed,
      long powerCuts,
      long pdLeaderCrashes,
      long diskFailures,
      long partitions,
      long transfers,
      long transferConflicts,
      long transferUnknown,
      long clientCrashes,
      long audits,
      long gcRounds) {}

  private static final Path DIR = Path.of("/node");
  private static final EngineOptions ENGINE =
      new EngineOptions(64 * 1024, 4096, 10, 256 * 1024, 4, 12, 1024 * 1024, 10);
  private static final Comparator<NodeId> BY_VALUE = Comparator.comparingInt(NodeId::value);

  private final long seed;
  private final Options options;
  private final RandomSource random;
  private final SimClock clock = new SimClock();
  private final SimScheduler scheduler = new SimScheduler(clock);
  private final SimNetwork net;
  private final Map<NodeId, DataNode> data = new TreeMap<>(BY_VALUE);
  private final Map<NodeId, PdNode> pds = new TreeMap<>(BY_VALUE);
  private final Map<String, Server> byAddress = new LinkedHashMap<>();
  private final Map<NodeId, String> pdPeers = new LinkedHashMap<>();
  private final List<SimClient> clients = new ArrayList<>();
  private final List<String> keys = new ArrayList<>();

  /** Where forced splits cut, when set (the SQL simulation's row keys); else the keys. */
  private List<byte[]> splitPoints = List.of();

  private final List<Linearizability.Op> history = new ArrayList<>();
  private final Map<KvStateMachine.RequestKey, String> effectAt = new HashMap<>();
  private final Set<KvStateMachine.RequestKey> acknowledged = new HashSet<>();
  private final Set<KvStateMachine.RequestKey> certainlyFailed = new HashSet<>();
  private final Set<Long> created = new HashSet<>();
  private final Set<String> configChanges = new HashSet<>();
  private final Map<String, String> committedAt = new HashMap<>();
  private final Map<Long, ArrayDeque<String>> rangeTrace = new HashMap<>();
  private boolean faulty = true;
  private boolean finished;
  private Throwable violation;
  private long quietStart;
  private long powerCuts;
  private long pdLeaderCrashes;
  private long diskFailures;
  private long partitions;
  private long destroyed;
  private long writesAcknowledged;
  private long readsAnswered;
  private long unknownOutcomes;
  private final List<BankClient> bankClients = new ArrayList<>();
  private boolean bankOpen;
  private long transfers;
  private long transferConflicts;
  private long transferUnknown;
  private long clientCrashes;
  private long audits;
  private long gcRounds;

  private ShardedKvSimulation(long seed, Options options) {
    this.seed = seed;
    this.options = Objects.requireNonNull(options);
    this.random = SplittableRandomSource.seeded(seed);
    this.net = new SimNetwork(scheduler, random.split(), SimNetwork.Faults.defaults());
    for (int i = 1; i <= 3; i++) {
      pdPeers.put(new NodeId(200 + i), "pd" + i);
    }
    for (NodeId id : pdPeers.keySet()) {
      PdNode p = new PdNode(id);
      pds.put(id, p);
      byAddress.put(pdPeers.get(id), p);
    }
    for (int i = 1; i <= options.dataNodes(); i++) {
      DataNode n = new DataNode(new NodeId(i));
      data.put(n.id, n);
      byAddress.put(n.address, n);
    }
    for (int k = 0; k < options.keys(); k++) {
      // In bank runs the keys are the accounts, so forced splits cut between them.
      keys.add(String.format(options.bank() ? "acct-%03d" : "k%03d", k));
    }
    for (int c = 0; c < options.clients(); c++) {
      if (options.bank()) {
        bankClients.add(new BankClient(c));
      } else {
        clients.add(new SimClient(c));
      }
    }
  }

  /** Runs one seed; throws an {@link AssertionError} on any violation. */
  public static Report run(long seed, Options options) {
    return new ShardedKvSimulation(seed, options).execute();
  }

  /**
   * A fault-free cluster on one simulated thread (a PD of three, three data nodes), for scripted
   * tests: the isolation anomaly tests drive transactions step by step through it.
   */
  public static final class Cluster {
    private final ShardedKvSimulation sim;
    private int clients;

    private Cluster(long seed) {
      sim =
          new ShardedKvSimulation(
              seed,
              new Options(
                  3,
                  0,
                  1,
                  1_000_000,
                  false,
                  Duration.ZERO,
                  Duration.ofSeconds(1),
                  1000,
                  Duration.ofMillis(200),
                  true,
                  Duration.ofHours(1)));
      sim.faulty = false;
      sim.pds.values().forEach(PdNode::boot);
      sim.data.values().forEach(DataNode::boot);
      sim.scheduler.runFor(Duration.ofSeconds(3));
    }

    public static Cluster start(long seed) {
      return new Cluster(seed);
    }

    /** A new client, on its own endpoint; {@code crash} decides where its transactions stop. */
    public TxnClient txnClient(TxnClient.Crash crash) {
      NodeId id = new NodeId(150 + clients++);
      sim.net.endpoint(id);
      PdClient pd =
          new PdClient(
              List.copyOf(sim.pdPeers.values()), a -> sim.new SimChannel(id, sim.server(a)));
      KvClient kv =
          new KvClient(
              pd,
              a -> sim.new SimChannel(id, sim.server(a)),
              sim.clock,
              sim.scheduler,
              sim.random.split(),
              ClientOptions.defaults());
      return new TxnClient(
          kv,
          pd,
          sim.clock,
          sim.scheduler,
          sim.random.split(),
          new TxnClient.Options(Duration.ofSeconds(1), Duration.ofSeconds(5), crash));
    }

    /** A raw KV client, for tests that send Percolator commands themselves. */
    public KvClient kvClient() {
      NodeId id = new NodeId(150 + clients++);
      sim.net.endpoint(id);
      PdClient pd =
          new PdClient(
              List.copyOf(sim.pdPeers.values()), a -> sim.new SimChannel(id, sim.server(a)));
      return new KvClient(
          pd,
          a -> sim.new SimChannel(id, sim.server(a)),
          sim.clock,
          sim.scheduler,
          sim.random.split(),
          ClientOptions.defaults());
    }

    /** Runs the simulation until {@code f} completes (at most a minute), and returns its value. */
    public <T> T await(CompletableFuture<T> f) {
      for (int i = 0; i < 6000 && !f.isDone(); i++) {
        sim.scheduler.runFor(Duration.ofMillis(10));
      }
      if (!f.isDone()) {
        throw new AssertionError("not done after a simulated minute");
      }
      try {
        return f.join();
      } catch (CompletionException e) {
        throw e.getCause() instanceof RuntimeException r ? r : e;
      }
    }

    public void run(Duration d) {
      sim.scheduler.runFor(d);
    }

    /** Simulated time since the start. */
    public Duration elapsed() {
      return Duration.ofNanos(sim.clock.nanoTime());
    }

    /** The placement driver's directory, for tests that look at placement. */
    public String ranges() {
      PdNode p = sim.pdLeader();
      if (p == null) {
        return "no PD leader";
      }
      StringBuilder b = new StringBuilder();
      for (RangeInfo r : p.pd.ranges()) {
        RangeDescriptor d = r.getDesc();
        b.append(" r")
            .append(d.getRangeId())
            .append(" v")
            .append(d.getVersion())
            .append(" from ")
            .append(
                io.distdb.common.encoding.KeyReader.toDebugString(d.getStartKey().toByteArray()))
            .append(" leader ")
            .append(r.getLeader())
            .append(" voters ")
            .append(d.getVotersList())
            .append(";");
      }
      return b.toString();
    }
  }

  /**
   * The faulty cluster of a seed, for a caller that drives its own clients step by step (the SQL
   * simulation): partitions, power cuts of data nodes and of the PD leader, disk failures, stalls,
   * forced splits and moves, and GC, all running while the caller waits on its futures. Then {@link
   * #heal()} and {@link #finish()}, which checks that replicas converged.
   */
  public static final class Chaos {
    private final ShardedKvSimulation sim;
    private int clients;

    private Chaos(long seed, Options options) {
      sim = new ShardedKvSimulation(seed, options);
      sim.faulty = false; // until faults(true)
      sim.pds.values().forEach(PdNode::boot);
      sim.data.values().forEach(DataNode::boot);
      sim.every(
          options.maxFaultInterval().dividedBy(6), options.maxFaultInterval(), sim::injectFault);
      if (options.forcedOps()) {
        sim.every(Duration.ofMillis(300), Duration.ofMillis(1500), sim::forceSplit);
        sim.every(Duration.ofMillis(300), Duration.ofMillis(1500), sim::forceMove);
      }
      sim.every(Duration.ofMillis(100), Duration.ofMillis(100), sim::checkDisjoint);
    }

    /** {@code options.clients()} must be 0: the caller brings its own. */
    public static Chaos start(long seed, Options options) {
      if (options.clients() != 0) {
        throw new IllegalArgumentException("the caller drives the clients");
      }
      return new Chaos(seed, options);
    }

    /** A transaction client on its own endpoint (partitioned like any node). */
    public TxnClient txnClient() {
      NodeId id = new NodeId(150 + clients++);
      sim.net.endpoint(id);
      PdClient pd =
          new PdClient(
              List.copyOf(sim.pdPeers.values()), a -> sim.new SimChannel(id, sim.server(a)));
      KvClient kv =
          new KvClient(
              pd,
              a -> sim.new SimChannel(id, sim.server(a)),
              sim.clock,
              sim.scheduler,
              sim.random.split(),
              ClientOptions.defaults());
      return new TxnClient(
          kv,
          pd,
          sim.clock,
          sim.scheduler,
          sim.random.split(),
          new TxnClient.Options(
              Duration.ofSeconds(1), Duration.ofSeconds(5), t -> TxnClient.CrashPoint.NONE));
    }

    /** Runs the world until {@code f} completes (at most ten simulated minutes). */
    public <T> T await(CompletableFuture<T> f) {
      for (int i = 0; i < 60_000 && !f.isDone(); i++) {
        sim.scheduler.runFor(Duration.ofMillis(10));
      }
      if (!f.isDone()) {
        throw new AssertionError("not done after ten simulated minutes");
      }
      try {
        return f.join();
      } catch (CompletionException e) {
        throw e.getCause() instanceof RuntimeException r ? r : e;
      }
    }

    public void run(Duration d) {
      sim.scheduler.runFor(d);
    }

    /**
     * Runs {@code task} on the simulation's thread after {@code delay}. A task may wait on futures
     * with {@link #await}: waiting runs the world (other tasks included) until they complete.
     */
    public void schedule(Duration delay, Runnable task) {
      sim.scheduler.schedule(delay, task);
    }

    /** Draws from the seed's randomness, for the caller's choices. */
    public RandomSource random() {
      return sim.random.split();
    }

    /** Simulated time since the start. */
    public Duration elapsed() {
      return Duration.ofNanos(sim.clock.nanoTime());
    }

    /** Forced splits cut at these keys from now on. */
    public void splitPoints(List<byte[]> points) {
      sim.splitPoints = List.copyOf(points);
    }

    /** Starts (or pauses) the partitions, power cuts, disk failures and stalls. */
    public void faults(boolean on) {
      sim.faulty = on;
    }

    /** Stops the faults, heals the network and restarts every node that is down. */
    public void heal() {
      sim.faulty = false;
      sim.net.heal();
      for (Node n : sim.allNodes()) {
        n.life().stall(1);
        if (n.isUp() && n.armed()) {
          n.crash();
        }
        if (!n.isUp()) {
          n.boot();
        }
      }
      sim.quietStart = sim.clock.nanoTime();
      sim.scheduler.runFor(Duration.ofSeconds(3));
    }

    /**
     * Freezes placement and GC, lets every replica catch up, and checks: no overlapping replicas,
     * the same entry applied at each index everywhere, and identical replicas tiling the key space.
     */
    public Report finish() {
      sim.finished = true;
      sim.pds.values().forEach(p -> p.pd.setScheduling(false));
      sim.pds.values().stream().filter(p -> p.gc != null).forEach(p -> p.gc.stop());
      sim.scheduler.runFor(Duration.ofSeconds(5));
      if (sim.violation != null) {
        throw new AssertionError("a check failed during the run", sim.violation);
      }
      sim.checkDisjoint();
      int ranges = sim.checkRanges();
      for (PdNode p : sim.pds.values()) {
        if (p.gc != null) {
          sim.gcRounds += p.gc.rounds();
        }
      }
      return new Report(
          sim.seed,
          0,
          0,
          0,
          0,
          ranges,
          sim.created.size(),
          sim.configChanges.size(),
          sim.destroyed,
          sim.powerCuts,
          sim.pdLeaderCrashes,
          sim.diskFailures,
          sim.partitions,
          0,
          0,
          0,
          0,
          0,
          sim.gcRounds);
    }
  }

  private Report execute() {
    pds.values().forEach(PdNode::boot);
    data.values().forEach(DataNode::boot);
    clients.forEach(SimClient::next);
    bankClients.forEach(BankClient::next);
    every(options.maxFaultInterval().dividedBy(6), options.maxFaultInterval(), this::injectFault);
    if (options.forcedOps()) {
      every(Duration.ofMillis(300), Duration.ofMillis(1500), this::forceSplit);
      every(Duration.ofMillis(300), Duration.ofMillis(1500), this::forceMove);
    }
    every(Duration.ofMillis(100), Duration.ofMillis(100), this::checkDisjoint);
    scheduler.runFor(options.faultPeriod());

    // Heal everything and let the cluster recover, with the scheduler still balancing.
    faulty = false;
    net.heal();
    for (Node n : allNodes()) {
      n.life().stall(1);
      if (n.isUp() && n.armed()) {
        n.crash(); // disarm a pending disk failure
      }
      if (!n.isUp()) {
        n.boot();
      }
    }
    quietStart = clock.nanoTime();
    scheduler.runFor(Duration.ofSeconds(8));
    finished = true; // clients stop starting operations
    // Freeze placement and GC (each new safe point is a log entry), and let every replica catch
    // up, so they can be compared.
    pds.values().forEach(p -> p.pd.setScheduling(false));
    pds.values().stream().filter(p -> p.gc != null).forEach(p -> p.gc.stop());
    scheduler.runFor(Duration.ofSeconds(5));

    if (violation != null) {
      throw new AssertionError("a check failed during the run", violation);
    }
    checkDisjoint();
    int ranges = checkRanges();
    if (options.bank()) {
      finalAudit();
      for (PdNode p : pds.values()) {
        if (p.gc != null) {
          gcRounds += p.gc.rounds();
        }
      }
    } else {
      checkLiveness();
      checkEffects();
    }
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
        ranges,
        created.size(),
        configChanges.size(),
        destroyed,
        powerCuts,
        pdLeaderCrashes,
        diskFailures,
        partitions,
        transfers,
        transferConflicts,
        transferUnknown,
        clientCrashes,
        audits,
        gcRounds);
  }

  // -----------------------------------------------------------------------------------------------
  // Faults, splits and moves

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

  private List<Node> allNodes() {
    List<Node> all = new ArrayList<>(data.values());
    all.addAll(pds.values());
    return all;
  }

  private void injectFault() {
    if (!faulty) {
      return;
    }
    int dice = random.nextInt(100);
    if (dice < 15) {
      List<NodeId> side = new ArrayList<>();
      for (Node n : allNodes()) {
        if (random.nextBoolean()) {
          side.add(n.id());
        }
      }
      clients.forEach(c -> {
        if (random.nextBoolean()) {
          side.add(c.id);
        }
      });
      net.heal();
      net.partition(side);
      partitions++;
    } else if (dice < 22) {
      List<Node> all = allNodes();
      net.heal();
      net.isolate(all.get(random.nextInt(all.size())).id());
      partitions++;
    } else if (dice < 32) {
      // A data node loses its peers but not its clients, and its timers stall: its ranges'
      // leaders go on believing they lead for a while.
      DataNode n = randomUpDataNode();
      if (n != null) {
        net.heal();
        net.cut(n.id, data.keySet());
        partitions++;
        stall(n);
      }
    } else if (dice < 42) {
      net.heal();
    } else if (dice < 47) {
      List<Node> up = allNodes().stream().filter(Node::isUp).toList();
      if (!up.isEmpty()) {
        stall(up.get(random.nextInt(up.size())));
      }
    } else if (dice < 72) {
      DataNode n = randomUpDataNode();
      if (n != null) {
        powerCuts++;
        n.crash();
        restartLater(n);
      }
    } else if (dice < 80) {
      PdNode leader = pdLeader();
      if (leader != null) {
        powerCuts++;
        pdLeaderCrashes++;
        leader.crash();
        restartLater(leader);
      }
    } else {
      DataNode n = randomUpDataNode();
      if (n != null && !n.armed) {
        n.armed = true;
        n.fs.crashAfter(random.nextInt(1, 60));
      }
    }
  }

  private void stall(Node n) {
    Life life = n.life();
    life.stall(random.nextInt(4, 9));
    scheduler.schedule(Duration.ofMillis(random.nextInt(500, 3000)), () -> life.stall(1));
  }

  private void restartLater(Node n) {
    scheduler.schedule(
        Duration.ofMillis(random.nextInt(100, 1500)),
        () -> {
          if (!n.isUp()) {
            n.boot();
          }
        });
  }

  private void forceSplit() {
    PdNode p = pdLeader();
    if (p == null) {
      return;
    }
    List<RangeInfo> ranges = p.pd.ranges();
    if (ranges.isEmpty()) {
      return;
    }
    RangeDescriptor d = ranges.get(random.nextInt(ranges.size())).getDesc();
    List<byte[]> points =
        splitPoints.isEmpty() ? keys.stream().map(ShardedKvSimulation::utf8).toList() : splitPoints;
    List<byte[]> inside = points.stream().filter(k -> Ranges.contains(d, k)).toList();
    if (inside.size() > 1) {
      byte[] key = inside.get(random.nextInt(1, inside.size()));
      p.pd.requestSplit(d.getRangeId(), key);
    }
  }

  private void forceMove() {
    PdNode p = pdLeader();
    if (p == null) {
      return;
    }
    List<RangeInfo> ranges = p.pd.ranges();
    if (ranges.isEmpty()) {
      return;
    }
    RangeDescriptor d = ranges.get(random.nextInt(ranges.size())).getDesc();
    if (d.getVotersCount() == 0) {
      return;
    }
    NodeId from = new NodeId(d.getVoters(random.nextInt(d.getVotersCount())));
    List<NodeId> targets = data.keySet().stream().filter(n -> !Ranges.isMember(d, n)).toList();
    if (!targets.isEmpty()) {
      p.pd.requestMove(d.getRangeId(), from, targets.get(random.nextInt(targets.size())));
    }
  }

  private DataNode randomUpDataNode() {
    List<DataNode> up = data.values().stream().filter(n -> n.up).toList();
    return up.isEmpty() ? null : up.get(random.nextInt(up.size()));
  }

  private PdNode pdLeader() {
    PdNode best = null;
    for (PdNode p : pds.values()) {
      if (p.up
          && p.kv.raftStatus().role() == RaftNode.Role.LEADER
          && (best == null || p.kv.raftStatus().term() > best.kv.raftStatus().term())) {
        best = p;
      }
    }
    return best;
  }

  // -----------------------------------------------------------------------------------------------
  // Checks

  /** No node holds two initialized replicas whose spans overlap. */
  private void checkDisjoint() {
    for (DataNode n : data.values()) {
      if (!n.up) {
        continue;
      }
      List<RangeDescriptor> spans = new ArrayList<>();
      for (Store.ReplicaView r : n.store.replicas()) {
        if (r.descriptor() == null) {
          continue;
        }
        for (RangeDescriptor other : spans) {
          if (Ranges.overlaps(other, r.descriptor())) {
            fail(
                new AssertionError(
                    n.id
                        + " holds overlapping replicas "
                        + Ranges.describe(other)
                        + " and "
                        + Ranges.describe(r.descriptor())));
          }
        }
        spans.add(r.descriptor());
      }
    }
  }

  /**
   * The newest descriptor of each range must tile the key space, and every member of each range
   * must hold that descriptor and the same data. Returns the number of ranges.
   */
  private int checkRanges() {
    Map<Long, RangeDescriptor> newest = new TreeMap<>();
    for (DataNode n : data.values()) {
      for (Store.ReplicaView r : n.store.replicas()) {
        RangeDescriptor d = r.descriptor();
        if (d == null) {
          continue;
        }
        RangeDescriptor seen = newest.get(r.rangeId());
        if (seen == null
            || d.getVersion() > seen.getVersion()
            || (d.getVersion() == seen.getVersion() && d.getConfVer() > seen.getConfVer())) {
          newest.put(r.rangeId(), d);
        }
      }
    }
    // A range whose newest descriptor is overlapped by a higher version was split away; any node
    // still showing it is an orphan that has not been told yet.
    List<RangeDescriptor> live = new ArrayList<>();
    for (RangeDescriptor d : newest.values()) {
      boolean superseded =
          newest.values().stream()
              .anyMatch(o ->
                  o.getRangeId() != d.getRangeId()
                      && Ranges.overlaps(o, d)
                      && o.getVersion() > d.getVersion());
      if (!superseded) {
        live.add(d);
      }
    }
    live.sort((a, b) ->
        Arrays.compareUnsigned(a.getStartKey().toByteArray(), b.getStartKey().toByteArray()));
    byte[] expect = new byte[0];
    for (int i = 0; i < live.size(); i++) {
      RangeDescriptor d = live.get(i);
      if (!Arrays.equals(d.getStartKey().toByteArray(), expect)) {
        throw new AssertionError(
            "the ranges do not tile the key space at "
                + Ranges.describe(d)
                + "; all: "
                + live.stream().map(Ranges::describe).toList());
      }
      if (i == live.size() - 1) {
        if (d.hasEndKey()) {
          throw new AssertionError("the last range ends: " + Ranges.describe(d));
        }
      } else {
        if (!d.hasEndKey()) {
          throw new AssertionError("a range before the last has no end: " + Ranges.describe(d));
        }
        expect = d.getEndKey().toByteArray();
      }
    }
    for (RangeDescriptor d : live) {
      Map<String, String> reference = null;
      NodeId referenceNode = null;
      List<Integer> members = new ArrayList<>(d.getVotersList());
      members.addAll(d.getLearnersList());
      for (int m : members) {
        DataNode n = data.get(new NodeId(m));
        Store.ReplicaView r =
            n.store.replicas().stream()
                .filter(v -> v.rangeId() == d.getRangeId())
                .findFirst()
                .orElse(null);
        if (r == null || !d.equals(r.descriptor())) {
          throw new AssertionError(
              n.id
                  + " should hold "
                  + Ranges.describe(d)
                  + " but has "
                  + (r == null
                      ? "no replica"
                      : Optional.ofNullable(r.descriptor())
                          .map(Ranges::describe)
                          .orElse("an uninitialized one"))
                  + ranges());
        }
        Map<String, String> userData = n.userData(d);
        if (reference == null) {
          reference = userData;
          referenceNode = n.id;
        } else if (!reference.equals(userData)) {
          throw new AssertionError(
              "replicas of "
                  + Ranges.describe(d)
                  + " diverged: "
                  + referenceNode
                  + " has "
                  + reference
                  + " but "
                  + n.id
                  + " has "
                  + userData
                  + ranges()
                  + "\n  trace of r"
                  + d.getRangeId()
                  + ":\n    "
                  + String.join(
                      "\n    ", rangeTrace.getOrDefault(d.getRangeId(), new ArrayDeque<>())));
        }
      }
    }
    return live.size();
  }

  private void checkLiveness() {
    for (SimClient c : clients) {
      if (c.lastSuccess < quietStart) {
        StringBuilder state = new StringBuilder(ranges());
        for (SimClient other : clients) {
          state
              .append("\n  client ")
              .append(other.number)
              .append(" last success at ")
              .append(other.lastSuccess / 1_000_000)
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

  /** Every replica on every node, for failure messages. */
  private String ranges() {
    StringBuilder out = new StringBuilder();
    for (DataNode n : data.values()) {
      out.append("\n  ").append(n.id).append(n.up ? "" : " down");
      if (n.up) {
        for (Store.ReplicaView r : n.store.replicas()) {
          RaftNode.Status s = r.raft();
          out.append("\n    ")
              .append(
                  r.descriptor() == null
                      ? "r" + r.rangeId() + " uninit"
                      : Ranges.describe(r.descriptor()))
              .append(" ")
              .append(s.role())
              .append(" t")
              .append(s.term())
              .append(" leader ")
              .append(s.leader())
              .append(" commit ")
              .append(s.commit())
              .append(" applied ")
              .append(r.applied())
              .append(" config@")
              .append(s.configIndex())
              .append(" ")
              .append(s.membership())
              .append(s.role() == RaftNode.Role.LEADER ? " match " + s.match() : "");
        }
      }
    }
    PdNode p = pdLeader();
    out.append("\n  PD leader ").append(p == null ? "none" : p.id);
    if (p != null) {
      p.pd
          .ranges()
          .forEach(r ->
              out.append("\n    ")
                  .append(Ranges.describe(r.getDesc()))
                  .append(" led by ")
                  .append(r.getLeader()));
      out.append("\n    ").append(p.pd.stats());
    }
    return out.toString();
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
                + " was reported as certainly failed but took effect at "
                + effectAt.get(k));
      }
    }
  }

  /**
   * State machine safety, per range: every replica that applies an index applies the same entry
   * (same term, same data).
   */
  private void onCommitted(NodeId node, long range, Entry e) {
    String key = range + "@" + e.index();
    String what = "t" + e.term() + " " + e.type() + " #" + java.util.Arrays.hashCode(e.data());
    trace(
        range,
        node
            + " applies "
            + e.index()
            + " "
            + what
            + (e.type() == Entry.Type.CONFIG ? " " + RaftCodec.decodeMembership(e.data()) : ""));
    String first = committedAt.putIfAbsent(key, what);
    if (first != null && !first.equals(what)) {
      fail(
          new AssertionError(
              "r"
                  + range
                  + " index "
                  + e.index()
                  + ": "
                  + node
                  + " applies "
                  + what
                  + " but another replica applied "
                  + first
                  + "\n  "
                  + String.join("\n  ", rangeTrace.getOrDefault(range, new ArrayDeque<>()))));
    }
  }

  private void trace(long range, String line) {
    ArrayDeque<String> t = rangeTrace.computeIfAbsent(range, k -> new ArrayDeque<>());
    t.addLast(clock.nanoTime() / 1_000_000 + " ms " + line);
    if (t.size() > 300) {
      t.removeFirst();
    }
  }

  /** Every effect of a request must be the same entry of the same range. */
  private void onApplied(NodeId node, long rangeId, RangeStateMachine.Applied a) {
    if (a.created() != null) {
      created.add(a.created().getRangeId());
    }
    if (a.membershipChanged()) {
      configChanges.add(rangeId + "@" + a.index());
    }
    if (!a.effect()) {
      return;
    }
    String at = "r" + rangeId + "@" + a.index();
    String first = effectAt.putIfAbsent(a.request(), at);
    if (first != null && !first.equals(at)) {
      fail(
          new AssertionError(
              "write "
                  + a.request()
                  + " took effect twice: at "
                  + first
                  + " and, on "
                  + node
                  + ", at "
                  + at));
    }
  }

  private void fail(Throwable t) {
    if (violation == null) {
      violation = t;
    }
    throw t instanceof AssertionError e ? e : new AssertionError(t);
  }

  // -----------------------------------------------------------------------------------------------
  // Nodes

  /** Something a channel can reach: a data node or a PD node. */
  private interface Server {
    NodeId id();

    boolean isUp();

    KvHandler handler();
  }

  private interface Node extends Server {
    Life life();

    boolean armed();

    void boot();

    void crash();
  }

  private final class DataNode implements Node {
    final NodeId id;
    final String address;
    final SimFileSystem fs;
    final RandomSource rnd;
    final Duration tick;
    Life life;
    LsmEngine engine;
    Store store;
    boolean up;
    boolean armed;

    DataNode(NodeId id) {
      this.id = id;
      this.address = "n" + id.value();
      this.fs = new SimFileSystem(random.split());
      this.rnd = random.split();
      this.tick = Duration.ofMillis(rnd.nextInt(8, 21)); // this node's clock rate
    }

    @Override
    public void boot() {
      while (true) {
        life = new Life(scheduler);
        try {
          engine = LsmEngine.open(fs, life, DIR, ENGINE);
          Store.Config cfg =
              new Store.Config(
                  id,
                  address,
                  List.of(new NodeId(1), new NodeId(2), new NodeId(3)),
                  RaftConfig.defaults(id),
                  KvServiceOptions.defaults(),
                  tick,
                  options.compactEvery(),
                  options.heartbeat());
          PdClient pd =
              new PdClient(List.copyOf(pdPeers.values()), a -> new SimChannel(id, server(a)));
          store =
              new Store(
                  cfg,
                  engine,
                  net.endpoint(id),
                  pd,
                  clock,
                  life,
                  rnd.split(),
                  this::storageFailed,
                  new Store.Observer() {
                    @Override
                    public void applied(NodeId node, long range, RangeStateMachine.Applied a) {
                      onApplied(node, range, a);
                    }

                    @Override
                    public void destroyed(NodeId node, long range) {
                      destroyed++;
                      trace(range, node + " destroyed its replica");
                    }

                    @Override
                    public void committed(NodeId node, long range, Entry e) {
                      onCommitted(node, range, e);
                    }

                    @Override
                    public void restored(NodeId node, long range, Snapshot s) {
                      trace(
                          range,
                          node
                              + " installed a snapshot at "
                              + s.index()
                              + " t"
                              + s.term()
                              + " "
                              + s.membership());
                    }
                  });
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
      store.start();
    }

    void storageFailed(Throwable t) {
      if (!(t instanceof StorageException) || !fs.isDead()) {
        // Only a failing disk may stop a store; anything else is a bug the restart would hide.
        violation = violation != null ? violation : new AssertionError(id + " stopped", t);
      }
      diskFailures++;
      scheduler.execute(() -> {
        crash();
        restartLater(this);
      });
    }

    @Override
    public void crash() {
      if (!up) {
        return;
      }
      up = false;
      armed = false;
      store.close();
      life.end();
      net.endpoint(id).setHandler(null);
      fs.crash();
    }

    Map<String, String> userData(RangeDescriptor d) {
      Map<String, String> out = new TreeMap<>();
      byte[] end = d.hasEndKey() ? d.getEndKey().toByteArray() : null;
      try (KvIterator it =
          engine.scan(KeySpace.user(d.getStartKey().toByteArray()), KeySpace.userEnd(end))) {
        while (it.hasNext()) {
          KvEntry e = it.next();
          out.put(
              new String(KeySpace.fromUser(e.key()), StandardCharsets.UTF_8),
              new String(e.value(), StandardCharsets.UTF_8));
        }
      }
      // And the transactional columns (data, locks, write records), byte for byte.
      for (byte[][] span : Ranges.transactionalSpans(d)) {
        try (KvIterator it = engine.scan(span[0], span[1])) {
          while (it.hasNext()) {
            KvEntry e = it.next();
            out.put("txn " + Bytes.toHex(e.key()), Bytes.toHex(e.value()));
          }
        }
      }
      return out;
    }

    @Override
    public NodeId id() {
      return id;
    }

    @Override
    public boolean isUp() {
      return up;
    }

    @Override
    public KvHandler handler() {
      return store;
    }

    @Override
    public Life life() {
      return life;
    }

    @Override
    public boolean armed() {
      return armed;
    }
  }

  private final class PdNode implements Node {
    final NodeId id;
    final SimFileSystem fs;
    final RandomSource rnd;
    Life life;
    LsmEngine engine;
    ReplicatedKv kv;
    PlacementDriver pd;
    GcWorker gc; // in the bank runs
    boolean up;

    PdNode(NodeId id) {
      this.id = id;
      this.fs = new SimFileSystem(random.split());
      this.rnd = random.split();
    }

    @Override
    public void boot() {
      while (true) {
        life = new Life(scheduler);
        try {
          engine = LsmEngine.open(fs, life, DIR, ENGINE);
          kv =
              new ReplicatedKv(
                  new ReplicatedKv.Config(
                      id,
                      pdPeers,
                      RaftConfig.defaults(id),
                      KvServiceOptions.defaults(),
                      Duration.ofMillis(10),
                      1000),
                  engine,
                  net.endpoint(id),
                  clock,
                  life,
                  rnd.split(),
                  t -> {
                    if (!(t instanceof StorageException) || !fs.isDead()) {
                      violation =
                          violation != null ? violation : new AssertionError(id + " stopped", t);
                    }
                    diskFailures++;
                    scheduler.execute(() -> {
                      crash();
                      restartLater(this);
                    });
                  },
                  null);
          break;
        } catch (StorageException e) {
          if (!fs.isDead()) {
            throw e;
          }
          life.end();
          fs.crash();
        }
      }
      pd =
          new PlacementDriver(
              new PlacementDriver.Config(
                  id,
                  pdPeers,
                  3,
                  options.splitKeys(),
                  Duration.ofSeconds(4),
                  Duration.ofSeconds(6),
                  4,
                  true),
              kv,
              clock,
              life,
              rnd.split());
      if (options.bank()) {
        PdClient self =
            new PdClient(List.copyOf(pdPeers.values()), a -> new SimChannel(id, server(a)));
        KvClient client =
            new KvClient(
                self,
                a -> new SimChannel(id, server(a)),
                clock,
                life,
                rnd.split(),
                new ClientOptions(
                    Duration.ofSeconds(3),
                    Duration.ofMinutes(5),
                    Duration.ofMillis(400),
                    Duration.ofMillis(20),
                    Duration.ofMillis(500)));
        TxnClient txn =
            new TxnClient(
                client,
                self,
                clock,
                life,
                rnd.split(),
                new TxnClient.Options(
                    Duration.ofSeconds(1), Duration.ofSeconds(3), t -> TxnClient.CrashPoint.NONE));
        PlacementDriver driver = pd;
        gc =
            new GcWorker(
                txn, self, life, options.gcLifetime(), Duration.ofSeconds(1), driver::isLeader);
        gc.start();
      }
      up = true;
      kv.start();
    }

    @Override
    public void crash() {
      if (!up) {
        return;
      }
      up = false;
      kv.close();
      pd.close();
      if (gc != null) {
        gc.stop();
        gcRounds += gc.rounds();
        gc = null;
      }
      life.end();
      net.endpoint(id).setHandler(null);
      fs.crash();
    }

    @Override
    public NodeId id() {
      return id;
    }

    @Override
    public boolean isUp() {
      return up;
    }

    @Override
    public KvHandler handler() {
      return pd;
    }

    @Override
    public Life life() {
      return life;
    }

    @Override
    public boolean armed() {
      return false;
    }
  }

  private Server server(String address) {
    Server s = byAddress.get(address);
    if (s == null) {
      throw new IllegalArgumentException("no node at " + address);
    }
    return s;
  }

  // -----------------------------------------------------------------------------------------------
  // Clients

  private final class SimClient {
    final int number;
    final NodeId id;
    final RandomSource choices;
    final KvClient client;
    final ArrayDeque<String> recent = new ArrayDeque<>();
    long lastSuccess = -1;
    int written;
    long writeSeq; // the client numbers its writes 1, 2, 3, ... in the order they are issued

    SimClient(int number) {
      this.number = number;
      this.id = new NodeId(100 + number);
      net.endpoint(id); // so partitions can cut clients off too
      this.choices = random.split();
      PdClient pd =
          new PdClient(List.copyOf(pdPeers.values()), a -> new SimChannel(id, server(a), this));
      this.client =
          new KvClient(
              pd,
              a -> new SimChannel(id, server(a), this),
              clock,
              scheduler,
              random.split(),
              // Attempts much shorter than the deadline: one lost message must cost a retry,
              // not the whole call.
              new ClientOptions(
                  Duration.ofSeconds(3),
                  Duration.ofMinutes(5),
                  Duration.ofMillis(400),
                  Duration.ofMillis(20),
                  Duration.ofMillis(500)));
    }

    void trace(String line) {
      recent.addLast(line);
      if (recent.size() > 12) {
        recent.removeFirst();
      }
    }

    void next() {
      if (finished) {
        return;
      }
      String key = keys.get(choices.nextInt(keys.size()));
      int dice = choices.nextInt(100);
      long call = clock.nanoTime();
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
      KvStateMachine.RequestKey request =
          kind == Linearizability.Kind.GET
              ? null
              : new KvStateMachine.RequestKey(ByteString.copyFrom(client.clientId()), ++writeSeq);
      String v = value;
      op.whenComplete((result, failure) -> {
        try {
          record(kind, key, v, request, call, result, failure);
        } catch (Throwable t) {
          if (violation == null) {
            violation = t; // swallowed inside a future callback otherwise
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
          history.add(new Linearizability.Op(number, key, kind, value, call, now));
        } else {
          readsAnswered++;
          @SuppressWarnings("unchecked")
          Optional<byte[]> got = (Optional<byte[]>) result;
          history.add(
              new Linearizability.Op(
                  number,
                  key,
                  kind,
                  got.map(b -> new String(b, StandardCharsets.UTF_8)).orElse(null),
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
        return;
      }
      if (e.outcomeUnknown()) {
        unknownOutcomes++;
        history.add(new Linearizability.Op(number, key, kind, value, call, Long.MAX_VALUE));
      } else {
        certainlyFailed.add(request);
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // The bank (Phase 7)

  private static final long OPENING_BALANCE = 100;

  private String account(int i) {
    return String.format("acct-%03d", i);
  }

  private long total() {
    return OPENING_BALANCE * options.keys();
  }

  /**
   * A bank client: transfers between random accounts in transactions, and audits (a read-only
   * transaction reading every account, whose total must never change). Client 0 first opens the
   * accounts. While faults run, one transaction in twenty gets its client "killed" after its
   * prewrites or right after its commit point, leaving locks for readers to resolve.
   */
  private final class BankClient {
    final int number;
    final NodeId id;
    final RandomSource choices;
    final RandomSource crashes;
    final TxnClient txn;
    final ArrayDeque<String> recent = new ArrayDeque<>();
    long lastSuccess = -1;

    BankClient(int number) {
      this.number = number;
      this.id = new NodeId(100 + number);
      net.endpoint(id);
      this.choices = random.split();
      this.crashes = random.split();
      PdClient pd = new PdClient(List.copyOf(pdPeers.values()), a -> new SimChannel(id, server(a)));
      KvClient kv =
          new KvClient(
              pd,
              a -> new SimChannel(id, server(a)),
              clock,
              scheduler,
              random.split(),
              new ClientOptions(
                  Duration.ofSeconds(3),
                  Duration.ofMinutes(5),
                  Duration.ofMillis(400),
                  Duration.ofMillis(20),
                  Duration.ofMillis(500)));
      this.txn =
          new TxnClient(
              kv,
              pd,
              clock,
              scheduler,
              random.split(),
              new TxnClient.Options(Duration.ofSeconds(1), Duration.ofSeconds(3), this::crashAt));
    }

    private TxnClient.CrashPoint crashAt(Txn t) {
      if (!faulty || !bankOpen) {
        return TxnClient.CrashPoint.NONE;
      }
      int dice = crashes.nextInt(100);
      return dice < 3
          ? TxnClient.CrashPoint.AFTER_PREWRITE
          : dice < 5 ? TxnClient.CrashPoint.AFTER_PRIMARY : TxnClient.CrashPoint.NONE;
    }

    void trace(String line) {
      recent.addLast(clock.nanoTime() / 1_000_000 + " ms " + line);
      if (recent.size() > 40) {
        recent.removeFirst();
      }
    }

    void next() {
      if (finished) {
        return;
      }
      if (!bankOpen) {
        if (number == 0) {
          open();
        } else {
          scheduler.schedule(Duration.ofMillis(100), this::next);
        }
        return;
      }
      if (choices.nextInt(100) < 25) {
        audit();
      } else {
        transfer();
      }
    }

    /** Opens every account with the same balance, once (a retry finds them open). */
    private void open() {
      txn.run(
              t ->
                  readAll(t)
                      .thenApply(balances -> {
                        for (int i = 0; i < options.keys(); i++) {
                          if (balances[i] < 0) {
                            t.put(utf8(account(i)), utf8("" + OPENING_BALANCE));
                          }
                        }
                        return true;
                      }),
              5)
          .whenComplete((ok, failure) -> {
            if (failure == null) {
              bankOpen = true;
              trace("opened the accounts");
            } else {
              trace("opening failed: " + failure);
            }
            scheduler.schedule(Duration.ofMillis(50), this::next);
          });
    }

    private void transfer() {
      int from = choices.nextInt(options.keys());
      int to = (from + 1 + choices.nextInt(options.keys() - 1)) % options.keys();
      long amount = choices.nextInt(1, 21);
      txn.run(
              t ->
                  t.get(utf8(account(from)))
                      .thenCompose(a ->
                          t.get(utf8(account(to)))
                              .thenApply(b -> {
                                long balanceFrom = balance(a, from);
                                long balanceTo = balance(b, to);
                                if (balanceFrom < amount) {
                                  return false;
                                }
                                t.put(utf8(account(from)), utf8("" + (balanceFrom - amount)));
                                t.put(utf8(account(to)), utf8("" + (balanceTo + amount)));
                                return true;
                              })),
              3)
          .whenComplete((moved, failure) -> {
            done("transfer " + amount + " " + from + "->" + to, failure);
            if (failure == null && moved) {
              transfers++;
            }
          });
    }

    /** Reads every account in one snapshot: the total must be exactly what the bank opened with. */
    private void audit() {
      txn.begin()
          .thenCompose(t ->
              readAll(t)
                  .thenApply(balances -> {
                    long sum = 0;
                    for (int i = 0; i < balances.length; i++) {
                      if (balances[i] < 0) {
                        throw new AssertionError(
                            "audit at " + t.startTs() + ": " + account(i) + " is missing");
                      }
                      sum += balances[i];
                    }
                    if (sum != total()) {
                      throw new AssertionError(
                          "audit at "
                              + t.startTs()
                              + ": the accounts hold "
                              + sum
                              + ", not "
                              + total()
                              + ": "
                              + java.util.Arrays.toString(balances));
                    }
                    return true;
                  }))
          .whenComplete((ok, failure) -> {
            done("audit", failure);
            if (failure == null) {
              audits++;
            }
          });
    }

    /** Every account's balance in {@code t}'s snapshot; -1 for one that does not exist. */
    private CompletableFuture<long[]> readAll(Txn t) {
      long[] out = new long[options.keys()];
      CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
      for (int i = 0; i < out.length; i++) {
        int k = i;
        chain =
            chain.thenCompose(v ->
                t.get(utf8(account(k)))
                    .thenAccept(b ->
                        out[k] =
                            b.map(x -> Long.parseLong(new String(x, StandardCharsets.UTF_8)))
                                .orElse(-1L)));
      }
      return chain.thenApply(v -> out);
    }

    private long balance(Optional<byte[]> value, int i) {
      long b =
          value
              .map(x -> Long.parseLong(new String(x, StandardCharsets.UTF_8)))
              .orElseThrow(() -> new AssertionError(account(i) + " vanished after opening"));
      if (b < 0) {
        throw new AssertionError(account(i) + " went negative: " + b);
      }
      return b;
    }

    private void done(String what, Throwable failure) {
      Throwable cause = failure instanceof CompletionException ce ? ce.getCause() : failure;
      if (cause instanceof AssertionError e) {
        fail(e);
        return;
      }
      if (cause == null) {
        lastSuccess = clock.nanoTime();
        trace(what + ": ok");
      } else if (cause instanceof TxnException e) {
        switch (e.code()) {
          case UNKNOWN -> transferUnknown++;
          case ABANDONED -> clientCrashes++;
          case CONFLICT, ABORTED, TOO_OLD -> transferConflicts++;
          case FAILED -> {}
        }
        trace(what + ": " + e.code() + " " + e.getMessage());
      } else {
        trace(what + ": " + cause);
      }
      scheduler.schedule(Duration.ofMillis(choices.nextInt(0, 30)), this::next);
    }
  }

  /** After the run: a final audit, given a few seconds, must see every unit of money. */
  private void finalAudit() {
    BankClient auditor = bankClients.getFirst();
    long[] result = new long[1];
    result[0] = Long.MIN_VALUE;
    Throwable[] error = new Throwable[1];
    auditor
        .txn
        .begin()
        .thenCompose(t -> auditor.readAll(t))
        .whenComplete((balances, failure) -> {
          if (failure != null) {
            error[0] = failure;
          } else {
            result[0] = java.util.Arrays.stream(balances).sum();
          }
        });
    scheduler.runFor(Duration.ofSeconds(5));
    if (result[0] == Long.MIN_VALUE) {
      throw new AssertionError("the final audit did not finish", error[0]);
    }
    if (result[0] != total()) {
      throw new AssertionError("the bank ended with " + result[0] + ", not " + total());
    }
    if (System.getProperty("distdb.sim.debug") != null) {
      for (BankClient c : bankClients) {
        System.out.println("DEBUG client " + c.number + "\n    " + String.join("\n    ", c.recent));
      }
    }
    for (BankClient c : bankClients) {
      if (c.lastSuccess < quietStart) {
        throw new AssertionError(
            "liveness: bank client "
                + c.number
                + " got nothing done after the faults stopped:\n    "
                + String.join("\n    ", c.recent)
                + ranges());
      }
    }
  }

  /**
   * One endpoint's link to a node: the request crosses the simulated network, the node handles it
   * with the caller's deadline, and the answer crosses back, each way subject to partitions and
   * loss.
   */
  private final class SimChannel implements KvChannel {
    private final NodeId from;
    private final Server server;
    private final SimClient client; // null for a store's PD channel

    SimChannel(NodeId from, Server server) {
      this(from, server, null);
    }

    SimChannel(NodeId from, Server server, SimClient client) {
      this.from = from;
      this.server = server;
      this.client = client;
    }

    @Override
    public CompletableFuture<KvResponse> call(KvRequest request, Duration timeout) {
      CompletableFuture<KvResponse> reply = new CompletableFuture<>();
      if (client != null) {
        long sentAt = clock.nanoTime();
        reply.whenComplete((r, t) ->
            client.trace(
                sentAt / 1_000_000
                    + " -> "
                    + server.id()
                    + " "
                    + request.getOpCase()
                    + (request.hasHeader() ? " r" + request.getHeader().getRangeId() : "")
                    + ": "
                    + (r != null ? (r.hasError() ? r.getError().getKindCase() : "ok") : t)));
      }
      long deadline = clock.nanoTime() + timeout.toNanos();
      scheduler.schedule(
          timeout,
          () ->
              reply.completeExceptionally(
                  new RpcException(RpcException.Kind.TIMEOUT, "no answer")));
      if (!server.isUp() && random.nextBoolean()) {
        reply.completeExceptionally(
            new RpcException(RpcException.Kind.UNAVAILABLE, false, "connection refused", null));
        return reply;
      }
      if (!link(from, server.id())) {
        return reply;
      }
      scheduler.schedule(
          delay(),
          () -> {
            if (!server.isUp() || !net.connected(from, server.id())) {
              return;
            }
            server
                .handler()
                .handle(request, deadline)
                .whenComplete((response, error) -> {
                  if (response != null && link(server.id(), from)) {
                    scheduler.schedule(delay(), () -> reply.complete(response));
                  }
                });
          });
      return reply;
    }

    private boolean link(NodeId a, NodeId b) {
      return net.connected(a, b) && random.nextInt(100) >= 2;
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

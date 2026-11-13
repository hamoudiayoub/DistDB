package io.distdb.sim;

import io.distdb.common.NodeId;
import io.distdb.env.RandomSource;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.Transport;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.kv.RaftStore;
import io.distdb.raft.ConfigChange;
import io.distdb.raft.Entry;
import io.distdb.raft.Membership;
import io.distdb.raft.Message;
import io.distdb.raft.PersistedState;
import io.distdb.raft.RaftCodec;
import io.distdb.raft.RaftConfig;
import io.distdb.raft.RaftNode;
import io.distdb.raft.Ready;
import io.distdb.storage.StorageException;
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
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * A whole Raft group in one thread, driven by one seed (the FoundationDB approach). Each node is a
 * real {@link RaftNode} storing its log through {@link RaftStore} in a real {@link LsmEngine} on
 * its own {@link SimFileSystem}, talking over a {@link SimNetwork}. Clients propose commands and
 * make ReadIndex reads; meanwhile the run partitions the network, cuts power to nodes, makes disks
 * fail in the middle of writes, and adds and removes members. A {@link RaftChecker} checks every
 * safety property as it goes. At the end the faults stop, and the group must recover: elect a
 * leader and commit a new entry on every member (liveness).
 *
 * <p>Everything is deterministic: rerunning a seed replays the identical run.
 */
public final class RaftSimulation {

  /**
   * What the run does.
   *
   * @param voters founding voters (n1..)
   * @param spares extra nodes that start empty and may be added as members
   * @param faultPeriod how long faults are injected
   * @param quietPeriod how long the healed group has to elect a leader and commit
   * @param membershipChanges whether to add, promote and remove members
   * @param diskFaults whether disks may fail in the middle of writes
   * @param maxFaultInterval the longest gap between injected faults (the shortest is a sixth)
   * @param raft the Raft settings for every node (its id is replaced per node)
   * @param network message-level fault rates
   */
  public record Options(
      int voters,
      int spares,
      Duration faultPeriod,
      Duration quietPeriod,
      boolean membershipChanges,
      boolean diskFaults,
      Duration maxFaultInterval,
      RaftConfig raft,
      SimNetwork.Faults network) {

    public static Options defaults() {
      return new Options(
          3,
          2,
          Duration.ofSeconds(12),
          Duration.ofSeconds(6),
          true,
          true,
          Duration.ofSeconds(2),
          RaftConfig.defaults(new NodeId(1)),
          SimNetwork.Faults.defaults());
    }

    /**
     * Settings drawn from the seed: 3 or 5 voters, 0 to 2 spares, 1 to 64 entries per message, 1 to
     * 32 messages in flight, gentle or harsh fault rates. Some bugs only show with small messages
     * or particular group sizes; varying the knobs per seed (FoundationDB calls it "buggify")
     * reaches them.
     */
    public static Options varied(long seed) {
      RandomSource r = SplittableRandomSource.seeded(seed * 0x9E3779B97F4A7C15L + 1);
      int[] batch = {1, 2, 64};
      int[] inflight = {1, 4, 32};
      RaftConfig raft =
          new RaftConfig(
              new NodeId(1), 10, 2, true, true, batch[r.nextInt(3)], inflight[r.nextInt(3)]);
      return new Options(
          r.nextBoolean() ? 3 : 5,
          r.nextInt(3),
          Duration.ofSeconds(12),
          Duration.ofSeconds(6),
          true,
          true,
          r.nextBoolean() ? Duration.ofMillis(600) : Duration.ofSeconds(2),
          raft,
          SimNetwork.Faults.defaults());
    }
  }

  /** What happened in one run, so a test can check the faults really occurred. */
  public record Report(
      long seed,
      long committed,
      long leaderChanges,
      long proposalsAccepted,
      long readsChecked,
      long powerCuts,
      long diskFailures,
      long partitions,
      long snapshotsInstalled,
      long configChanges,
      long messagesSent,
      long messagesLost,
      long leaderTransfers) {}

  private static final Duration TICK = Duration.ofMillis(10);
  private static final Path DIR = Path.of("/node");
  private static final long GROUP = 1;
  private static final int COMPACT_EVERY = 64;
  private static final EngineOptions ENGINE =
      new EngineOptions(64 * 1024, 4096, 10, 256 * 1024, 4, 12, 1024 * 1024, 10);
  private static final byte[] MARKER = "liveness-marker".getBytes(StandardCharsets.UTF_8);

  private final long seed;
  private final Options options;
  private final RandomSource random;
  private final SimClock clock = new SimClock();
  private final SimScheduler scheduler = new SimScheduler(clock);
  private final SimNetwork net;
  private final RaftChecker checker = new RaftChecker();
  private final Map<NodeId, SimNode> nodes = new TreeMap<>(Comparator.comparingInt(NodeId::value));
  private final Membership founders;
  private final Set<NodeId> retired = new HashSet<>();
  private boolean faulty = true;
  private boolean finished;
  private long proposals;
  private long proposalsAccepted;
  private long powerCuts;
  private long diskFailures;
  private long partitions;
  private long configChanges;
  private long leaderTransfers;
  private long readSeq;

  private RaftSimulation(long seed, Options options) {
    this.seed = seed;
    this.options = Objects.requireNonNull(options);
    this.random = SplittableRandomSource.seeded(seed);
    this.net = new SimNetwork(scheduler, random.split(), options.network());
    List<NodeId> voters = new ArrayList<>();
    for (int i = 1; i <= options.voters(); i++) {
      voters.add(new NodeId(i));
    }
    this.founders = new Membership(voters, List.of());
    for (int i = 1; i <= options.voters() + options.spares(); i++) {
      NodeId id = new NodeId(i);
      nodes.put(id, new SimNode(id, i <= options.voters()));
    }
  }

  /** Runs one seed; throws an {@link AssertionError} on any safety or liveness failure. */
  public static Report run(long seed, Options options) {
    return new RaftSimulation(seed, options).execute();
  }

  private Report execute() {
    nodes.values().forEach(SimNode::boot);
    every(Duration.ofMillis(5), Duration.ofMillis(30), this::propose);
    every(Duration.ofMillis(20), Duration.ofMillis(120), this::read);
    every(options.maxFaultInterval().dividedBy(6), options.maxFaultInterval(), this::injectFault);
    every(Duration.ofMillis(500), Duration.ofMillis(500), this::checkLogs);
    if (options.membershipChanges()) {
      every(Duration.ofMillis(400), Duration.ofMillis(1500), this::changeMembership);
    }
    scheduler.runFor(options.faultPeriod());

    // Stop the faults, heal everything, and give the group time to recover.
    faulty = false;
    net.heal();
    for (SimNode n : nodes.values()) {
      if (n.up && n.armed) {
        n.crash(); // disarm a pending disk failure
      }
      if (!n.up) {
        n.boot();
      }
    }
    scheduler.runFor(Duration.ofSeconds(1));
    every(Duration.ofMillis(100), Duration.ofMillis(100), this::proposeMarker);
    scheduler.runFor(options.quietPeriod());
    finished = true;
    checkLiveness();
    checkLogs();

    SimNetwork.Stats s = net.stats();
    return new Report(
        seed,
        checker.committedNow(),
        checker.leaderChanges,
        proposalsAccepted,
        checker.readsChecked,
        powerCuts,
        diskFailures,
        partitions,
        checker.snapshotsInstalled,
        configChanges,
        s.sent,
        s.dropped + s.cut,
        leaderTransfers);
  }

  // -----------------------------------------------------------------------------------------------
  // Workload and faults

  /** Runs {@code action} repeatedly, a random interval apart, until the run finishes. */
  private void every(Duration min, Duration max, Runnable action) {
    long delay = random.nextLong(min.toNanos(), max.toNanos() + 1);
    scheduler.schedule(
        Duration.ofNanos(delay),
        () -> {
          if (!finished) {
            action.run();
            every(min, max, action);
          }
        });
  }

  private void propose() {
    SimNode n = randomUpNode();
    if (n != null && n.isLeader()) {
      byte[] command = ("p" + proposals++).getBytes(StandardCharsets.UTF_8);
      if (n.raft.propose(command)) {
        proposalsAccepted++;
      }
      n.drain();
    }
  }

  private void read() {
    SimNode n = randomUpNode();
    if (n != null && n.isLeader()) {
      n.startRead();
    }
  }

  private boolean markerAccepted;
  private long markerIndex;

  private void proposeMarker() {
    if (markerAccepted) {
      return;
    }
    SimNode leader = leader();
    if (leader != null && leader.raft.propose(MARKER)) {
      markerAccepted = true;
      leader.drain();
    }
  }

  private void injectFault() {
    if (!faulty) {
      return;
    }
    List<NodeId> all = List.copyOf(nodes.keySet());
    int dice = random.nextInt(100);
    if (dice < 20) {
      List<NodeId> side = new ArrayList<>();
      for (NodeId id : all) {
        if (random.nextBoolean()) {
          side.add(id);
        }
      }
      net.heal();
      net.partition(side);
      partitions++;
    } else if (dice < 35) {
      net.heal();
      net.isolate(all.get(random.nextInt(all.size())));
      partitions++;
    } else if (dice < 60) {
      net.heal();
    } else if (dice < 85) {
      SimNode n = randomUpNode();
      if (n != null) {
        powerCuts++;
        n.crash();
        n.restartLater();
      }
    } else if (options.diskFaults()) {
      SimNode n = randomUpNode();
      if (n != null && !n.armed) {
        n.armed = true;
        n.fs.crashAfter(random.nextInt(1, 60)); // a write soon fails, and the disk plays dead
      }
    }
  }

  private void changeMembership() {
    if (!faulty) {
      return;
    }
    SimNode leader = leader();
    if (leader == null) {
      return;
    }
    RaftNode.Status s = leader.raft.status();
    Membership m = s.membership();
    if (random.nextInt(3) == 0) {
      // Hand the leadership to another voter, as a rebalancer does before moving a replica.
      List<NodeId> others = new ArrayList<>(m.voters());
      others.remove(leader.id);
      if (!others.isEmpty()
          && leader.raft.transferLeadership(others.get(random.nextInt(others.size())))) {
        leaderTransfers++;
        leader.drain();
      }
      return;
    }
    List<ConfigChange> choices = new ArrayList<>();
    for (NodeId id : nodes.keySet()) {
      if (!m.isMember(id) && !retired.contains(id)) {
        choices.add(new ConfigChange(ConfigChange.Kind.ADD_LEARNER, id));
      }
    }
    for (NodeId l : m.learners()) {
      if (s.match().getOrDefault(l, 0L) >= s.commit() - 5) {
        choices.add(new ConfigChange(ConfigChange.Kind.PROMOTE, l)); // caught up
      }
    }
    if (m.voters().size() > 3) {
      for (NodeId v : m.voters()) {
        choices.add(new ConfigChange(ConfigChange.Kind.REMOVE, v));
      }
    }
    if (choices.isEmpty()) {
      return;
    }
    ConfigChange c = choices.get(random.nextInt(choices.size()));
    if (leader.raft.proposeConfigChange(c)) {
      configChanges++;
      if (c.kind() == ConfigChange.Kind.REMOVE) {
        retired.add(c.node()); // never re-added: it keeps its stale log, like a real zombie
      }
      leader.drain();
    }
  }

  private void checkLogs() {
    Map<NodeId, RaftNode> live = new TreeMap<>(Comparator.comparingInt(NodeId::value));
    nodes.forEach((id, n) -> {
      if (n.up) {
        live.put(id, n.raft);
      }
    });
    checker.checkLogMatching(live);
  }

  private void checkLiveness() {
    SimNode leader = leader();
    if (leader == null) {
      throw new AssertionError(
          "liveness: no leader " + options.quietPeriod() + " after the faults stopped");
    }
    if (!markerAccepted) {
      throw new AssertionError("liveness: no leader accepted a proposal after the faults stopped");
    }
    if (markerIndex == 0) {
      throw new AssertionError("liveness: the entry proposed after the faults never committed");
    }
    // Every member must have reached it, whether by applying it or by installing a snapshot that
    // includes it.
    for (NodeId member : leader.raft.status().membership().members()) {
      SimNode n = nodes.get(member);
      if (!n.up || n.machine.applied() < markerIndex) {
        throw new AssertionError(
            "liveness: "
                + member
                + " has not reached index "
                + markerIndex
                + ", proposed after the faults stopped ("
                + n.raft
                + ", leader "
                + leader.raft
                + ")");
      }
    }
  }

  private SimNode randomUpNode() {
    List<SimNode> up = nodes.values().stream().filter(n -> n.up).toList();
    return up.isEmpty() ? null : up.get(random.nextInt(up.size()));
  }

  /** The up leader with the highest term, if any. */
  private SimNode leader() {
    SimNode best = null;
    for (SimNode n : nodes.values()) {
      if (n.up
          && n.isLeader()
          && (best == null || n.raft.status().term() > best.raft.status().term())) {
        best = n;
      }
    }
    return best;
  }

  // -----------------------------------------------------------------------------------------------
  // A node process

  private final class SimNode {
    final NodeId id;
    final boolean founder;
    final SimFileSystem fs;
    final Transport transport;
    final RandomSource rnd;

    Life life;
    RaftStore store;
    RaftNode raft;
    HashChain machine;
    boolean up;
    boolean armed;

    boolean wasLeader;
    long leaderTerm;
    final Map<String, Long> pendingReads = new HashMap<>();
    final List<Ready.ReadState> readStates = new ArrayList<>();

    SimNode(NodeId id, boolean founder) {
      this.id = id;
      this.founder = founder;
      this.fs = new SimFileSystem(random.split());
      this.transport = net.endpoint(id);
      this.rnd = random.split();
    }

    void boot() {
      life = new Life(scheduler);
      LsmEngine engine;
      while (true) {
        try {
          engine = LsmEngine.open(fs, life, DIR, ENGINE);
          break;
        } catch (StorageException e) {
          if (!fs.isDead()) {
            throw e;
          }
          fs.crash(); // the disk failed during recovery: power-cycle and try again
        }
      }
      store = new RaftStore(engine, GROUP);
      PersistedState state = store.load();
      machine = HashChain.fromSnapshot(state.snapshot());
      RaftConfig cfg = configFor(id);
      raft =
          founder && state.equals(PersistedState.empty())
              ? RaftNode.bootstrap(cfg, founders, rnd.split())
              : RaftNode.restart(cfg, state, state.snapshot().index(), rnd.split());
      up = true;
      armed = false;
      wasLeader = false;
      transport.setHandler(this::onMessage);
      life.schedule(TICK, this::tick);
      drain();
    }

    void onMessage(NodeId from, byte[] bytes) {
      if (!up) {
        return;
      }
      Message m = RaftCodec.decode(bytes);
      raft.step(m);
      drain();
    }

    void tick() {
      if (!up) {
        return;
      }
      raft.tick();
      drain();
      if (up) {
        life.schedule(TICK, this::tick);
      }
    }

    boolean isLeader() {
      return up && raft.status().role() == RaftNode.Role.LEADER;
    }

    /** The host loop: persist, then send, then apply, then advance. */
    void drain() {
      while (up && raft.hasReady()) {
        Ready r = raft.ready();
        try {
          store.save(r);
        } catch (StorageException e) {
          // The disk failed mid-write. Nothing of this Ready may be sent: crash-only.
          diskFailures++;
          crash();
          restartLater();
          return;
        }
        for (Message m : r.messages()) {
          transport.send(m.to(), RaftCodec.encode(m));
        }
        if (r.snapshot() != null && r.snapshotReplacesLog()) {
          machine = HashChain.fromSnapshot(r.snapshot());
          checker.onSnapshotInstalled(id, machine.applied(), machine.hash());
        }
        for (Entry e : r.committedEntries()) {
          machine.apply(e);
          checker.onApplied(id, e, machine.hash());
          if (Arrays.equals(e.data(), MARKER)) {
            markerIndex = e.index();
          }
        }
        readStates.addAll(r.readStates());
        raft.advance(r);
        serveReads();
        observeLeadership();
        if (machine.applied() - raft.status().snapshotIndex() >= COMPACT_EVERY) {
          raft.compact(machine.applied(), machine.snapshotData());
        }
      }
    }

    void startRead() {
      String context = "read-" + readSeq++;
      long committedAtStart = checker.committedNow();
      if (raft.readIndex(context.getBytes(StandardCharsets.UTF_8))) {
        pendingReads.put(context, committedAtStart);
        drain();
      }
    }

    private void serveReads() {
      readStates.removeIf(rs -> {
        if (machine.applied() < rs.index()) {
          return false; // wait until the state machine has caught up
        }
        Long start = pendingReads.remove(new String(rs.context(), StandardCharsets.UTF_8));
        if (start != null) {
          checker.onRead(id, start, machine.applied());
        }
        return true;
      });
    }

    private void observeLeadership() {
      RaftNode.Status s = raft.status();
      boolean leading = s.role() == RaftNode.Role.LEADER;
      if (leading && (!wasLeader || s.term() != leaderTerm)) {
        checker.onLeader(id, raft);
      }
      wasLeader = leading;
      leaderTerm = s.term();
    }

    /** Power cut: the process dies, and unsynced data on its disk is lost or torn. */
    void crash() {
      if (!up) {
        return;
      }
      up = false;
      armed = false;
      life.end();
      transport.setHandler(null);
      fs.crash();
      pendingReads.clear();
      readStates.clear();
    }

    void restartLater() {
      scheduler.schedule(
          Duration.ofMillis(random.nextInt(50, 2000)),
          () -> {
            if (!up) {
              boot();
            }
          });
    }
  }

  private RaftConfig configFor(NodeId id) {
    RaftConfig t = options.raft();
    return new RaftConfig(
        id,
        t.electionTicks(),
        t.heartbeatTicks(),
        t.preVote(),
        t.checkQuorum(),
        t.maxEntriesPerAppend(),
        t.maxInflightAppends());
  }
}

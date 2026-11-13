package io.distdb.pd;

import com.google.protobuf.ByteString;
import io.distdb.client.ClientOptions;
import io.distdb.client.KvClient;
import io.distdb.common.NodeId;
import io.distdb.env.Clock;
import io.distdb.env.RandomSource;
import io.distdb.env.Scheduler;
import io.distdb.kv.Ranges;
import io.distdb.kv.ReplicatedKv;
import io.distdb.raft.RaftNode;
import io.distdb.rpc.KvChannel;
import io.distdb.rpc.KvErrors;
import io.distdb.rpc.KvHandler;
import io.distdb.rpc.RpcException;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.ListRangesResponse;
import io.distdb.rpc.proto.LocateResponse;
import io.distdb.rpc.proto.Operator;
import io.distdb.rpc.proto.PdRequest;
import io.distdb.rpc.proto.PdResponse;
import io.distdb.rpc.proto.RangeDescriptor;
import io.distdb.rpc.proto.RangeInfo;
import io.distdb.rpc.proto.RangeReport;
import io.distdb.rpc.proto.ReplicaProgress;
import io.distdb.rpc.proto.SplitOperator;
import io.distdb.rpc.proto.StoreHeartbeat;
import io.distdb.rpc.proto.StoreHeartbeatResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * The placement driver (PD): where ranges are, and where they should be.
 *
 * <p>The PD is a small replicated group of its own, a Phase 5 {@link ReplicatedKv}. Its logic runs
 * on whichever PD node leads that group; the others answer {@code NotLeader}. Only one thing is
 * replicated: the <b>range-id allocator</b>, a counter advanced by conditional puts, so even two PD
 * nodes that both believe they lead never hand out the same id. Everything else is <b>soft
 * state</b>, rebuilt from heartbeats after a PD failover: the directory (each range's descriptor
 * with the highest epoch seen, and its leader), and each store's address and last heartbeat. The
 * truth is in the ranges themselves.
 *
 * <p>The <b>scheduler</b> runs as range reports arrive. Per range, the first rule that applies:
 * finish the operator in flight; promote a learner (or remove one stuck); replace a replica on a
 * dead store; add a replica if there are too few, remove one if there are too many; split a range
 * with too many keys; move a replica from the fullest store to the emptiest; hand leadership from a
 * store with too many leaders. An operator is a sequence of steps (a move is add learner, promote,
 * remove), each sent to the range's leader until its report shows it done.
 *
 * <p>Nothing the PD says is trusted for safety: the range's leader checks every operator and turns
 * it into a Raft proposal, or ignores it. A stale directory costs a client a retry.
 *
 * <p>Thread-safe: requests arrive on gRPC threads; the state is guarded by this object.
 */
public final class PlacementDriver implements KvHandler, AutoCloseable {

  private static final Comparator<NodeId> BY_VALUE = Comparator.comparingInt(NodeId::value);
  private static final byte[] NEXT_RANGE_ID = "pd/next-range-id".getBytes(StandardCharsets.UTF_8);
  private static final long ID_BLOCK = 100;
  private static final long FIRST_ID = 2; // range 1 is the one the founders bootstrap

  /**
   * How the PD places ranges.
   *
   * @param self this PD node
   * @param peers every PD node's address, this one included
   * @param replicas how many voters each range should have
   * @param splitKeys a range with more keys than this is split
   * @param storeDeadAfter a store silent for this long is dead, and its replicas are replaced
   * @param operatorTimeout an operator not done by then is dropped
   * @param maxOperators at most this many operators the scheduler starts at once
   * @param balance whether to move replicas and leaders for balance (repair always runs)
   */
  public record Config(
      NodeId self,
      Map<NodeId, String> peers,
      int replicas,
      long splitKeys,
      Duration storeDeadAfter,
      Duration operatorTimeout,
      int maxOperators,
      boolean balance) {

    public Config {
      peers = Map.copyOf(peers);
    }

    /**
     * 3 replicas, split above 10 000 keys, stores dead after 30 s, operators time out after 30 s.
     */
    public static Config defaults(NodeId self, Map<NodeId, String> peers) {
      return new Config(
          self, peers, 3, 10_000, Duration.ofSeconds(30), Duration.ofSeconds(30), 4, true);
    }
  }

  /** Counts, for tests and the simulator. */
  public record Stats(
      long operatorsStarted, long operatorsDone, long operatorsTimedOut, long splitsStarted) {}

  private enum Kind {
    ADD_LEARNER,
    PROMOTE,
    REMOVE,
    TRANSFER,
    SPLIT
  }

  private record Step(Kind kind, NodeId node) {}

  private static final class Op {
    final long rangeId;
    final List<Step> steps;
    final long created;
    final long newRangeId;
    final byte[] splitKey;
    final long baseVersion;
    int next;

    Op(long rangeId, List<Step> steps, long created, long newRangeId, byte[] splitKey, long base) {
      this.rangeId = rangeId;
      this.steps = steps;
      this.created = created;
      this.newRangeId = newRangeId;
      this.splitKey = splitKey;
      this.baseVersion = base;
    }
  }

  private static final class RangeState {
    RangeDescriptor desc;
    NodeId leader;
    long term = -1;
    RangeReport report;

    RangeState(RangeDescriptor desc) {
      this.desc = desc;
    }
  }

  private record StoreState(String address, long lastHeartbeat) {}

  private final Config config;
  private final ReplicatedKv kv;
  private final Clock clock;
  private final KvClient local;

  // Soft state, guarded by this.
  private long leaderTerm = -1;
  private long leaderSince;
  private final Map<Long, RangeState> ranges = new TreeMap<>();
  private final Map<NodeId, StoreState> stores = new TreeMap<>(BY_VALUE);
  private final Map<Long, Op> operators = new TreeMap<>();
  private final Set<String> stuckLearners = new HashSet<>();
  private long nextId;
  private long idLimit;
  private boolean allocating;
  private boolean scheduling = true;
  private long started;
  private long done;
  private long timedOut;
  private long splits;

  /**
   * @param kv this PD node's replica of the PD group; started by the caller
   */
  public PlacementDriver(
      Config config, ReplicatedKv kv, Clock clock, Scheduler scheduler, RandomSource random) {
    this.config = Objects.requireNonNull(config);
    this.kv = Objects.requireNonNull(kv);
    this.clock = Objects.requireNonNull(clock);
    this.local =
        new KvClient(
            new LocalChannel(kv, clock, scheduler),
            clock,
            scheduler,
            random,
            new ClientOptions(
                Duration.ofSeconds(2),
                Duration.ofMinutes(5),
                Duration.ofMillis(500),
                Duration.ofMillis(20),
                Duration.ofMillis(200)));
  }

  @Override
  public CompletableFuture<KvResponse> handle(KvRequest request, long deadlineNanos) {
    if (!request.hasPd()) {
      return kv.handle(request, deadlineNanos); // the PD group's own data
    }
    return CompletableFuture.completedFuture(handlePd(request.getPd()));
  }

  private synchronized KvResponse handlePd(PdRequest request) {
    RaftNode.Status s = kv.raftStatus();
    if (s.role() != RaftNode.Role.LEADER) {
      NodeId leader = s.leader();
      boolean known = leader != null && !leader.equals(config.self());
      return KvErrors.notLeader(
          known ? leader.value() : 0,
          known ? config.peers().getOrDefault(leader, "") : "",
          false,
          config.self() + " is not the PD leader");
    }
    if (s.term() != leaderTerm) {
      // Newly leading: forget what an earlier term knew; the heartbeats will say again.
      leaderTerm = s.term();
      leaderSince = clock.nanoTime();
      ranges.clear();
      stores.clear();
      operators.clear();
      stuckLearners.clear();
    }
    PdResponse.Builder response = PdResponse.newBuilder();
    switch (request.getKindCase()) {
      case HEARTBEAT -> response.setHeartbeat(onHeartbeat(request.getHeartbeat()));
      case LOCATE -> response.setLocate(locate(request.getLocate().getKey().toByteArray()));
      case LIST_RANGES -> response.setListRanges(list());
      case KIND_NOT_SET -> {
        return KvErrors.invalidArgument("empty PD request");
      }
    }
    return KvResponse.newBuilder().setPd(response).build();
  }

  // -----------------------------------------------------------------------------------------------
  // Heartbeats and the directory

  private StoreHeartbeatResponse onHeartbeat(StoreHeartbeat hb) {
    NodeId store = new NodeId(hb.getStoreId());
    stores.put(store, new StoreState(hb.getAddress(), clock.nanoTime()));
    StoreHeartbeatResponse.Builder out = StoreHeartbeatResponse.newBuilder();
    for (RangeReport report : hb.getRangesList()) {
      if (learn(report, store)) {
        Operator op = schedule(report, store);
        if (op != null) {
          out.addOperators(op);
        }
      }
    }
    ensureIds();
    stores.forEach((id, st) -> out.putStores(id.value(), st.address()));
    return out.build();
  }

  /**
   * Updates the directory from a leader's report. Returns false if the report is stale: another
   * range overlapping it has a higher version (spans only shrink, so overlapping descriptors are an
   * ancestor and a descendant, and the descendant's version is higher), or a newer leader of the
   * same range has reported.
   */
  private boolean learn(RangeReport report, NodeId leader) {
    RangeDescriptor d = report.getDesc();
    for (RangeState other : ranges.values()) {
      if (other.desc.getRangeId() != d.getRangeId()
          && Ranges.overlaps(other.desc, d)
          && other.desc.getVersion() > d.getVersion()) {
        return false;
      }
    }
    RangeState st = ranges.computeIfAbsent(d.getRangeId(), id -> new RangeState(d));
    if (report.getTerm() < st.term) {
      return false;
    }
    if (newer(d, st.desc) || d.equals(st.desc)) {
      st.desc = d;
    }
    st.leader = leader;
    st.term = report.getTerm();
    st.report = report;
    ranges
        .values()
        .removeIf(o ->
            o.desc.getRangeId() != d.getRangeId()
                && Ranges.overlaps(o.desc, d)
                && o.desc.getVersion() < d.getVersion());
    return true;
  }

  private static boolean newer(RangeDescriptor a, RangeDescriptor b) {
    return a.getVersion() > b.getVersion()
        || (a.getVersion() == b.getVersion() && a.getConfVer() > b.getConfVer());
  }

  private LocateResponse locate(byte[] key) {
    LocateResponse.Builder out = LocateResponse.newBuilder();
    RangeState best = null;
    for (RangeState st : ranges.values()) {
      if (Ranges.contains(st.desc, key)
          && (best == null || st.desc.getVersion() > best.desc.getVersion())) {
        best = st;
      }
    }
    if (best != null) {
      out.setFound(true).setRange(info(best));
      for (int n : members(best.desc)) {
        StoreState s = stores.get(new NodeId(n));
        if (s != null) {
          out.putAddresses(n, s.address());
        }
      }
    }
    return out.build();
  }

  private ListRangesResponse list() {
    ListRangesResponse.Builder out = ListRangesResponse.newBuilder();
    ranges.values().forEach(st -> out.addRanges(info(st)));
    stores.forEach((id, st) -> out.putStores(id.value(), st.address()));
    return out.build();
  }

  private static RangeInfo info(RangeState st) {
    return RangeInfo.newBuilder()
        .setDesc(st.desc)
        .setLeader(st.leader == null ? 0 : st.leader.value())
        .build();
  }

  // -----------------------------------------------------------------------------------------------
  // The scheduler

  private Operator schedule(RangeReport report, NodeId leader) {
    if (!scheduling) {
      return null;
    }
    RangeDescriptor d = report.getDesc();
    long now = clock.nanoTime();
    Op op = operators.get(d.getRangeId());
    if (op != null && now - op.created > config.operatorTimeout().toNanos()) {
      operators.remove(d.getRangeId());
      timedOut++;
      Step step = op.steps.get(op.next);
      if (step.kind() == Kind.PROMOTE) {
        stuckLearners.add(d.getRangeId() + "/" + step.node()); // it never caught up
      }
      op = null;
    }
    if (op != null) {
      while (op.next < op.steps.size() && finished(op, op.steps.get(op.next), d, leader)) {
        op.next++;
      }
      if (op.next == op.steps.size()) {
        operators.remove(d.getRangeId());
        done++;
        return null;
      }
      return emit(op, d);
    }
    if (operators.size() >= config.maxOperators()) {
      return null;
    }
    op = decide(report, leader);
    if (op == null) {
      return null;
    }
    operators.put(d.getRangeId(), op);
    started++;
    return emit(op, d);
  }

  private Op decide(RangeReport report, NodeId leader) {
    RangeDescriptor d = report.getDesc();
    long id = d.getRangeId();
    for (int l : d.getLearnersList()) {
      NodeId n = new NodeId(l);
      boolean stuck = stuckLearners.remove(id + "/" + n);
      return op(id, stuck ? step(Kind.REMOVE, n) : step(Kind.PROMOTE, n));
    }
    for (int v : d.getVotersList()) {
      NodeId n = new NodeId(v);
      if (!alive(n)) {
        NodeId target = target(d);
        if (target != null) {
          return op(id, step(Kind.ADD_LEARNER, target), step(Kind.PROMOTE, target), remove(n));
        }
        if (d.getVotersCount() > config.replicas()) {
          return op(id, remove(n));
        }
      }
    }
    if (d.getVotersCount() < config.replicas()) {
      NodeId target = target(d);
      if (target != null) {
        return op(id, step(Kind.ADD_LEARNER, target), step(Kind.PROMOTE, target));
      }
    }
    if (d.getVotersCount() > config.replicas()) {
      return op(id, remove(fullest(d, leader)));
    }
    if (report.getKeyCount() > config.splitKeys() && hasId()) {
      return split(d, new byte[0]);
    }
    if (!config.balance()) {
      return null;
    }
    Map<NodeId, Integer> counts = replicaCounts();
    NodeId target = target(d);
    NodeId from = fullest(d, null);
    if (target != null
        && from != null
        && counts.getOrDefault(from, 0) - counts.getOrDefault(target, 0) >= 2) {
      return op(id, step(Kind.ADD_LEARNER, target), step(Kind.PROMOTE, target), remove(from));
    }
    Map<NodeId, Integer> leaders = leaderCounts();
    NodeId calmest = null;
    for (int v : d.getVotersList()) {
      NodeId n = new NodeId(v);
      if (!n.equals(leader)
          && alive(n)
          && (calmest == null || leaders.getOrDefault(n, 0) < leaders.getOrDefault(calmest, 0))) {
        calmest = n;
      }
    }
    if (calmest != null
        && leaders.getOrDefault(leader, 0) - leaders.getOrDefault(calmest, 0) >= 2) {
      return op(id, step(Kind.TRANSFER, calmest));
    }
    return null;
  }

  private boolean finished(Op op, Step step, RangeDescriptor d, NodeId leader) {
    NodeId n = step.node();
    return switch (step.kind()) {
      case ADD_LEARNER -> Ranges.isMember(d, n);
      case PROMOTE -> d.getVotersList().contains(n.value());
      case REMOVE -> !Ranges.isMember(d, n);
      case TRANSFER -> leader.equals(n);
      case SPLIT -> d.getVersion() > op.baseVersion;
    };
  }

  private static Operator emit(Op op, RangeDescriptor d) {
    Step step = op.steps.get(op.next);
    Operator.Builder out = Operator.newBuilder().setRangeId(op.rangeId);
    switch (step.kind()) {
      case ADD_LEARNER -> out.setAddLearner(step.node().value());
      case PROMOTE -> out.setPromote(step.node().value());
      case REMOVE -> out.setRemove(step.node().value());
      case TRANSFER -> out.setTransferLeader(step.node().value());
      case SPLIT ->
          out.setSplit(
              SplitOperator.newBuilder()
                  .setSplitKey(ByteString.copyFrom(op.splitKey))
                  .setNewRangeId(op.newRangeId)
                  .setExpectedVersion(op.baseVersion));
    }
    return out.build();
  }

  private Op op(long rangeId, Step... steps) {
    return new Op(rangeId, List.of(steps), clock.nanoTime(), 0, new byte[0], 0);
  }

  private Op split(RangeDescriptor d, byte[] key) {
    splits++;
    return new Op(
        d.getRangeId(),
        List.of(step(Kind.SPLIT, null)),
        clock.nanoTime(),
        takeId(),
        key,
        d.getVersion());
  }

  private static Step step(Kind kind, NodeId node) {
    return new Step(kind, node);
  }

  private static Step remove(NodeId node) {
    return new Step(Kind.REMOVE, node);
  }

  private boolean alive(NodeId store) {
    StoreState s = stores.get(store);
    long dead = config.storeDeadAfter().toNanos();
    long now = clock.nanoTime();
    return s == null ? now - leaderSince < dead : now - s.lastHeartbeat() < dead;
  }

  /** The live store with the fewest replicas that does not hold {@code d}; null if none. */
  private NodeId target(RangeDescriptor d) {
    Map<NodeId, Integer> counts = replicaCounts();
    NodeId best = null;
    for (NodeId s : stores.keySet()) {
      if (alive(s)
          && !Ranges.isMember(d, s)
          && (best == null || counts.getOrDefault(s, 0) < counts.getOrDefault(best, 0))) {
        best = s;
      }
    }
    return best;
  }

  /**
   * The voter of {@code d} on the store with the most replicas, preferring one that is not {@code
   * avoid}; null if none.
   */
  private NodeId fullest(RangeDescriptor d, NodeId avoid) {
    Map<NodeId, Integer> counts = replicaCounts();
    NodeId best = null;
    for (int v : d.getVotersList()) {
      NodeId n = new NodeId(v);
      if (best == null
          || counts.getOrDefault(n, 0) > counts.getOrDefault(best, 0)
          || (best.equals(avoid) && !n.equals(avoid))) {
        best = n;
      }
    }
    return best;
  }

  private Map<NodeId, Integer> replicaCounts() {
    Map<NodeId, Integer> counts = new TreeMap<>(BY_VALUE);
    for (RangeState st : ranges.values()) {
      for (int n : members(st.desc)) {
        counts.merge(new NodeId(n), 1, Integer::sum);
      }
    }
    return counts;
  }

  private Map<NodeId, Integer> leaderCounts() {
    Map<NodeId, Integer> counts = new TreeMap<>(BY_VALUE);
    for (RangeState st : ranges.values()) {
      if (st.leader != null) {
        counts.merge(st.leader, 1, Integer::sum);
      }
    }
    return counts;
  }

  private static List<Integer> members(RangeDescriptor d) {
    List<Integer> out = new ArrayList<>(d.getVotersList());
    out.addAll(d.getLearnersList());
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Range ids

  private boolean hasId() {
    return nextId < idLimit;
  }

  private long takeId() {
    return nextId++;
  }

  /** Keeps a block of ids in hand, taken from the replicated counter by a conditional put. */
  private void ensureIds() {
    if (idLimit - nextId > ID_BLOCK / 4 || allocating) {
      return;
    }
    allocating = true;
    local
        .get(NEXT_RANGE_ID)
        .thenCompose(current -> {
          long first = current.map(PlacementDriver::readLong).orElse(FIRST_ID);
          return local
              .conditionalPut(
                  NEXT_RANGE_ID, longBytes(first + ID_BLOCK), current, Duration.ofSeconds(2))
              .thenApply(ok -> ok ? first : -1L);
        })
        .whenComplete((first, failure) -> {
          synchronized (this) {
            allocating = false;
            if (failure == null && first > 0) {
              // Ours alone: the conditional put moved the counter past them.
              nextId = first;
              idLimit = first + ID_BLOCK;
            }
          }
        });
  }

  // -----------------------------------------------------------------------------------------------
  // For the simulator and tests

  /** Whether this PD node leads the PD group now. */
  public boolean isLeader() {
    return kv.raftStatus().role() == RaftNode.Role.LEADER;
  }

  /** Starts a split of {@code rangeId} at {@code key}, as an operator; false if it can't now. */
  public synchronized boolean requestSplit(long rangeId, byte[] key) {
    RangeState st = ranges.get(rangeId);
    if (!isLeader() || st == null || operators.containsKey(rangeId) || !hasId()) {
      return false;
    }
    operators.put(rangeId, split(st.desc, key.clone()));
    started++;
    return true;
  }

  /** Starts moving {@code rangeId}'s replica from {@code from} to {@code to}; false if it can't. */
  public synchronized boolean requestMove(long rangeId, NodeId from, NodeId to) {
    RangeState st = ranges.get(rangeId);
    if (!isLeader()
        || st == null
        || operators.containsKey(rangeId)
        || !st.desc.getVotersList().contains(from.value())
        || Ranges.isMember(st.desc, to)) {
      return false;
    }
    operators.put(
        rangeId, op(rangeId, step(Kind.ADD_LEARNER, to), step(Kind.PROMOTE, to), remove(from)));
    started++;
    return true;
  }

  /** Turns the scheduler on or off; heartbeats and lookups go on either way. */
  public synchronized void setScheduling(boolean on) {
    scheduling = on;
    if (!on) {
      operators.clear();
    }
  }

  /** The directory as this node knows it. */
  public synchronized List<RangeInfo> ranges() {
    return ranges.values().stream().map(PlacementDriver::info).toList();
  }

  public synchronized Stats stats() {
    return new Stats(started, done, timedOut, splits);
  }

  /** The progress a leader reported for each replica of {@code rangeId}; for tests. */
  synchronized List<ReplicaProgress> progress(long rangeId) {
    RangeState st = ranges.get(rangeId);
    return st == null || st.report == null ? List.of() : st.report.getProgressList();
  }

  @Override
  public void close() {
    local.close();
  }

  /** A channel to this node's own PD replica, for the allocator's conditional puts. */
  private static final class LocalChannel implements KvChannel {
    private final ReplicatedKv kv;
    private final Clock clock;
    private final Scheduler scheduler;

    LocalChannel(ReplicatedKv kv, Clock clock, Scheduler scheduler) {
      this.kv = kv;
      this.clock = clock;
      this.scheduler = scheduler;
    }

    @Override
    public CompletableFuture<KvResponse> call(KvRequest request, Duration timeout) {
      CompletableFuture<KvResponse> out = new CompletableFuture<>();
      scheduler.schedule(
          timeout,
          () ->
              out.completeExceptionally(
                  new RpcException(RpcException.Kind.TIMEOUT, "no answer from the PD replica")));
      kv.handle(request, clock.nanoTime() + timeout.toNanos())
          .whenComplete((response, failure) -> {
            if (response != null) {
              out.complete(response);
            } else {
              out.completeExceptionally(
                  new RpcException(RpcException.Kind.TIMEOUT, "the PD replica gave up"));
            }
          });
      return out;
    }

    @Override
    public void close() {}
  }

  private static byte[] longBytes(long v) {
    return java.nio.ByteBuffer.allocate(8).putLong(v).array();
  }

  private static long readLong(byte[] b) {
    return java.nio.ByteBuffer.wrap(b).getLong();
  }
}

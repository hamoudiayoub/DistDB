package io.distdb.kv;

import com.google.protobuf.ByteString;
import io.distdb.common.Bytes;
import io.distdb.common.NodeId;
import io.distdb.env.Clock;
import io.distdb.env.RandomSource;
import io.distdb.env.Scheduler;
import io.distdb.env.Transport;
import io.distdb.raft.ConfigChange;
import io.distdb.raft.Entry;
import io.distdb.raft.Membership;
import io.distdb.raft.Message;
import io.distdb.raft.RaftCodec;
import io.distdb.raft.RaftConfig;
import io.distdb.raft.RaftNode;
import io.distdb.raft.Ready;
import io.distdb.rpc.KvErrors;
import io.distdb.rpc.KvHandler;
import io.distdb.rpc.PdClient;
import io.distdb.rpc.proto.Command;
import io.distdb.rpc.proto.GcCommand;
import io.distdb.rpc.proto.GetRangeRequest;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.Operator;
import io.distdb.rpc.proto.PdRequest;
import io.distdb.rpc.proto.RangeDescriptor;
import io.distdb.rpc.proto.RangeReport;
import io.distdb.rpc.proto.ReplicaProgress;
import io.distdb.rpc.proto.ScanRequest;
import io.distdb.rpc.proto.SplitCommand;
import io.distdb.rpc.proto.StoreHeartbeat;
import io.distdb.rpc.proto.StoreHeartbeatResponse;
import io.distdb.storage.KvEngine;
import io.distdb.storage.KvIterator;
import io.distdb.storage.StorageException;
import io.distdb.storage.WriteBatch;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * One node of a sharded cluster: replicas of many ranges, each its own Raft group, sharing one
 * engine, one transport and one mailbox.
 *
 * <ul>
 *   <li><b>Requests</b> name the range they were routed to and its version. The node answers {@code
 *       RangeNotFound} if it has no initialized replica of it, {@code EpochMismatch} if the version
 *       or span no longer match, {@code NotLeader} if another replica leads; otherwise the range's
 *       leader proposes the write or starts a ReadIndex read, as in Phase 5. A request with no
 *       range is routed here by key, among this node's own replicas.
 *   <li><b>Raft traffic</b> of all ranges is batched: what the replicas send to one peer while the
 *       mailbox runs goes out as one {@link StoreFrames} frame.
 *   <li><b>Replicas come and go.</b> A message for an unknown range creates an uninitialized
 *       replica (that is how a node joins a range); a split creates the right-hand range on every
 *       replica of the left one; a replica learns it was removed from a member's {@code REMOVED}
 *       notice, and destroys itself, keeping only its Raft term and vote and a tombstone.
 *   <li><b>The placement driver</b> hears from the store every heartbeat interval: a report for
 *       each range it leads. Its answer carries operators (add a learner, promote, remove, transfer
 *       leadership, split), which the range's leader checks and proposes.
 * </ul>
 *
 * <p>Invariant: the spans of a node's initialized replicas are disjoint. Splits keep it; a snapshot
 * that would break it is dropped until the replica in the way has caught up.
 */
public final class Store implements KvHandler, AutoCloseable {

  private static final System.Logger LOG = System.getLogger(Store.class.getName());
  private static final Comparator<NodeId> BY_VALUE = Comparator.comparingInt(NodeId::value);
  private static final Duration ORPHAN_PROBE_AFTER = Duration.ofSeconds(3);

  /**
   * How a store is set up.
   *
   * @param self this node
   * @param address where clients and peers reach it
   * @param founders the nodes that create the first range of a new cluster (each bootstraps it on
   *     an empty engine); a node not among them waits to be added to ranges
   * @param raft Raft timing and limits for every range (its id is ignored)
   * @param kv limits, inflight cap, session TTL
   * @param tick how much real time one Raft tick is
   * @param compactEvery compact a range's log after this many applied entries
   * @param heartbeatInterval how often the store reports to the placement driver
   */
  public record Config(
      NodeId self,
      String address,
      List<NodeId> founders,
      RaftConfig raft,
      KvServiceOptions kv,
      Duration tick,
      int compactEvery,
      Duration heartbeatInterval) {

    public Config {
      founders = List.copyOf(founders);
    }

    /**
     * 50 ms ticks (elections after 0.5 to 1 s), compaction every 1000 entries, a heartbeat every
     * second. A store ticks every range it holds, so ticks cost CPU per range, and with many
     * processes on one machine, 10 ms ticks made busy nodes call elections they did not need.
     */
    public static Config defaults(NodeId self, String address, List<NodeId> founders) {
      return new Config(
          self,
          address,
          founders,
          RaftConfig.defaults(self),
          KvServiceOptions.defaults(),
          Duration.ofMillis(50),
          1000,
          Duration.ofSeconds(1));
    }
  }

  /** Sees every entry a replica on this store applies; for simulations and tests. */
  @FunctionalInterface
  public interface Observer {
    void applied(NodeId node, long rangeId, RangeStateMachine.Applied applied);

    /** Replica {@code rangeId} on {@code node} was destroyed: this node was removed from it. */
    default void destroyed(NodeId node, long rangeId) {}

    /** Replica {@code rangeId} on {@code node} is about to apply committed entry {@code e}. */
    default void committed(NodeId node, long rangeId, Entry e) {}

    /** Replica {@code rangeId} on {@code node} installed a snapshot from its leader. */
    default void restored(NodeId node, long rangeId, io.distdb.raft.Snapshot s) {}
  }

  /** A replica as the store sees it; for the simulator, tests and debugging. */
  public record ReplicaView(
      long rangeId, RangeDescriptor descriptor, RaftNode.Status raft, long applied) {}

  private final Config config;

  /** Runs pushed-down query fragments; null until the server plugs one in. */
  private volatile io.distdb.rpc.Coprocessor coprocessor;

  private final KvEngine engine;
  private final Transport transport;
  private final PdClient pd;
  private final Clock clock;
  private final Scheduler scheduler;
  private final RandomSource random;
  private final Consumer<Throwable> onStorageFailure;
  private final Observer observer;

  // The mailbox.
  private final ConcurrentLinkedQueue<Runnable> mailbox = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean scheduled = new AtomicBoolean();
  private final AtomicInteger inflight = new AtomicInteger();

  // Everything below is touched only from mailbox tasks.
  private final TreeMap<Long, Replica> replicas = new TreeMap<>();
  private final LinkedHashSet<Replica> dirty = new LinkedHashSet<>();
  private final Map<NodeId, List<StoreFrames.Item>> outbox = new TreeMap<>(BY_VALUE);
  private final Map<NodeId, String> addresses = new ConcurrentHashMap<>(); // read by the transport
  private boolean reportScheduled;
  private long gcSafePoint; // the newest GC safe point the placement driver has announced
  private volatile boolean stopped;
  private volatile Throwable failure;

  /**
   * Opens the store on {@code engine}: loads every replica it holds, finishing any snapshot a crash
   * interrupted, and bootstraps the first range if this is a founder with an empty engine. Call
   * {@link #start()} to begin.
   *
   * @param pd the placement driver, or null for a store that never splits or moves (tests, a single
   *     node)
   */
  public Store(
      Config config,
      KvEngine engine,
      Transport transport,
      PdClient pd,
      Clock clock,
      Scheduler scheduler,
      RandomSource random,
      Consumer<Throwable> onStorageFailure,
      Observer observer) {
    this.config = Objects.requireNonNull(config);
    this.engine = Objects.requireNonNull(engine);
    this.transport = Objects.requireNonNull(transport);
    this.pd = pd;
    this.clock = Objects.requireNonNull(clock);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.random = Objects.requireNonNull(random);
    this.onStorageFailure = Objects.requireNonNull(onStorageFailure);
    this.observer = observer;
    addresses.put(config.self(), config.address());
    // Initialized replicas have a descriptor; one whose snapshot was stored but not applied yet
    // has a snapshot only.
    for (byte kind : new byte[] {KeySpace.DESCRIPTOR, KeySpace.RAFT_SNAPSHOT}) {
      byte[] prefix = KeySpace.local(kind);
      try (KvIterator it = engine.scan(prefix, Bytes.prefixEnd(prefix).orElseThrow())) {
        while (it.hasNext()) {
          long id = KeySpace.readUint64(it.next().key(), 2);
          replicas.computeIfAbsent(id, i -> new Replica(this, i));
        }
      }
    }
    if (replicas.isEmpty()
        && config.founders().contains(config.self())
        && new RaftStore(engine, 1).hardState().isEmpty()
        && RangeStateMachine.tombstone(engine, 1).isEmpty()) {
      RangeDescriptor first =
          RangeDescriptor.newBuilder()
              .setRangeId(1)
              .setStartKey(ByteString.EMPTY)
              .addAllVoters(config.founders().stream().sorted(BY_VALUE).map(NodeId::value).toList())
              .setConfVer(RangeStateMachine.INITIAL_INDEX)
              .setVersion(1)
              .build();
      WriteBatch batch = new WriteBatch();
      RangeStateMachine.bootstrap(engine, batch, first);
      engine.write(batch, true);
      replicas.put(1L, new Replica(this, 1));
    }
  }

  /** Starts receiving peer messages, ticking, and reporting to the placement driver. */
  public void start() {
    transport.setHandler((from, frame) -> submit(() -> onFrame(from, frame)));
    submit(() -> replicas.values().forEach(this::markDirty));
    scheduler.schedule(config.tick(), () -> submit(this::onTick));
    if (pd != null) {
      scheduler.schedule(config.heartbeatInterval(), this::heartbeatTimer);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Client requests

  @Override
  public CompletableFuture<KvResponse> handle(KvRequest request, long deadlineNanos) {
    if (inflight.incrementAndGet() > config.kv().maxInflightRequests()) {
      inflight.decrementAndGet();
      return CompletableFuture.completedFuture(
          KvErrors.serverBusy("more than " + config.kv().maxInflightRequests() + " in flight"));
    }
    CompletableFuture<KvResponse> result = new CompletableFuture<>();
    result.whenComplete((r, t) -> inflight.decrementAndGet());
    submit(() -> dispatch(request, deadlineNanos, result));
    return result;
  }

  private void dispatch(KvRequest request, long deadline, CompletableFuture<KvResponse> result) {
    Throwable f = failure;
    if (f != null || stopped) {
      result.complete(KvErrors.storageFailure(false, "store stopped: " + f));
      return;
    }
    if (clock.nanoTime() >= deadline) {
      result.complete(KvErrors.deadlineExceeded("deadline passed before the request was handled"));
      return;
    }
    boolean write = KvRequests.isWrite(request);
    boolean txn = KvRequests.isTxnCommand(request);
    try {
      if (write) {
        KvRequests.validateWrite(request, config.kv());
      } else if (txn) {
        KvRequests.validateTxn(request, config.kv());
      } else if (KvRequests.isRead(request)) {
        KvRequests.validateRead(request, config.kv());
      } else {
        result.complete(KvErrors.invalidArgument("request has no operation"));
        return;
      }
    } catch (KvRequests.InvalidRequest e) {
      result.complete(KvErrors.invalidArgument(e.getMessage()));
      return;
    }
    Replica r;
    if (request.hasHeader() && request.getHeader().getRangeId() != 0) {
      r = replicas.get(request.getHeader().getRangeId());
      if (r == null || !r.initialized()) {
        result.complete(
            KvErrors.rangeNotFound(
                config.self() + " has no replica of r" + request.getHeader().getRangeId()));
        return;
      }
      if (r.descriptor().getVersion() != request.getHeader().getVersion()) {
        result.complete(
            KvErrors.epochMismatch(
                "r" + r.id + " is at version " + r.descriptor().getVersion() + " now"));
        return;
      }
    } else {
      r = replicaHolding(routingKey(request));
      if (r == null) {
        result.complete(KvErrors.rangeNotFound(config.self() + " holds no range with that key"));
        return;
      }
      KvRequest clipped = clipToRange(request, r.descriptor());
      if (clipped != request) {
        // Cut at the range's end: say where to go on, or the client would think it was done.
        ByteString end = r.descriptor().getEndKey();
        CompletableFuture<KvResponse> outer = result;
        result = new CompletableFuture<>();
        result.whenComplete((response, t) -> {
          if (response == null) {
            outer.completeExceptionally(t);
            return;
          }
          outer.complete(
              response.hasScan() && !response.getScan().hasResumeKey()
                  ? response.toBuilder()
                      .setScan(response.getScan().toBuilder().setResumeKey(end))
                      .build()
                  : response);
        });
        request = clipped;
      }
    }
    if (!inSpan(request, r.descriptor())) {
      result.complete(KvErrors.epochMismatch("outside " + Ranges.describe(r.descriptor())));
      return;
    }
    if (!r.leading()) {
      result.complete(notLeader(r, false));
      return;
    }
    if (write) {
      r.propose(request, deadline, result);
    } else if (txn) {
      r.proposeTxn(request, deadline, result);
    } else {
      r.startRead(request, deadline, result);
    }
    markDirty(r);
  }

  /** Plugs in the program runner for coprocessor requests (the SQL layer's). */
  public void coprocessor(io.distdb.rpc.Coprocessor c) {
    this.coprocessor = c;
  }

  io.distdb.rpc.Coprocessor coprocessor() {
    return coprocessor;
  }

  /** Whether every key {@code request} touches is in {@code d}'s span. */
  static boolean inSpan(KvRequest request, RangeDescriptor d) {
    if (request.hasScan()) {
      ScanRequest s = request.getScan();
      return Ranges.containsSpan(
          d, s.getStartKey().toByteArray(), s.hasEndKey() ? s.getEndKey().toByteArray() : null);
    }
    if (request.hasTxnScan()) {
      var s = request.getTxnScan();
      return Ranges.containsSpan(
          d, s.getStartKey().toByteArray(), s.hasEndKey() ? s.getEndKey().toByteArray() : null);
    }
    if (request.hasCoprocessor()) {
      var s = request.getCoprocessor();
      return Ranges.containsSpan(
          d, s.getStartKey().toByteArray(), s.hasEndKey() ? s.getEndKey().toByteArray() : null);
    }
    return Ranges.contains(d, routingKey(request));
  }

  private static byte[] routingKey(KvRequest request) {
    return KvRequests.routingKey(request);
  }

  private Replica replicaHolding(byte[] key) {
    for (Replica r : replicas.values()) {
      if (r.initialized() && Ranges.contains(r.descriptor(), key)) {
        return r;
      }
    }
    return null;
  }

  /**
   * A request routed by key that scans past the end of the range holding its start is cut at the
   * range's end, for clients without a range cache (a single node, the CLI's {@code --server}).
   */
  private static KvRequest clipToRange(KvRequest request, RangeDescriptor d) {
    if (!request.hasScan() || !d.hasEndKey()) {
      return request;
    }
    ScanRequest s = request.getScan();
    if (s.hasEndKey()
        && Bytes.compare(s.getEndKey().toByteArray(), d.getEndKey().toByteArray()) <= 0) {
      return request;
    }
    return request.toBuilder().setScan(s.toBuilder().setEndKey(d.getEndKey())).build();
  }

  KvResponse notLeader(Replica r, boolean maybeApplied) {
    NodeId leader = r.raft.leader();
    boolean known = leader != null && !leader.equals(config.self());
    return KvErrors.notLeader(
        known ? leader.value() : 0,
        known ? addresses.getOrDefault(leader, "") : "",
        maybeApplied,
        config.self() + " does not lead r" + r.id);
  }

  // -----------------------------------------------------------------------------------------------
  // Raft traffic

  private void onFrame(NodeId from, byte[] frame) {
    if (stopped || failure != null) {
      return;
    }
    for (StoreFrames.Item item : StoreFrames.decode(frame)) {
      if (item.kind() == StoreFrames.PROBE) {
        Replica r = replicas.get(item.rangeId());
        if (r != null && r.initialized() && !r.isMember(from)) {
          send(
              from,
              new StoreFrames.Item(
                  r.id, StoreFrames.REMOVED, r.descriptor().getConfVer(), new byte[0]));
        }
        continue;
      }
      if (item.kind() == StoreFrames.REMOVED) {
        Replica r = replicas.get(item.rangeId());
        // Destroy only if our own log holds no configuration as new as the one that removed us:
        // then it holds none that added us back either, and we are certainly not a voter. And not
        // if a leader with a configuration at least that new has since sent to us: we are being
        // added back, and that leader may already count what we hold.
        if (r != null
            && r.raft.configIndex() < item.configIndex()
            && r.heardConfig < item.configIndex()) {
          destroy(r, item.configIndex());
        }
        continue;
      }
      Message m = RaftCodec.decode(item.payload());
      if (!m.to().equals(config.self())) {
        continue;
      }
      Replica r = replicas.get(item.rangeId());
      boolean fromLeaderOrCandidate =
          m instanceof Message.RequestVote
              || m instanceof Message.AppendEntries
              || m instanceof Message.Heartbeat
              || m instanceof Message.InstallSnapshot
              || m instanceof Message.TimeoutNow;
      if (r == null) {
        Optional<Long> tomb = RangeStateMachine.tombstone(engine, item.rangeId());
        if (!fromLeaderOrCandidate || (tomb.isPresent() && item.configIndex() <= tomb.get())) {
          continue; // a reply to nothing, or from before this node was removed
        }
        r = new Replica(this, item.rangeId());
        replicas.put(r.id, r);
      } else if (r.initialized() && !r.isMember(from)) {
        // Probably a replica this range removed, which nobody tells: tell it.
        send(
            from,
            new StoreFrames.Item(
                r.id, StoreFrames.REMOVED, r.descriptor().getConfVer(), new byte[0]));
      }
      if (fromLeaderOrCandidate) {
        r.heardConfig = Math.max(r.heardConfig, item.configIndex());
      }
      if (m instanceof Message.AppendEntries
          || m instanceof Message.Heartbeat
          || m instanceof Message.InstallSnapshot) {
        r.leaderlessTicks = 0; // a leader still counts it
      }
      if (r.recovering() && (m instanceof Message.RequestVote || m instanceof Message.TimeoutNow)) {
        // It was destroyed and added back, and its log is shorter than before: it may have lost
        // entries it acknowledged, so its vote could help elect a leader without them.
        continue;
      }
      if (m instanceof Message.InstallSnapshot is) {
        RangeDescriptor d = RangeStateMachine.descriptorOf(is.snapshot());
        if (overlapsAnother(r.id, d)) {
          continue; // a replica here has not applied a split or removal yet; the leader retries
        }
        r.raft.step(m);
        drain(r); // install it now, so the next snapshot's overlap check sees it
        continue;
      }
      r.raft.step(m);
      markDirty(r);
    }
  }

  private boolean overlapsAnother(long id, RangeDescriptor d) {
    for (Replica other : replicas.values()) {
      if (other.id != id && other.initialized() && Ranges.overlaps(other.descriptor(), d)) {
        return true;
      }
    }
    return false;
  }

  private void onTick() {
    if (stopped || failure != null) {
      return;
    }
    for (Replica r : List.copyOf(replicas.values())) {
      if (!r.recovering()) {
        r.raft.tick(); // a recovering replica must not campaign
      }
      if (r.initialized() && !r.leading()) {
        if (++r.leaderlessTicks * config.tick().toMillis() >= ORPHAN_PROBE_AFTER.toMillis()) {
          r.leaderlessTicks = 0;
          probe(r);
        }
      } else {
        r.leaderlessTicks = 0;
      }
      r.maybeSweepSessions(config.tick());
      markDirty(r);
    }
    scheduler.schedule(config.tick(), () -> submit(this::onTick));
  }

  void markDirty(Replica r) {
    dirty.add(r);
  }

  private void send(NodeId to, StoreFrames.Item item) {
    outbox.computeIfAbsent(to, k -> new ArrayList<>()).add(item);
  }

  private void flush() {
    outbox.forEach((to, items) -> transport.send(to, StoreFrames.encode(items)));
    outbox.clear();
  }

  private void drainDirty() {
    while (!dirty.isEmpty() && failure == null && !stopped) {
      Iterator<Replica> it = dirty.iterator();
      Replica r = it.next();
      it.remove();
      drain(r);
    }
  }

  /** The host loop of one replica: persist, then send, then apply, then advance. */
  private void drain(Replica r) {
    while (failure == null && !stopped && replicas.get(r.id) == r && r.raft.hasReady()) {
      Ready rd = r.raft.ready();
      r.raftStore.save(rd); // synced: nothing in this Ready may be seen before it is on disk
      long configIndex = r.raft.configIndex();
      for (Message m : rd.messages()) {
        // The stamp tells the receiver "my latest configuration, at this index, counts you", so a
        // replica being added back can tell this from a stale message. A message made before a
        // change that removed its receiver is sent after it (same Ready): it must not carry the
        // new index, or that receiver would take its own removal as proof it is a member.
        long stamp = r.raft.membership().isMember(m.to()) ? configIndex : 0;
        send(m.to(), new StoreFrames.Item(r.id, StoreFrames.RAFT, stamp, RaftCodec.encode(m)));
      }
      if (rd.snapshot() != null && rd.snapshotReplacesLog()) {
        r.machine.restore(rd.snapshot());
        if (observer != null) {
          observer.restored(config.self(), r.id, rd.snapshot());
        }
        reportSoon();
      }
      List<RangeDescriptor> created = new ArrayList<>();
      long removedAt = -1;
      boolean split = false;
      for (Entry e : rd.committedEntries()) {
        long version = r.machine.descriptor().getVersion();
        if (observer != null) {
          observer.committed(config.self(), r.id, e);
        }
        RangeStateMachine.Applied a = r.machine.apply(e);
        if (observer != null) {
          observer.applied(config.self(), r.id, a);
        }
        r.applied(a);
        if (a.created() != null) {
          created.add(a.created());
        }
        split |= r.machine.descriptor().getVersion() != version;
        if (a.membershipChanged() || split) {
          reportSoon();
        }
        if (a.membershipChanged() && !Ranges.isMember(r.descriptor(), config.self())) {
          removedAt = e.index();
        }
      }
      for (Ready.ReadState rs : rd.readStates()) {
        r.readIndexConfirmed(new String(rs.context(), StandardCharsets.UTF_8), rs.index());
      }
      r.raft.advance(rd);
      r.serveReads();
      r.watchLeadership();
      for (RangeDescriptor d : created) {
        createFromSplit(r, d);
      }
      if (removedAt >= 0 && !r.raft.membership().isMember(config.self())) {
        destroy(r, removedAt); // we applied our own removal (a leader that removed itself)
        return;
      }
      // After a split, compact at once: a snapshot sent from an older compaction would carry the
      // span from before the split, which overlaps the new range on the receiving node, so it
      // could never be installed there.
      if (split
          || r.machine.appliedIndex() - r.raft.status().snapshotIndex() >= config.compactEvery()) {
        r.raft.compact(r.machine.appliedIndex(), r.machine.snapshotData());
      }
    }
  }

  /** The split that {@code left} just applied created range {@code d} here too. */
  private void createFromSplit(Replica left, RangeDescriptor d) {
    Replica existing = replicas.get(d.getRangeId());
    if (existing != null && existing.initialized()) {
      throw new IllegalStateException(
          "split created " + Ranges.describe(d) + " but it already exists: " + existing);
    }
    // An uninitialized replica of it (created by an early message) is replaced; the split kept
    // its term and vote.
    Replica right = new Replica(this, d.getRangeId());
    replicas.put(right.id, right);
    if (left.leading()) {
      right.raft.campaignNow(); // the left range's leader is the natural first leader here
    }
    markDirty(right);
  }

  /**
   * Destroys replica {@code r}: its data, sessions, log and descriptor go, in one synced batch; its
   * term and vote stay, with a tombstone recording {@code confVer}, the configuration that removed
   * this node.
   */
  private void destroy(Replica r, long confVer) {
    WriteBatch batch = new WriteBatch();
    r.machine.destroy(batch);
    r.raftStore.destroy(batch);
    // The tombstone also remembers how long the log was: this replica may have acknowledged entries
    // up to there, and a candidate still using an older configuration would count its vote.
    long held = Math.max(r.raft.lastIndex(), RangeStateMachine.tombstoneLastIndex(engine, r.id));
    batch.put(
        RangeStateMachine.tombstoneKey(r.id), RangeStateMachine.tombstoneValue(confVer, held));
    engine.write(batch, true);
    replicas.remove(r.id);
    dirty.remove(r);
    if (observer != null) {
      observer.destroyed(config.self(), r.id);
    }
    // A proposed write may still commit through the others: its outcome is unknown.
    r.failAll(
        KvErrors.notLeader(0, "", true, "r" + r.id + " was removed from " + config.self()),
        KvErrors.rangeNotFound("r" + r.id + " was removed from " + config.self()));
  }

  // -----------------------------------------------------------------------------------------------
  // The placement driver

  private void heartbeatTimer() {
    if (stopped) {
      return;
    }
    submit(this::sendHeartbeat);
    scheduler.schedule(config.heartbeatInterval(), this::heartbeatTimer);
  }

  /**
   * Asks the placement driver who the members of {@code r}'s range are now, and asks each of them
   * whether it still counts this node. A replica that hears from no leader may simply be
   * partitioned; or it may be an orphan, removed without being told, whose old configuration's
   * members no longer even host the range, so its own messages reach nobody who could answer {@code
   * REMOVED}. The PD only says whom to ask; the members decide, under the usual rule.
   */
  private void probe(Replica r) {
    if (pd == null) {
      return;
    }
    long id = r.id;
    pd.call(
            PdRequest.newBuilder().setGetRange(GetRangeRequest.newBuilder().setRangeId(id)).build(),
            config.heartbeatInterval())
        .whenComplete((response, error) -> {
          if (response == null || !response.getGetRange().getFound()) {
            return;
          }
          submit(() -> {
            Replica now = replicas.get(id);
            if (now == null || stopped || failure != null) {
              return;
            }
            response
                .getGetRange()
                .getAddressesMap()
                .forEach((n, a) -> addresses.putIfAbsent(new NodeId(n), a));
            RangeDescriptor d = response.getGetRange().getRange().getDesc();
            List<Integer> members = new ArrayList<>(d.getVotersList());
            members.addAll(d.getLearnersList());
            for (int n : members) {
              if (n != config.self().value()) {
                send(
                    new NodeId(n),
                    new StoreFrames.Item(
                        id, StoreFrames.PROBE, now.raft.configIndex(), new byte[0]));
              }
            }
          });
        });
  }

  /** Reports to the placement driver soon, after a split or membership change. */
  private void reportSoon() {
    if (pd == null || reportScheduled) {
      return;
    }
    reportScheduled = true;
    scheduler.schedule(
        Duration.ofMillis(20),
        () ->
            submit(() -> {
              reportScheduled = false;
              sendHeartbeat();
            }));
  }

  private void sendHeartbeat() {
    if (pd == null || stopped || failure != null) {
      return;
    }
    StoreHeartbeat.Builder hb =
        StoreHeartbeat.newBuilder().setStoreId(config.self().value()).setAddress(config.address());
    int count = 0;
    for (Replica r : replicas.values()) {
      if (!r.initialized()) {
        continue;
      }
      count++;
      if (!r.leading()) {
        continue;
      }
      RaftNode.Status s = r.raft.status();
      RangeReport.Builder report =
          RangeReport.newBuilder()
              .setDesc(r.descriptor())
              .setTerm(s.term())
              .setCommit(s.commit())
              .setKeyCount(keyCount(r.descriptor()));
      s.match()
          .forEach((node, match) ->
              report.addProgress(
                  ReplicaProgress.newBuilder().setNode(node.value()).setMatch(match)));
      hb.addRanges(report);
    }
    hb.setReplicaCount(count);
    pd.call(PdRequest.newBuilder().setHeartbeat(hb).build(), config.heartbeatInterval())
        .whenComplete((response, error) -> {
          if (response != null && response.hasHeartbeat()) {
            submit(() -> onHeartbeatResponse(response.getHeartbeat()));
          }
        });
  }

  private void onHeartbeatResponse(StoreHeartbeatResponse response) {
    if (stopped || failure != null) {
      return;
    }
    response.getStoresMap().forEach((id, address) -> addresses.put(new NodeId(id), address));
    gcSafePoint = Math.max(gcSafePoint, response.getGcSafePoint());
    for (Replica r : replicas.values()) {
      // Every range drops what no transaction can read any more, through its own log.
      if (r.initialized()
          && r.leading()
          && r.machine.limits().safePoint() < gcSafePoint
          && r.gcProposed < gcSafePoint
          && r.raft.propose(
              Command.newBuilder()
                  .setGc(GcCommand.newBuilder().setSafePoint(gcSafePoint))
                  .build()
                  .toByteArray())) {
        r.gcProposed = gcSafePoint;
        markDirty(r);
      }
    }
    for (Operator op : response.getOperatorsList()) {
      Replica r = replicas.get(op.getRangeId());
      if (r != null && r.initialized() && r.leading()) {
        execute(r, op);
        markDirty(r);
      }
    }
  }

  /**
   * Carries out one operator, if it still makes sense: the placement driver's view may be stale,
   * and nothing here relies on it for safety. Raft refuses unsafe membership changes, and a split
   * is ignored unless the range still has the version the operator was made for.
   */
  private void execute(Replica r, Operator op) {
    Membership m = r.raft.membership();
    switch (op.getKindCase()) {
      case ADD_LEARNER -> {
        NodeId n = new NodeId(op.getAddLearner());
        if (!m.isMember(n)) {
          change(r, new ConfigChange(ConfigChange.Kind.ADD_LEARNER, n));
        }
      }
      case PROMOTE -> {
        NodeId n = new NodeId(op.getPromote());
        RaftNode.Status s = r.raft.status();
        if (m.isLearner(n) && s.match().getOrDefault(n, 0L) >= s.commit()) {
          change(r, new ConfigChange(ConfigChange.Kind.PROMOTE, n));
        }
      }
      case REMOVE -> {
        NodeId n = new NodeId(op.getRemove());
        if (n.equals(config.self())) {
          handOver(r); // a leader that removed itself would leave the range leaderless a while
        } else if (m.isMember(n)) {
          change(r, new ConfigChange(ConfigChange.Kind.REMOVE, n));
        }
      }
      case TRANSFER_LEADER -> r.raft.transferLeadership(new NodeId(op.getTransferLeader()));
      case SPLIT -> {
        RangeDescriptor d = r.descriptor();
        if (d.getVersion() != op.getSplit().getExpectedVersion()) {
          return;
        }
        byte[] key =
            op.getSplit().getSplitKey().isEmpty()
                ? middleKey(d)
                : op.getSplit().getSplitKey().toByteArray();
        if (key == null
            || !Ranges.contains(d, key)
            || Bytes.compare(key, d.getStartKey().toByteArray()) == 0) {
          return;
        }
        r.raft.propose(
            Command.newBuilder()
                .setSplit(
                    SplitCommand.newBuilder()
                        .setSplitKey(ByteString.copyFrom(key))
                        .setNewRangeId(op.getSplit().getNewRangeId())
                        .setExpectedVersion(d.getVersion()))
                .build()
                .toByteArray());
      }
      case KIND_NOT_SET -> {}
    }
  }

  /** Carries out {@code op} as if the placement driver had sent it; for tests. */
  void operate(Operator op) {
    submit(() -> {
      Replica r = replicas.get(op.getRangeId());
      if (r != null && r.initialized() && r.leading()) {
        execute(r, op);
        markDirty(r);
      }
    });
  }

  private static void change(Replica r, ConfigChange c) {
    try {
      r.raft.proposeConfigChange(c); // false: a change is in flight; the PD will ask again
    } catch (IllegalArgumentException impossible) {
      LOG.log(System.Logger.Level.DEBUG, "ignoring " + c + " for " + r + ": " + impossible);
    }
  }

  /** Hands the leadership to the most caught-up other voter. */
  private void handOver(Replica r) {
    RaftNode.Status s = r.raft.status();
    NodeId best = null;
    for (NodeId v : s.membership().voters()) {
      if (!v.equals(config.self())
          && (best == null || s.match().getOrDefault(v, 0L) > s.match().getOrDefault(best, 0L))) {
        best = v;
      }
    }
    if (best != null) {
      r.raft.transferLeadership(best);
    }
  }

  /** Plain keys plus transactional versions and locks in {@code d}'s span. */
  private long keyCount(RangeDescriptor d) {
    List<byte[][]> spans = new ArrayList<>(Percolator.spans(d).subList(1, 3));
    spans.add(Ranges.userSpan(d));
    long n = 0;
    for (byte[][] span : spans) {
      try (KvIterator it = engine.scan(span[0], span[1])) {
        while (it.hasNext()) {
          it.next();
          n++;
        }
      }
    }
    return n;
  }

  /** The middle key of {@code d}'s data, or null if it has fewer than two keys. */
  private byte[] middleKey(RangeDescriptor d) {
    java.util.TreeSet<byte[]> keys = new java.util.TreeSet<>(Bytes.COMPARATOR);
    byte[][] span = Ranges.userSpan(d);
    try (KvIterator it = engine.scan(span[0], span[1])) {
      while (it.hasNext()) {
        keys.add(KeySpace.fromUser(it.next().key()));
      }
    }
    byte[][] writes = Percolator.spans(d).get(2);
    try (KvIterator it = engine.scan(writes[0], writes[1])) {
      while (it.hasNext()) {
        keys.add(Percolator.userKeyOf(it.next().key()));
      }
    }
    List<byte[]> sorted = new ArrayList<>(keys);
    return sorted.size() < 2 ? null : sorted.get(sorted.size() / 2);
  }

  // -----------------------------------------------------------------------------------------------
  // The mailbox

  void submit(Runnable task) {
    mailbox.add(task);
    if (scheduled.compareAndSet(false, true)) {
      scheduler.execute(this::runMailbox);
    }
  }

  private void runMailbox() {
    while (true) {
      Runnable task;
      while ((task = mailbox.poll()) != null) {
        try {
          task.run();
          drainDirty();
        } catch (StorageException e) {
          failStop(e);
        } catch (RuntimeException e) {
          LOG.log(System.Logger.Level.ERROR, "store " + config.self() + " failed", e);
          failStop(e);
        }
      }
      if (failure == null && !stopped) {
        flush(); // everything this run produced, one frame per peer
      }
      scheduled.set(false);
      // A task may have arrived after the poll above and before the flag was cleared.
      if (mailbox.isEmpty() || !scheduled.compareAndSet(false, true)) {
        return;
      }
    }
  }

  private void failStop(Throwable e) {
    if (failure != null) {
      return;
    }
    failure = e;
    transport.setHandler(null);
    outbox.clear();
    for (Replica r : replicas.values()) {
      // A proposed write may still commit through the others: its outcome is unknown.
      r.failAll(
          KvErrors.storageFailure(true, e.toString()),
          KvErrors.storageFailure(false, e.toString()));
    }
    onStorageFailure.accept(e);
  }

  // -----------------------------------------------------------------------------------------------

  Config config() {
    return config;
  }

  KvEngine engine() {
    return engine;
  }

  Clock clock() {
    return clock;
  }

  Scheduler scheduler() {
    return scheduler;
  }

  RandomSource random() {
    return random;
  }

  /** The address of node {@code id} as the placement driver last told it, or null. Thread-safe. */
  public String addressOf(NodeId id) {
    return addresses.get(id);
  }

  /**
   * Every replica on this store. Not thread-safe: call it from the store's scheduler thread (the
   * simulator's only thread), or for debugging.
   */
  public List<ReplicaView> replicas() {
    List<ReplicaView> out = new ArrayList<>();
    for (Replica r : replicas.values()) {
      out.add(new ReplicaView(r.id, r.descriptor(), r.raft.status(), r.machine.appliedIndex()));
    }
    return out;
  }

  /** Stops ticking and receiving. The engine is the caller's to close, after this. */
  @Override
  public void close() {
    stopped = true;
    transport.setHandler(null);
  }
}

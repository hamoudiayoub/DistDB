package io.distdb.kv;

import io.distdb.common.NodeId;
import io.distdb.env.Clock;
import io.distdb.env.RandomSource;
import io.distdb.env.Scheduler;
import io.distdb.env.Transport;
import io.distdb.raft.Entry;
import io.distdb.raft.Membership;
import io.distdb.raft.Message;
import io.distdb.raft.PersistedState;
import io.distdb.raft.RaftCodec;
import io.distdb.raft.RaftConfig;
import io.distdb.raft.RaftNode;
import io.distdb.raft.Ready;
import io.distdb.rpc.KvErrors;
import io.distdb.rpc.KvHandler;
import io.distdb.rpc.proto.Command;
import io.distdb.rpc.proto.ExpireSessions;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.WriteCommand;
import io.distdb.storage.KvEngine;
import io.distdb.storage.StorageException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A replica of the KV service: one member of a Raft group that replicates every write.
 *
 * <ul>
 *   <li><b>Writes</b> go to the leader, which proposes them and answers once the entry is applied
 *       here: by then it is committed, so on a majority of disks.
 *   <li><b>Reads</b> go to the leader too, via ReadIndex: the leader confirms with a majority that
 *       it still leads, waits until it has applied up to its commit index as of the read, then
 *       reads its local engine. That makes reads linearizable without writing them to the log.
 *   <li>Anywhere else, requests get {@code NotLeader} with the leader's address, if known.
 * </ul>
 *
 * <p><b>Threading.</b> {@link RaftNode} is not thread-safe, but requests arrive on gRPC threads,
 * peer messages on transport threads, and ticks on timers. Everything that touches Raft goes
 * through one mailbox and runs one task at a time on the {@link Scheduler} (an actor). In the
 * simulator, where the scheduler has one thread anyway, the same code runs unchanged.
 *
 * <p><b>Crash safety.</b> Each {@link Ready} is stored by {@link RaftStore} (synced) before its
 * messages are sent. The state machine records its applied index with every apply, so a restart
 * resumes exactly where it stopped. A storage error stops the replica for good (crash-only): the
 * process should exit and restart.
 */
public final class ReplicatedKv implements KvHandler, AutoCloseable {

  private static final System.Logger LOG = System.getLogger(ReplicatedKv.class.getName());
  private static final long GROUP = 1;

  /**
   * How this replica is set up.
   *
   * @param self this node
   * @param peers every founding member's client address, this node included
   * @param raft Raft settings (its id must be {@code self})
   * @param kv limits, inflight cap, session TTL
   * @param tick how much real time one Raft tick is
   * @param compactEvery compact the log after this many applied entries
   */
  public record Config(
      NodeId self,
      Map<NodeId, String> peers,
      RaftConfig raft,
      KvServiceOptions kv,
      Duration tick,
      int compactEvery) {

    public Config {
      peers = Map.copyOf(peers);
      if (!raft.id().equals(self) || !peers.containsKey(self)) {
        throw new IllegalArgumentException("self must be in peers and match the Raft id");
      }
    }

    /** 10 ms ticks, so elections after 100 to 200 ms; compaction every 1000 entries. */
    public static Config defaults(NodeId self, Map<NodeId, String> peers) {
      return new Config(
          self,
          peers,
          RaftConfig.defaults(self),
          KvServiceOptions.defaults(),
          Duration.ofMillis(10),
          1000);
    }
  }

  /** Sees every entry this replica applies; for simulations and tests. */
  @FunctionalInterface
  public interface ApplyObserver {
    void applied(NodeId replica, KvStateMachine.Applied applied);
  }

  private final Config config;
  private final KvEngine engine;
  private final Transport transport;
  private final Clock clock;
  private final Scheduler scheduler;
  private final Consumer<Throwable> onStorageFailure;
  private final ApplyObserver observer;
  private final RaftStore store;
  private final KvStateMachine machine;
  private final RaftNode raft;

  // The mailbox.
  private final ConcurrentLinkedQueue<Runnable> mailbox = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean scheduled = new AtomicBoolean();
  private final AtomicInteger inflight = new AtomicInteger();

  // Everything below is touched only from mailbox tasks.
  private final Map<KvStateMachine.RequestKey, List<CompletableFuture<KvResponse>>> writes =
      new HashMap<>();
  private final Map<String, PendingRead> reads = new LinkedHashMap<>();
  private long readSeq;
  private boolean wasLeader;
  private int ticksSinceSweep;
  private volatile boolean stopped;
  private volatile Throwable failure;
  private volatile RaftNode.Status published; // for readers on other threads

  private static final class PendingRead {
    final KvRequest request;
    final long deadline;
    final CompletableFuture<KvResponse> future;
    long index = -1; // set once the leadership check passed

    PendingRead(KvRequest request, long deadline, CompletableFuture<KvResponse> future) {
      this.request = request;
      this.deadline = deadline;
      this.future = future;
    }
  }

  /**
   * Opens the replica on {@code engine}: loads the Raft log, brings the state machine level with
   * the stored snapshot if a crash interrupted installing one, and joins (or founds) the group.
   * Call {@link #start()} to begin.
   */
  public ReplicatedKv(
      Config config,
      KvEngine engine,
      Transport transport,
      Clock clock,
      Scheduler scheduler,
      RandomSource random,
      Consumer<Throwable> onStorageFailure,
      ApplyObserver observer) {
    this.config = Objects.requireNonNull(config);
    this.engine = Objects.requireNonNull(engine);
    this.transport = Objects.requireNonNull(transport);
    this.clock = Objects.requireNonNull(clock);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.onStorageFailure = Objects.requireNonNull(onStorageFailure);
    this.observer = observer;
    this.store = new RaftStore(engine, GROUP);
    this.machine = new KvStateMachine(engine, GROUP, config.kv());
    PersistedState state = store.load();
    if (machine.appliedIndex() < state.snapshot().index()) {
      // A snapshot from the leader was stored, but the crash came before it was applied.
      machine.restore(state.snapshot());
    }
    if (state.equals(PersistedState.empty())) {
      List<NodeId> founders = new ArrayList<>(config.peers().keySet());
      this.raft = RaftNode.bootstrap(config.raft(), new Membership(founders, List.of()), random);
    } else {
      this.raft = RaftNode.restart(config.raft(), state, machine.appliedIndex(), random);
    }
    this.published = raft.status();
  }

  /** Starts receiving peer messages and ticking. */
  public void start() {
    transport.setHandler((from, bytes) -> submit(() -> onMessage(bytes)));
    submit(this::drain); // a bootstrapped node persists its membership first
    scheduleTick();
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
      result.complete(KvErrors.storageFailure(false, "replica stopped: " + f));
      return;
    }
    if (clock.nanoTime() >= deadline) {
      result.complete(KvErrors.deadlineExceeded("deadline passed before the request was handled"));
      return;
    }
    boolean write = KvRequests.isWrite(request);
    try {
      if (write) {
        KvRequests.validateWrite(request, config.kv());
      } else if (request.hasGet() || request.hasScan()) {
        KvRequests.validateRead(request, config.kv());
      } else {
        result.complete(KvErrors.invalidArgument("request has no operation"));
        return;
      }
    } catch (KvRequests.InvalidRequest e) {
      result.complete(KvErrors.invalidArgument(e.getMessage()));
      return;
    }
    if (raft.status().role() != RaftNode.Role.LEADER) {
      result.complete(notLeader(false));
      return;
    }
    if (write) {
      propose(request, deadline, result);
    } else {
      startRead(request, deadline, result);
    }
  }

  private void propose(KvRequest request, long deadline, CompletableFuture<KvResponse> result) {
    Command command =
        Command.newBuilder()
            .setWrite(
                WriteCommand.newBuilder()
                    .setRequest(request)
                    .setProposeTimeMillis(clock.wallTimeMillis()))
            .build();
    if (!raft.propose(command.toByteArray())) {
      result.complete(notLeader(false));
      return;
    }
    KvStateMachine.RequestKey key = KvStateMachine.RequestKey.of(request);
    writes.computeIfAbsent(key, k -> new ArrayList<>()).add(result);
    // Past the deadline the client has given up. The entry may still commit, so no answer is the
    // honest one; after a grace period, just stop holding the request.
    long grace = deadline == NO_DEADLINE ? Long.MAX_VALUE : deadline - clock.nanoTime();
    if (grace != Long.MAX_VALUE) {
      scheduler.schedule(
          Duration.ofNanos(grace).plusSeconds(1),
          () ->
              submit(() -> {
                List<CompletableFuture<KvResponse>> waiting = writes.get(key);
                if (waiting != null && waiting.remove(result) && waiting.isEmpty()) {
                  writes.remove(key);
                }
                result.cancel(false);
              }));
    }
    drain();
  }

  private void startRead(KvRequest request, long deadline, CompletableFuture<KvResponse> result) {
    String context = "read-" + readSeq++;
    if (!raft.readIndex(context.getBytes(StandardCharsets.UTF_8))) {
      result.complete(notLeader(false));
      return;
    }
    reads.put(context, new PendingRead(request, deadline, result));
    if (deadline != NO_DEADLINE) {
      long remaining = Math.max(0, deadline - clock.nanoTime());
      scheduler.schedule(
          Duration.ofNanos(remaining),
          () ->
              submit(() -> {
                PendingRead p = reads.remove(context);
                if (p != null) {
                  p.future.complete(KvErrors.deadlineExceeded("read timed out"));
                }
              }));
    }
    drain();
  }

  // -----------------------------------------------------------------------------------------------
  // The Raft loop

  private void onMessage(byte[] bytes) {
    if (stopped || failure != null) {
      return;
    }
    Message m = RaftCodec.decode(bytes);
    raft.step(m);
    drain();
  }

  private void scheduleTick() {
    scheduler.schedule(config.tick(), () -> submit(this::onTick));
  }

  private void onTick() {
    if (stopped || failure != null) {
      return;
    }
    raft.tick();
    if (raft.status().role() == RaftNode.Role.LEADER
        && ++ticksSinceSweep * config.tick().toMillis()
            >= config.kv().sessionSweepInterval().toMillis()) {
      ticksSinceSweep = 0;
      // Session expiry is a replicated command carrying the leader's time, so every replica
      // forgets the same sessions at the same index.
      raft.propose(
          Command.newBuilder()
              .setExpireSessions(
                  ExpireSessions.newBuilder()
                      .setNowMillis(clock.wallTimeMillis())
                      .setTtlMillis(config.kv().sessionTtl().toMillis()))
              .build()
              .toByteArray());
    }
    drain();
    scheduleTick();
  }

  /** The host loop: persist, then send, then apply, then advance; until nothing is left. */
  private void drain() {
    while (failure == null && !stopped && raft.hasReady()) {
      Ready r = raft.ready();
      store.save(r); // synced: nothing in this Ready may be seen before it is on disk
      for (Message m : r.messages()) {
        transport.send(m.to(), RaftCodec.encode(m));
      }
      if (r.snapshot() != null && r.snapshotReplacesLog()) {
        machine.restore(r.snapshot());
      }
      for (Entry e : r.committedEntries()) {
        KvStateMachine.Applied a = machine.apply(e);
        if (observer != null) {
          observer.applied(config.self(), a);
        }
        if (a.request() != null) {
          List<CompletableFuture<KvResponse>> waiting = writes.remove(a.request());
          if (waiting != null) {
            waiting.forEach(w -> w.complete(a.response()));
          }
        }
      }
      for (Ready.ReadState rs : r.readStates()) {
        PendingRead p = reads.get(new String(rs.context(), StandardCharsets.UTF_8));
        if (p != null) {
          p.index = rs.index();
        }
      }
      raft.advance(r);
      serveReads();
      watchLeadership();
      if (machine.appliedIndex() - raft.status().snapshotIndex() >= config.compactEvery()) {
        raft.compact(machine.appliedIndex(), machine.snapshotData());
      }
    }
  }

  private void serveReads() {
    Iterator<PendingRead> it = reads.values().iterator();
    while (it.hasNext()) {
      PendingRead p = it.next();
      if (p.index < 0 || machine.appliedIndex() < p.index) {
        continue; // leadership not confirmed yet, or not applied that far yet
      }
      it.remove();
      p.future.complete(
          p.request.hasGet()
              ? KvReads.get(engine, p.request.getGet())
              : KvReads.scan(engine, p.request.getScan(), config.kv(), clock, p.deadline));
    }
  }

  /** On losing the leadership, redirect everyone still waiting. */
  private void watchLeadership() {
    boolean leading = raft.status().role() == RaftNode.Role.LEADER;
    if (wasLeader && !leading) {
      writes.values().forEach(ws -> ws.forEach(w -> w.complete(notLeader(true))));
      writes.clear();
      reads.values().forEach(p -> p.future.complete(notLeader(false)));
      reads.clear();
    }
    wasLeader = leading;
  }

  private KvResponse notLeader(boolean maybeApplied) {
    NodeId leader = raft.status().leader();
    String address = leader == null ? "" : config.peers().getOrDefault(leader, "");
    return KvErrors.notLeader(
        leader == null || leader.equals(config.self()) ? 0 : leader.value(),
        leader == null || leader.equals(config.self()) ? "" : address,
        maybeApplied,
        config.self() + " is not the leader");
  }

  // -----------------------------------------------------------------------------------------------
  // The mailbox

  private void submit(Runnable task) {
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
          published = raft.status();
        } catch (StorageException e) {
          failStop(e);
        } catch (RuntimeException e) {
          LOG.log(System.Logger.Level.ERROR, "replica " + config.self() + " failed", e);
          failStop(e);
        }
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
    // A proposed write may still commit through the others: its outcome is unknown.
    writes
        .values()
        .forEach(ws -> ws.forEach(w -> w.complete(KvErrors.storageFailure(true, e.toString()))));
    writes.clear();
    reads.values().forEach(p -> p.future.complete(KvErrors.storageFailure(false, e.toString())));
    reads.clear();
    onStorageFailure.accept(e);
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * A snapshot of the Raft state as of the last task this replica ran; safe to call from any thread
   * (the placement driver asks it whether it leads).
   */
  public RaftNode.Status raftStatus() {
    return published;
  }

  /** Stops ticking and receiving. The engine is the caller's to close, after this. */
  @Override
  public void close() {
    stopped = true;
    transport.setHandler(null);
  }
}

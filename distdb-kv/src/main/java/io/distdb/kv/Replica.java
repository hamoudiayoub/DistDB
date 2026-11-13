package io.distdb.kv;

import io.distdb.common.NodeId;
import io.distdb.raft.PersistedState;
import io.distdb.raft.RaftConfig;
import io.distdb.raft.RaftNode;
import io.distdb.rpc.KvErrors;
import io.distdb.rpc.KvHandler;
import io.distdb.rpc.proto.Command;
import io.distdb.rpc.proto.ExpireSessions;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.RangeDescriptor;
import io.distdb.rpc.proto.TxnCommand;
import io.distdb.rpc.proto.WriteCommand;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * One range's replica on one node: its Raft node, its durable Raft state, its state machine, and
 * the client requests waiting on it. Everything here runs in the {@link Store}'s mailbox.
 */
final class Replica {

  final long id;
  final RaftStore raftStore;
  final RangeStateMachine machine;
  final RaftNode raft;

  private final Store store;

  private final Map<KvStateMachine.RequestKey, List<CompletableFuture<KvResponse>>> writes =
      new HashMap<>();
  private final Map<String, PendingRead> reads = new LinkedHashMap<>();
  // Transactional commands carry no request id; the proposer tags each with a random id.
  private final Map<Long, CompletableFuture<KvResponse>> commands = new HashMap<>();
  private long readSeq;
  private boolean wasLeader;

  /**
   * The newest configuration index heard from a leader or candidate that sent to this replica (so
   * counts it as a member). In memory only: it just spares a replica being added back from a stale
   * {@code REMOVED} notice; safety comes from the replica's own log.
   */
  long heardConfig;

  /** The newest GC safe point this replica has proposed, so it proposes each only once. */
  long gcProposed;

  /**
   * Ticks since a leader last sent to this replica. After a while it probes the range's members: an
   * orphaned learner never campaigns and keeps believing in its old leader, so only this tells.
   */
  int leaderlessTicks;

  /**
   * How long the log of this node's destroyed replica of this range was (from its tombstone), or 0.
   * Until the log is that long again, this replica neither votes nor campaigns.
   */
  private final long mustReach;

  private int ticksSinceSweep;

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
   * Loads range {@code id}'s replica from the engine: an initialized one, an uninitialized one, or
   * one whose snapshot was stored but not yet applied when the node crashed (it is applied now).
   */
  Replica(Store store, long id) {
    this.store = store;
    this.id = id;
    this.raftStore = new RaftStore(store.engine(), id);
    this.machine =
        new RangeStateMachine(store.engine(), store.config().self(), id, store.config().kv());
    PersistedState state = raftStore.load();
    if (state.snapshot().index() > machine.appliedIndex()) {
      machine.restore(state.snapshot());
    }
    RaftConfig t = store.config().raft();
    RaftConfig cfg =
        new RaftConfig(
            store.config().self(),
            t.electionTicks(),
            t.heartbeatTicks(),
            t.preVote(),
            t.checkQuorum(),
            t.maxEntriesPerAppend(),
            t.maxInflightAppends());
    this.raft = RaftNode.restart(cfg, state, machine.appliedIndex(), store.random().split());
    this.mustReach = RangeStateMachine.tombstoneLastIndex(store.engine(), id);
  }

  /**
   * Whether this replica was destroyed, added back, and has not yet caught up to the log it had: it
   * may have acknowledged entries it no longer holds. A candidate still using a configuration from
   * before the removal counts this node's vote, so voting now could elect a leader without them.
   * Once its log is as long as before, it holds every entry it acknowledged (it caught up from a
   * leader, and leaders hold every committed entry), and it votes as usual.
   */
  boolean recovering() {
    return raft.lastIndex() < mustReach;
  }

  boolean initialized() {
    return machine.initialized();
  }

  RangeDescriptor descriptor() {
    return machine.descriptor();
  }

  boolean leading() {
    return raft.role() == RaftNode.Role.LEADER;
  }

  // -----------------------------------------------------------------------------------------------
  // Client requests

  void propose(KvRequest request, long deadline, CompletableFuture<KvResponse> result) {
    Command command =
        Command.newBuilder()
            .setWrite(
                WriteCommand.newBuilder()
                    .setRequest(request)
                    .setProposeTimeMillis(store.clock().wallTimeMillis()))
            .build();
    if (!raft.propose(command.toByteArray())) {
      result.complete(store.notLeader(this, false));
      return;
    }
    KvStateMachine.RequestKey key = KvStateMachine.RequestKey.of(request);
    writes.computeIfAbsent(key, k -> new ArrayList<>()).add(result);
    // Past the deadline the client has given up; the entry may still commit, so no answer is the
    // honest one. After a grace period, stop holding the request.
    if (deadline != KvHandler.NO_DEADLINE) {
      long grace = deadline - store.clock().nanoTime();
      store
          .scheduler()
          .schedule(
              Duration.ofNanos(Math.max(0, grace)).plusSeconds(1),
              () ->
                  store.submit(() -> {
                    List<CompletableFuture<KvResponse>> waiting = writes.get(key);
                    if (waiting != null && waiting.remove(result) && waiting.isEmpty()) {
                      writes.remove(key);
                    }
                    result.cancel(false);
                  }));
    }
  }

  /**
   * Proposes a transactional command. It is decided when applied, so the answer comes from the
   * apply, matched by a random proposal id (random, so that ids from different leaders never meet).
   */
  void proposeTxn(KvRequest request, long deadline, CompletableFuture<KvResponse> result) {
    long id;
    do {
      id = store.random().nextLong();
    } while (id == 0 || commands.containsKey(id));
    Command command =
        Command.newBuilder()
            .setTxn(
                TxnCommand.newBuilder()
                    .setRequest(request)
                    .setProposalId(id)
                    .setProposeTimeMillis(store.clock().wallTimeMillis()))
            .build();
    if (!raft.propose(command.toByteArray())) {
      result.complete(store.notLeader(this, false));
      return;
    }
    commands.put(id, result);
    long proposal = id;
    if (deadline != KvHandler.NO_DEADLINE) {
      long grace = deadline - store.clock().nanoTime();
      store
          .scheduler()
          .schedule(
              Duration.ofNanos(Math.max(0, grace)).plusSeconds(1),
              () ->
                  store.submit(() -> {
                    commands.remove(proposal);
                    result.cancel(false);
                  }));
    }
  }

  void startRead(KvRequest request, long deadline, CompletableFuture<KvResponse> result) {
    String context = "read-" + readSeq++;
    if (!raft.readIndex(context.getBytes(StandardCharsets.UTF_8))) {
      result.complete(store.notLeader(this, false));
      return;
    }
    reads.put(context, new PendingRead(request, deadline, result));
    if (deadline != KvHandler.NO_DEADLINE) {
      long remaining = Math.max(0, deadline - store.clock().nanoTime());
      store
          .scheduler()
          .schedule(
              Duration.ofNanos(remaining),
              () ->
                  store.submit(() -> {
                    PendingRead p = reads.remove(context);
                    if (p != null) {
                      p.future.complete(KvErrors.deadlineExceeded("read timed out"));
                    }
                  }));
    }
  }

  /** The leader answers a write once it has applied it. */
  void applied(RangeStateMachine.Applied a) {
    if (a.proposal() != 0) {
      CompletableFuture<KvResponse> waiting = commands.remove(a.proposal());
      if (waiting != null) {
        waiting.complete(a.response());
      }
    }
    if (a.request() != null) {
      List<CompletableFuture<KvResponse>> waiting = writes.remove(a.request());
      if (waiting != null) {
        waiting.forEach(w -> w.complete(a.response()));
      }
    }
  }

  void readIndexConfirmed(String context, long index) {
    PendingRead p = reads.get(context);
    if (p != null) {
      p.index = index;
    }
  }

  /**
   * Serves the reads whose leadership check passed and whose index is applied. The span is checked
   * again: a split applied since the read began may have moved the key to another range, whose
   * replica on this node shares the engine but may be behind or ahead of its own leader.
   */
  void serveReads() {
    Iterator<PendingRead> it = reads.values().iterator();
    while (it.hasNext()) {
      PendingRead p = it.next();
      if (p.index < 0 || machine.appliedIndex() < p.index) {
        continue;
      }
      it.remove();
      RangeDescriptor d = machine.descriptor();
      if (!Store.inSpan(p.request, d)) {
        p.future.complete(KvErrors.epochMismatch("the range split while the read waited"));
      } else if (p.request.hasGet()) {
        p.future.complete(KvReads.get(store.engine(), p.request.getGet()));
      } else if (p.request.hasTxnGet()) {
        p.future.complete(Percolator.get(store.engine(), p.request.getTxnGet(), machine.limits()));
      } else if (p.request.hasTxnScan()) {
        p.future.complete(
            Percolator.scan(
                store.engine(),
                p.request.getTxnScan(),
                machine.limits(),
                store.config().kv().maxScanLimit()));
      } else if (p.request.hasCoprocessor()) {
        p.future.complete(
            Percolator.coprocessor(
                store.engine(),
                p.request.getCoprocessor(),
                machine.limits(),
                store.coprocessor(),
                store.config().self().value()));
      } else if (p.request.hasScanLocks()) {
        p.future.complete(Percolator.scanLocks(store.engine(), p.request.getScanLocks(), d));
      } else {
        p.future.complete(
            KvReads.scan(
                store.engine(),
                p.request.getScan(),
                store.config().kv(),
                store.clock(),
                p.deadline));
      }
    }
  }

  /** On losing the leadership, redirect everyone still waiting. */
  void watchLeadership() {
    boolean leading = leading();
    if (wasLeader && !leading) {
      failAll(store.notLeader(this, true), store.notLeader(this, false));
    }
    wasLeader = leading;
  }

  /** Answers every waiting write with {@code forWrites} and every waiting read with the other. */
  void failAll(KvResponse forWrites, KvResponse forReads) {
    writes.values().forEach(ws -> ws.forEach(w -> w.complete(forWrites)));
    writes.clear();
    commands.values().forEach(c -> c.complete(forWrites));
    commands.clear();
    reads.values().forEach(p -> p.future.complete(forReads));
    reads.clear();
  }

  /** Session expiry is a replicated command carrying the leader's time. */
  void maybeSweepSessions(Duration tick) {
    KvServiceOptions kv = store.config().kv();
    if (leading() && ++ticksSinceSweep * tick.toMillis() >= kv.sessionSweepInterval().toMillis()) {
      ticksSinceSweep = 0;
      raft.propose(
          Command.newBuilder()
              .setExpireSessions(
                  ExpireSessions.newBuilder()
                      .setNowMillis(store.clock().wallTimeMillis())
                      .setTtlMillis(kv.sessionTtl().toMillis()))
              .build()
              .toByteArray());
    }
  }

  boolean isMember(NodeId node) {
    RangeDescriptor d = machine.descriptor();
    return (d != null && Ranges.isMember(d, node)) || raft.membership().isMember(node);
  }

  @Override
  public String toString() {
    RangeDescriptor d = machine.descriptor();
    return (d == null ? "r" + id + " (uninitialized)" : Ranges.describe(d)) + " " + raft;
  }
}

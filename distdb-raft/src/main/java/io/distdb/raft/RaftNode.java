package io.distdb.raft;

import io.distdb.common.NodeId;
import io.distdb.env.RandomSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * One member of a Raft group, as a pure state machine: no threads, clocks, disks or sockets. The
 * host feeds it {@link #tick()}, {@link #step(Message)} and proposals, then repeatedly takes a
 * {@link Ready} (what to persist, send and apply), does it, and calls {@link #advance(Ready)}.
 * Given the same inputs and the same {@link RandomSource}, it always does the same thing, which is
 * what lets the simulator replay any failure from its seed.
 *
 * <p>Implements, from the Raft paper and Ongaro's thesis: leader election with randomized timeouts;
 * log replication with the consistency check and fast backtracking by term; the commit rule of
 * Figure 8; snapshots and InstallSnapshot; pre-vote and check-quorum (thesis 9.6); single server
 * membership changes with learners (chapter 4); and ReadIndex for linearizable reads (6.4).
 *
 * <p>Not thread-safe: the host serializes every call, and calls nothing between {@link #ready()}
 * and {@link #advance(Ready)}.
 */
public final class RaftNode {

  /** What a node is doing. */
  public enum Role {
    FOLLOWER,
    /** Asking "would you vote for me?" without having raised its term (pre-vote). */
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER
  }

  /**
   * A read-only view, for hosts, tests and checkers.
   *
   * @param membership the latest membership in the log, committed or not
   * @param configIndex the index of the entry (or snapshot) that holds {@code membership}
   * @param match on a leader, each member's match index (its own is what it has stored)
   * @param transferee on a leader handing over its leadership, the node it hands over to
   */
  public record Status(
      NodeId id,
      Role role,
      long term,
      NodeId leader,
      long commit,
      long applied,
      long snapshotIndex,
      long lastIndex,
      Membership membership,
      long configIndex,
      Map<NodeId, Long> match,
      NodeId transferee) {}

  private static final Comparator<NodeId> BY_VALUE = Comparator.comparingInt(NodeId::value);
  private static final int MAX_APPLY_BATCH = 1024;

  private final RaftConfig cfg;
  private final NodeId id;
  private final RandomSource random;
  private final RaftLog log;

  private Role role = Role.FOLLOWER;
  private long term;
  private NodeId vote;
  private NodeId leader;
  private long applied;

  private int electionElapsed;
  private int heartbeatElapsed;
  private int randomizedElectionTimeout;

  /** The latest membership in the log, committed or not, and the index of the entry holding it. */
  private Membership membership;

  private long configIndex;

  // Leader state. TreeMaps, so iteration (and thus message order) is deterministic.
  private final Map<NodeId, Progress> progress = new TreeMap<>(BY_VALUE);
  private long leaderStartIndex;
  private long round;
  private NodeId transferee; // a leadership transfer in progress, or null
  private final ArrayDeque<PendingRead> pendingReads = new ArrayDeque<>();
  private final List<byte[]> readsWaitingForCommit = new ArrayList<>();

  // Candidate state.
  private final Map<NodeId, Boolean> votes = new TreeMap<>(BY_VALUE);

  // Output for the next Ready.
  private final List<Message> outbox = new ArrayList<>();
  private final List<Ready.ReadState> readStates = new ArrayList<>();
  private HardState persistedHardState;
  private Snapshot pendingSnapshot;
  private boolean pendingSnapshotReplacesLog;

  private record PendingRead(byte[] context, long index, long round) {}

  private RaftNode(RaftConfig cfg, PersistedState state, long applied, RandomSource random) {
    this.cfg = Objects.requireNonNull(cfg);
    this.id = cfg.id();
    this.random = Objects.requireNonNull(random);
    HardState hs = state.hardState();
    this.log = new RaftLog(state.snapshot(), state.entries(), hs.commit());
    this.term = hs.term();
    this.vote = hs.vote();
    this.persistedHardState = hs;
    this.applied = Math.max(log.snapshotIndex(), applied);
    if (this.applied > log.lastIndex()) {
      throw new IllegalArgumentException("applied " + applied + " beyond the stored log");
    }
    log.commitTo(this.applied);
    refreshMembership();
    resetElectionTimer();
  }

  /**
   * A founding member of a new group. Its first {@link Ready} stores the initial membership (as a
   * snapshot at index 0) so it survives a restart.
   */
  public static RaftNode bootstrap(RaftConfig cfg, Membership members, RandomSource random) {
    RaftNode node =
        new RaftNode(
            cfg,
            new PersistedState(HardState.EMPTY, Snapshot.initial(members), List.of()),
            0,
            random);
    node.pendingSnapshot = node.log.snapshot();
    return node;
  }

  /**
   * A node rebuilt from what its host stored. A node that has stored nothing ({@link
   * PersistedState#empty()}) knows no members, so it never campaigns; it waits for a leader to add
   * it and send it the log.
   *
   * @param applied how far the host's state machine has already applied; it is at least the
   *     snapshot's index, and the next {@link Ready} delivers committed entries after it
   */
  public static RaftNode restart(
      RaftConfig cfg, PersistedState state, long applied, RandomSource random) {
    return new RaftNode(cfg, state, applied, random);
  }

  // -----------------------------------------------------------------------------------------------
  // Inputs

  /** Advances logical time by one tick: drives election timeouts and heartbeats. */
  public void tick() {
    if (role == Role.LEADER) {
      tickLeader();
    } else {
      electionElapsed++;
      if (electionElapsed >= randomizedElectionTimeout && membership.isVoter(id)) {
        campaign(cfg.preVote(), false);
      }
    }
  }

  private void tickLeader() {
    heartbeatElapsed++;
    electionElapsed++;
    for (Progress pr : progress.values()) {
      if (pr.state == Progress.State.SNAPSHOT && ++pr.snapshotTicks >= cfg.electionTicks()) {
        pr.becomeProbe(); // presume the snapshot lost; the next attempt sends it again
      }
    }
    if (electionElapsed >= cfg.electionTicks()) {
      electionElapsed = 0;
      // A transfer that has not finished within an election timeout has failed (the target is
      // down, say): take proposals again.
      transferee = null;
      if (cfg.checkQuorum() && !quorumActive()) {
        // Cut off from a majority: a new leader may already exist on the other side. Stepping
        // down stops this one serving reads it can no longer vouch for.
        becomeFollower(term, null);
        return;
      }
    }
    if (heartbeatElapsed >= cfg.heartbeatTicks()) {
      heartbeatElapsed = 0;
      broadcastHeartbeat();
    }
  }

  /**
   * Handles a message from a peer.
   *
   * <p>Term rules first. A message from a higher term makes this node a follower in that term,
   * except: a node that still hears from its leader ignores vote requests (they come from a node
   * that cannot see the leader, and would only disrupt it); and pre-votes never change anyone's
   * term. A message from a lower term is from someone out of date: tell a stale leader our term so
   * it steps down, refuse a pre-vote, ignore the rest.
   */
  public void step(Message m) {
    if (!id.equals(m.to())) {
      return;
    }
    if (m.term() > term) {
      if (m instanceof Message.RequestVote rv
          && !rv.transfer()
          && cfg.checkQuorum()
          && leader != null
          && electionElapsed < cfg.electionTicks()) {
        return; // in a leader lease
      }
      boolean keepTerm =
          (m instanceof Message.RequestVote rv && rv.preVote())
              || (m instanceof Message.VoteResponse vr && vr.preVote() && vr.granted());
      if (!keepTerm) {
        boolean fromLeader =
            m instanceof Message.AppendEntries
                || m instanceof Message.Heartbeat
                || m instanceof Message.InstallSnapshot
                || m instanceof Message.TimeoutNow;
        becomeFollower(m.term(), fromLeader ? m.from() : null);
      }
    } else if (m.term() < term) {
      if (m instanceof Message.AppendEntries
          || m instanceof Message.Heartbeat
          || m instanceof Message.InstallSnapshot) {
        send(new Message.AppendResponse(id, m.from(), term, false, 0, 0, 0));
      } else if (m instanceof Message.RequestVote rv && rv.preVote()) {
        send(new Message.VoteResponse(id, m.from(), term, false, true));
      }
      return;
    }
    switch (m) {
      case Message.RequestVote rv -> handleVoteRequest(rv);
      case Message.VoteResponse vr -> handleVoteResponse(vr);
      case Message.AppendEntries ae -> {
        followLeader(ae.from());
        handleAppend(ae);
      }
      case Message.Heartbeat hb -> {
        followLeader(hb.from());
        handleHeartbeat(hb);
      }
      case Message.InstallSnapshot is -> {
        followLeader(is.from());
        handleSnapshot(is);
      }
      case Message.AppendResponse ar -> {
        if (role == Role.LEADER) {
          handleAppendResponse(ar);
        }
      }
      case Message.HeartbeatResponse hr -> {
        if (role == Role.LEADER) {
          handleHeartbeatResponse(hr);
        }
      }
      case Message.TimeoutNow t -> {
        // The leader hands over: campaign at once, without pre-vote, and tell voters it was asked.
        if (role != Role.LEADER && membership.isVoter(id)) {
          campaign(false, true);
        }
      }
    }
  }

  /**
   * Appends a command to the log if this node is the leader.
   *
   * @return false if it is not the leader; the host should redirect the client
   */
  public boolean propose(byte[] command) {
    if (role != Role.LEADER || transferee != null) {
      return false; // during a transfer, so that the target can catch up
    }
    appendAsLeader(Entry.Type.NORMAL, command.clone());
    return true;
  }

  /**
   * Proposes a membership change if this node is the leader and may change membership now: the
   * previous change must be committed (one at a time), and the leader must have committed an entry
   * of its own term (otherwise, with single-server changes, a leader elected under an older
   * configuration could commit a change that conflicts with one it doesn't know about).
   *
   * @return false if it cannot be proposed now; try again later
   * @throws IllegalArgumentException if the change makes no sense for the current membership
   */
  public boolean proposeConfigChange(ConfigChange change) {
    if (role != Role.LEADER
        || transferee != null
        || configIndex > log.committed()
        || log.committed() < leaderStartIndex) {
      return false;
    }
    Membership next = membership.apply(change);
    appendAsLeader(Entry.Type.CONFIG, RaftCodec.encodeMembership(next));
    return true;
  }

  /**
   * Hands the leadership to voter {@code target} (thesis 3.10). The leader stops taking proposals,
   * brings the target's log up to date, then sends it {@link Message.TimeoutNow} so it campaigns at
   * once and wins. If that has not happened within an election timeout, the transfer is abandoned
   * and this node leads on.
   *
   * @return false if this node is not the leader or {@code target} is not another voter
   */
  public boolean transferLeadership(NodeId target) {
    if (role != Role.LEADER || target.equals(id) || !membership.isVoter(target)) {
      return false;
    }
    if (target.equals(transferee)) {
      return true;
    }
    transferee = target;
    electionElapsed = 0;
    Progress pr = progress.get(target);
    if (pr.match == log.lastIndex()) {
      send(new Message.TimeoutNow(id, target, term));
    } else {
      replicateTo(target, pr, true);
    }
    return true;
  }

  /**
   * Starts an election now instead of waiting for the timeout, if this node is a voter and not the
   * leader. With pre-vote on, it first asks; so it cannot depose a leader the others still follow.
   * Used for a range just created by a split, whose old leader is the natural new one.
   */
  public void campaignNow() {
    if (role != Role.LEADER && membership.isVoter(id)) {
      campaign(cfg.preVote(), false);
    }
  }

  /**
   * Starts a linearizable read (ReadIndex, thesis 6.4) if this node is the leader. Once a majority
   * has confirmed, after this call, that this node is still the leader, a {@link Ready.ReadState}
   * comes out with the commit index as of this call: reading after applying up to it sees every
   * write that completed before the read began.
   *
   * @return false if it is not the leader
   */
  public boolean readIndex(byte[] context) {
    if (role != Role.LEADER) {
      return false;
    }
    if (log.committed() < leaderStartIndex) {
      // A new leader's commit index may lag the true one until it commits an entry of its term.
      readsWaitingForCommit.add(context.clone());
    } else {
      startReads(List.of(context.clone()));
    }
    return true;
  }

  /**
   * Replaces the log up to {@code index} with a snapshot of the state machine, which must have
   * applied that far. The next {@link Ready} carries the snapshot to store.
   */
  public void compact(long index, byte[] stateData) {
    if (index <= log.snapshotIndex()) {
      return;
    }
    if (index > applied) {
      throw new IllegalArgumentException("cannot compact to " + index + ", applied " + applied);
    }
    log.compact(index, stateData.clone(), membershipAt(index));
    pendingSnapshot = log.snapshot();
    pendingSnapshotReplacesLog = false;
  }

  // -----------------------------------------------------------------------------------------------
  // Output

  /** Whether {@link #ready()} has anything to do. */
  public boolean hasReady() {
    return !outbox.isEmpty()
        || !readStates.isEmpty()
        || pendingSnapshot != null
        || !hardState().equals(persistedHardState)
        || log.stable() < log.lastIndex()
        || log.committed() >= applyFrom();
  }

  /** What the host must do now; see {@link Ready} for the order. */
  public Ready ready() {
    HardState hs = hardState();
    long from = applyFrom();
    List<Entry> toApply =
        log.committed() >= from ? log.slice(from, log.committed(), MAX_APPLY_BATCH) : List.of();
    Ready r =
        new Ready(
            hs.equals(persistedHardState) ? null : hs,
            log.unstable(),
            pendingSnapshot,
            pendingSnapshotReplacesLog,
            toApply,
            outbox,
            readStates);
    outbox.clear();
    readStates.clear();
    return r;
  }

  /** The host has done everything in {@code r}. */
  public void advance(Ready r) {
    if (r.hardState() != null) {
      persistedHardState = r.hardState();
    }
    if (!r.entries().isEmpty()) {
      Entry last = r.entries().getLast();
      log.stableTo(last.index(), last.term());
    }
    if (r.snapshot() != null) {
      if (r.snapshotReplacesLog()) {
        applied = Math.max(applied, r.snapshot().index());
      }
      if (pendingSnapshot == r.snapshot()) {
        pendingSnapshot = null;
        pendingSnapshotReplacesLog = false;
      }
    }
    if (!r.committedEntries().isEmpty()) {
      applied = Math.max(applied, r.committedEntries().getLast().index());
    }
    // The leader counts itself only for entries now on its own disk.
    if (role == Role.LEADER && maybeCommit()) {
      broadcastAppend(true); // carry the new commit index
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Inspection

  public Status status() {
    Map<NodeId, Long> match = new TreeMap<>(BY_VALUE);
    if (role == Role.LEADER) {
      progress.forEach((n, pr) -> match.put(n, pr.match));
      match.put(id, log.stable());
    }
    return new Status(
        id,
        role,
        term,
        leader,
        log.committed(),
        applied,
        log.snapshotIndex(),
        log.lastIndex(),
        membership,
        configIndex,
        Collections.unmodifiableMap(match),
        transferee);
  }

  // Cheap views of single fields, for hosts that check them per message (status() copies maps).

  public Role role() {
    return role;
  }

  /** The leader this node follows (itself if it leads), or null if it knows none. */
  public NodeId leader() {
    return leader;
  }

  public long term() {
    return term;
  }

  /** The latest membership in the log, committed or not. */
  public Membership membership() {
    return membership;
  }

  /** The index of the entry (or snapshot) holding {@link #membership()}. */
  public long configIndex() {
    return configIndex;
  }

  /** The index of the last entry in the log (the snapshot's, if the log is empty). */
  public long lastIndex() {
    return log.lastIndex();
  }

  /** Entries {@code from..to} inclusive, which must be in the log (not compacted). */
  public List<Entry> entries(long from, long to) {
    return log.slice(from, to, Integer.MAX_VALUE);
  }

  /** The term of entry {@code index}, or -1 if compacted away or beyond the end. */
  public long termAt(long index) {
    return log.term(index);
  }

  // -----------------------------------------------------------------------------------------------
  // Elections

  private void campaign(boolean pre, boolean transfer) {
    if (pre) {
      role = Role.PRE_CANDIDATE;
    } else {
      term++;
      vote = id;
      role = Role.CANDIDATE;
    }
    leader = null;
    resetElectionTimer();
    progress.clear();
    votes.clear();
    votes.put(id, true);
    if (pollVotes() > 0) {
      won(pre);
      return;
    }
    long voteTerm = pre ? term + 1 : term;
    for (NodeId v : membership.voters()) {
      if (!v.equals(id)) {
        send(
            new Message.RequestVote(
                id, v, voteTerm, log.lastIndex(), log.lastTerm(), pre, transfer));
      }
    }
  }

  private void won(boolean pre) {
    if (pre) {
      campaign(false, false);
    } else {
      becomeLeader();
    }
  }

  /** 1 if a majority granted, -1 if a majority refused, 0 if undecided. */
  private int pollVotes() {
    int granted = 0;
    int refused = 0;
    for (NodeId v : membership.voters()) {
      Boolean b = votes.get(v);
      if (b != null) {
        if (b) {
          granted++;
        } else {
          refused++;
        }
      }
    }
    if (granted >= membership.quorum()) {
      return 1;
    }
    return refused > membership.voters().size() - membership.quorum() ? -1 : 0;
  }

  private void handleVoteRequest(Message.RequestVote rv) {
    boolean canVote =
        rv.from().equals(vote)
            || (vote == null && leader == null)
            || (rv.preVote() && rv.term() > term);
    // The election restriction: never help elect a node whose log lacks entries this node has,
    // since some of them may be committed.
    if (canVote && log.isUpToDate(rv.lastIndex(), rv.lastTerm())) {
      send(new Message.VoteResponse(id, rv.from(), rv.term(), true, rv.preVote()));
      if (!rv.preVote()) {
        vote = rv.from(); // persisted (Ready.hardState) before the response is sent
        electionElapsed = 0;
      }
    } else {
      send(new Message.VoteResponse(id, rv.from(), term, false, rv.preVote()));
    }
  }

  private void handleVoteResponse(Message.VoteResponse vr) {
    boolean expected = vr.preVote() ? role == Role.PRE_CANDIDATE : role == Role.CANDIDATE;
    if (!expected || !membership.isVoter(vr.from())) {
      return;
    }
    votes.put(vr.from(), vr.granted());
    int result = pollVotes();
    if (result > 0) {
      won(vr.preVote());
    } else if (result < 0) {
      becomeFollower(term, null);
    }
  }

  private void becomeFollower(long newTerm, NodeId newLeader) {
    if (newTerm != term) {
      term = newTerm;
      vote = null;
    }
    role = Role.FOLLOWER;
    leader = newLeader;
    transferee = null;
    resetElectionTimer();
    progress.clear();
    votes.clear();
    pendingReads.clear();
    readsWaitingForCommit.clear();
  }

  private void becomeLeader() {
    role = Role.LEADER;
    leader = id;
    transferee = null;
    resetElectionTimer();
    progress.clear();
    for (NodeId m : membership.members()) {
      if (!m.equals(id)) {
        progress.put(m, new Progress(log.lastIndex() + 1));
      }
    }
    // A no-op in the new term. Entries from earlier terms can only be committed together with
    // one from the current term (Figure 8), so this commits them promptly, and ReadIndex needs it
    // to learn the true commit index.
    Entry noop = new Entry(log.lastIndex() + 1, term, Entry.Type.NORMAL, new byte[0]);
    log.append(noop);
    leaderStartIndex = noop.index();
    broadcastAppend(true);
  }

  private void followLeader(NodeId from) {
    if (role == Role.LEADER) {
      throw new IllegalStateException(
          id + " leads term " + term + " but heard from another leader, " + from);
    }
    if (role != Role.FOLLOWER) {
      becomeFollower(term, from);
    }
    leader = from;
    electionElapsed = 0;
  }

  private void resetElectionTimer() {
    electionElapsed = 0;
    heartbeatElapsed = 0;
    randomizedElectionTimeout = cfg.electionTicks() + random.nextInt(cfg.electionTicks());
  }

  // -----------------------------------------------------------------------------------------------
  // Follower side of replication

  private void handleAppend(Message.AppendEntries ae) {
    if (ae.prevIndex() < log.committed()) {
      // Everything up to our commit index matches the leader already.
      send(new Message.AppendResponse(id, ae.from(), term, true, log.committed(), 0, 0));
      return;
    }
    if (log.matchTerm(ae.prevIndex(), ae.prevTerm())) {
      long lastNew = ae.prevIndex() + ae.entries().size();
      long conflict = log.findConflict(ae.entries());
      if (conflict > 0) {
        long configBefore = configIndex;
        List<Entry> rest =
            ae.entries().subList((int) (conflict - ae.prevIndex() - 1), ae.entries().size());
        log.truncateAndAppend(rest);
        if (configBefore >= conflict
            || rest.stream().anyMatch(e -> e.type() == Entry.Type.CONFIG)) {
          refreshMembership();
        }
      }
      // Only up to lastNew: entries past it may be stale leftovers that don't match the leader.
      log.commitTo(Math.min(ae.commit(), lastNew));
      send(new Message.AppendResponse(id, ae.from(), term, true, lastNew, 0, 0));
      return;
    }
    // Reject, with a hint that lets the leader skip a whole term per round trip instead of one
    // entry: our term at prevIndex and where it starts, or our length if we are too short.
    long conflictIndex;
    long conflictTerm;
    if (ae.prevIndex() > log.lastIndex()) {
      conflictIndex = log.lastIndex() + 1;
      conflictTerm = 0;
    } else {
      conflictTerm = log.term(ae.prevIndex());
      conflictIndex = log.firstIndexOfTerm(conflictTerm, ae.prevIndex());
    }
    send(
        new Message.AppendResponse(
            id, ae.from(), term, false, ae.prevIndex(), conflictIndex, conflictTerm));
  }

  private void handleHeartbeat(Message.Heartbeat hb) {
    log.commitTo(Math.min(hb.commit(), log.lastIndex()));
    send(new Message.HeartbeatResponse(id, hb.from(), term, hb.round()));
  }

  private void handleSnapshot(Message.InstallSnapshot is) {
    Snapshot s = is.snapshot();
    if (s.index() <= log.committed()) {
      send(new Message.AppendResponse(id, is.from(), term, true, log.committed(), 0, 0));
      return;
    }
    if (log.matchTerm(s.index(), s.term())) {
      // We already hold that entry, so the log up to it matches: just commit.
      log.commitTo(s.index());
    } else {
      log.restore(s);
      setMembership(s.membership(), s.index());
      pendingSnapshot = s;
      pendingSnapshotReplacesLog = true;
    }
    send(new Message.AppendResponse(id, is.from(), term, true, s.index(), 0, 0));
  }

  // -----------------------------------------------------------------------------------------------
  // Leader side of replication

  private void appendAsLeader(Entry.Type type, byte[] data) {
    Entry e = new Entry(log.lastIndex() + 1, term, type, data);
    log.append(e);
    if (type == Entry.Type.CONFIG) {
      // Takes effect on append, not on commit (thesis 4.1).
      setMembership(RaftCodec.decodeMembership(data), e.index());
    }
    broadcastAppend(false);
  }

  private void handleAppendResponse(Message.AppendResponse ar) {
    Progress pr = progress.get(ar.from());
    if (pr == null) {
      return;
    }
    pr.recentActive = true;
    if (!ar.success()) {
      boolean stale =
          switch (pr.state) {
            case REPLICATE -> ar.index() <= pr.match;
            case PROBE -> ar.index() != pr.next - 1;
            case SNAPSHOT -> true;
          };
      if (stale) {
        return;
      }
      if (ar.conflictTerm() == 0 && ar.conflictIndex() <= pr.match) {
        // Its log now ends before what it once acknowledged: it lost entries. A host can do that
        // to a replica it destroyed and created again (a learner removed and being added back,
        // say); Raft never does. Start over from what it has, or it would be probed forever just
        // past an index it no longer holds.
        pr.match = ar.conflictIndex() - 1;
      }
      long next;
      if (ar.conflictTerm() > 0) {
        long last = log.lastIndexOfTerm(ar.conflictTerm());
        next = last > 0 ? last + 1 : ar.conflictIndex();
      } else {
        next = ar.conflictIndex();
      }
      pr.next = Math.max(pr.match + 1, Math.min(next, ar.index()));
      pr.becomeProbe();
      replicateTo(ar.from(), pr, true);
      return;
    }
    boolean advanced = ar.index() > pr.match;
    if (advanced) {
      pr.match = ar.index();
    }
    pr.next = Math.max(pr.next, ar.index() + 1);
    switch (pr.state) {
      case PROBE -> pr.becomeReplicate();
      case SNAPSHOT -> {
        if (pr.match >= pr.pendingSnapshot) {
          pr.becomeReplicate();
        }
      }
      case REPLICATE -> pr.freeInflightTo(ar.index());
    }
    if (ar.from().equals(transferee) && pr.match == log.lastIndex()) {
      send(new Message.TimeoutNow(id, transferee, term)); // caught up: hand over
    }
    if (advanced && maybeCommit()) {
      broadcastAppend(true);
    } else {
      replicateTo(ar.from(), pr, false);
    }
  }

  private void handleHeartbeatResponse(Message.HeartbeatResponse hr) {
    Progress pr = progress.get(hr.from());
    if (pr == null) {
      return;
    }
    pr.recentActive = true;
    pr.ackedRound = Math.max(pr.ackedRound, hr.round());
    // The follower is alive. If replication stalled because messages were lost, unstick it.
    if (pr.state == Progress.State.PROBE) {
      pr.probeSent = false;
    } else if (pr.state == Progress.State.REPLICATE
        && pr.inflight.size() >= cfg.maxInflightAppends()) {
      pr.inflight.pollFirst();
    }
    if (pr.match < log.lastIndex()) {
      replicateTo(hr.from(), pr, true);
    }
    checkReads();
  }

  /**
   * Sends what {@code to} needs next; in REPLICATE, keeps going until paused or caught up.
   *
   * @param allowEmpty whether to send an AppendEntries with no entries: worth it to carry a new
   *     commit index, or to check a follower that may have lost messages, but never in answer to an
   *     acknowledgement (the follower would acknowledge that too, forever)
   */
  private void replicateTo(NodeId to, Progress pr, boolean allowEmpty) {
    boolean empty = allowEmpty;
    while (sendAppend(to, pr, empty)
        && pr.state == Progress.State.REPLICATE
        && pr.next <= log.lastIndex()) {
      empty = false; // keep streaming
    }
  }

  private boolean sendAppend(NodeId to, Progress pr, boolean allowEmpty) {
    if (pr.paused(cfg.maxInflightAppends())) {
      return false;
    }
    long prevIndex = pr.next - 1;
    long prevTerm = log.term(prevIndex);
    if (prevTerm < 0) {
      // The entries it needs are compacted away: send the snapshot instead.
      send(new Message.InstallSnapshot(id, to, term, log.snapshot()));
      pr.becomeSnapshot(log.snapshotIndex());
      return true;
    }
    List<Entry> entries =
        pr.next <= log.lastIndex()
            ? log.slice(pr.next, log.lastIndex(), cfg.maxEntriesPerAppend())
            : List.of();
    if (entries.isEmpty() && !allowEmpty && pr.state != Progress.State.PROBE) {
      return false;
    }
    send(new Message.AppendEntries(id, to, term, prevIndex, prevTerm, entries, log.committed()));
    if (pr.state == Progress.State.REPLICATE) {
      if (!entries.isEmpty()) {
        long last = entries.getLast().index();
        pr.next = last + 1; // optimistic: assume it arrives, back off if rejected
        pr.inflight.addLast(last);
      }
    } else if (pr.state == Progress.State.PROBE) {
      pr.probeSent = true;
    }
    return true;
  }

  /** Sends new entries to everyone; with {@code allowEmpty}, also tells caught-up followers. */
  private void broadcastAppend(boolean allowEmpty) {
    progress.forEach((to, pr) -> replicateTo(to, pr, allowEmpty));
  }

  private void broadcastHeartbeat() {
    progress.forEach((to, pr) ->
        send(new Message.Heartbeat(id, to, term, Math.min(pr.match, log.committed()), round)));
  }

  /**
   * Commits the highest index stored on a majority of voters, but only if that entry is from the
   * current term (Figure 8 of the paper). An entry from an earlier term can sit on a majority and
   * still be overwritten by a later leader; once an entry of the current term is on a majority, no
   * leader without it can be elected, so it and everything before it are safe.
   */
  private boolean maybeCommit() {
    if (membership.voters().isEmpty()) {
      return false;
    }
    List<Long> matches = new ArrayList<>();
    for (NodeId v : membership.voters()) {
      if (v.equals(id)) {
        matches.add(log.stable()); // only what is on our own disk counts
      } else {
        Progress pr = progress.get(v);
        matches.add(pr == null ? 0L : pr.match);
      }
    }
    matches.sort(Comparator.reverseOrder());
    long n = matches.get(membership.quorum() - 1);
    if (n <= log.committed() || log.term(n) != term) {
      return false;
    }
    log.commitTo(n);
    if (!readsWaitingForCommit.isEmpty() && log.committed() >= leaderStartIndex) {
      List<byte[]> waiting = List.copyOf(readsWaitingForCommit);
      readsWaitingForCommit.clear();
      startReads(waiting);
    }
    if (!membership.isVoter(id) && configIndex <= log.committed()) {
      // Our own removal is committed: tell the others, then step aside (thesis 4.2.2).
      broadcastAppend(true);
      becomeFollower(term, null);
    }
    return true;
  }

  private boolean quorumActive() {
    int active = membership.isVoter(id) ? 1 : 0;
    for (NodeId v : membership.voters()) {
      Progress pr = progress.get(v);
      if (pr != null && pr.recentActive) {
        active++;
      }
    }
    progress.values().forEach(pr -> pr.recentActive = false);
    return active >= membership.quorum();
  }

  // -----------------------------------------------------------------------------------------------
  // ReadIndex

  private void startReads(List<byte[]> contexts) {
    round++;
    for (byte[] c : contexts) {
      pendingReads.addLast(new PendingRead(c, log.committed(), round));
    }
    broadcastHeartbeat();
    checkReads();
  }

  private void checkReads() {
    while (!pendingReads.isEmpty()) {
      PendingRead r = pendingReads.peekFirst();
      int acks = membership.isVoter(id) ? 1 : 0;
      for (NodeId v : membership.voters()) {
        Progress pr = progress.get(v);
        if (pr != null && pr.ackedRound >= r.round()) {
          acks++;
        }
      }
      if (acks < membership.quorum()) {
        return;
      }
      pendingReads.pollFirst();
      readStates.add(new Ready.ReadState(r.index(), r.context()));
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Membership

  private void refreshMembership() {
    for (long i = log.lastIndex(); i > log.snapshotIndex(); i--) {
      Entry e = log.entry(i);
      if (e.type() == Entry.Type.CONFIG) {
        setMembership(RaftCodec.decodeMembership(e.data()), i);
        return;
      }
    }
    setMembership(log.snapshot().membership(), log.snapshotIndex());
  }

  private Membership membershipAt(long index) {
    for (long i = index; i > log.snapshotIndex(); i--) {
      Entry e = log.entry(i);
      if (e.type() == Entry.Type.CONFIG) {
        return RaftCodec.decodeMembership(e.data());
      }
    }
    return log.snapshot().membership();
  }

  private void setMembership(Membership m, long index) {
    membership = m;
    configIndex = index;
    if (transferee != null && !m.isVoter(transferee)) {
      transferee = null;
    }
    if (role == Role.LEADER) {
      progress.keySet().removeIf(n -> !m.isMember(n));
      for (NodeId n : m.members()) {
        if (!n.equals(id) && !progress.containsKey(n)) {
          progress.put(n, new Progress(log.lastIndex() + 1));
        }
      }
      if (maybeCommit()) {
        broadcastAppend(true);
      }
    } else if (role != Role.FOLLOWER && !m.isVoter(id)) {
      becomeFollower(term, null);
    }
  }

  // -----------------------------------------------------------------------------------------------

  private HardState hardState() {
    return new HardState(term, vote, log.committed());
  }

  private long applyFrom() {
    long base = applied;
    if (pendingSnapshot != null && pendingSnapshotReplacesLog) {
      base = Math.max(base, pendingSnapshot.index());
    }
    return base + 1;
  }

  private void send(Message m) {
    outbox.add(m);
  }

  @Override
  public String toString() {
    return id
        + "["
        + role
        + " t"
        + term
        + " commit "
        + log.committed()
        + "/"
        + log.lastIndex()
        + "]";
  }
}

package io.distdb.sim;

import io.distdb.common.NodeId;
import io.distdb.raft.Entry;
import io.distdb.raft.RaftNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks Raft's safety properties while a simulation runs, and fails at the first violation with
 * enough detail to start debugging (the seed then replays it exactly):
 *
 * <ol>
 *   <li><b>Election safety:</b> at most one leader per term.
 *   <li><b>Log matching:</b> if two logs have an entry with the same index and term, they are
 *       identical up to it.
 *   <li><b>Leader completeness:</b> an entry that is committed is in the log of every later leader.
 *   <li><b>State machine safety:</b> every replica applies the same entry at each index, so all
 *       replicas pass through the same states.
 *   <li><b>Linearizable reads:</b> a ReadIndex read sees every entry committed before it began.
 * </ol>
 *
 * <p>"Committed" here means applied by some replica: a sound, slightly conservative view, since
 * only committed entries are ever applied.
 */
final class RaftChecker {

  /** Thrown when a safety property is broken. */
  static final class Violation extends AssertionError {
    private static final long serialVersionUID = 1L;

    Violation(String message) {
      super(message);
    }
  }

  private final Map<Long, NodeId> leaderOfTerm = new HashMap<>();
  private final Map<Long, Entry> committed = new HashMap<>();
  private final Map<Long, Long> hashAt = new HashMap<>();
  private long maxCommitted;
  long leaderChanges;
  long readsChecked;
  long snapshotsInstalled;

  /** Election safety; and leader completeness for the new leader's log. */
  void onLeader(NodeId id, RaftNode node) {
    RaftNode.Status s = node.status();
    NodeId previous = leaderOfTerm.putIfAbsent(s.term(), id);
    if (previous == null) {
      leaderChanges++;
    } else if (!previous.equals(id)) {
      throw new Violation(
          "election safety: " + previous + " and " + id + " both led term " + s.term());
    }
    if (s.lastIndex() < maxCommitted) {
      throw new Violation(
          "leader completeness: "
              + id
              + " leads term "
              + s.term()
              + " with last index "
              + s.lastIndex()
              + " but "
              + maxCommitted
              + " is committed");
    }
    for (long i = s.snapshotIndex() + 1; i <= maxCommitted; i++) {
      Entry c = committed.get(i);
      if (c != null && node.termAt(i) != c.term()) {
        throw new Violation(
            "leader completeness: "
                + id
                + " leads term "
                + s.term()
                + " but has term "
                + node.termAt(i)
                + " at "
                + i
                + " where "
                + c
                + " is committed");
      }
    }
  }

  /** State machine safety: one entry, and one resulting state, per index. */
  void onApplied(NodeId id, Entry e, long hashAfter) {
    Entry first = committed.putIfAbsent(e.index(), e);
    if (first != null && !first.equals(e)) {
      throw new Violation(
          "state machine safety: " + id + " applied " + e + " where " + first + " was applied");
    }
    recordHash(id, e.index(), hashAfter);
    maxCommitted = Math.max(maxCommitted, e.index());
  }

  /** A replica replaced its state with a snapshot: its state must be the one others had there. */
  void onSnapshotInstalled(NodeId id, long index, long hash) {
    snapshotsInstalled++;
    recordHash(id, index, hash);
    maxCommitted = Math.max(maxCommitted, index);
  }

  private void recordHash(NodeId id, long index, long hash) {
    Long known = hashAt.putIfAbsent(index, hash);
    if (known != null && known != hash) {
      throw new Violation(
          "state machine safety: " + id + " has a different state after index " + index);
    }
  }

  /** The commit index a read must reflect if it starts now. */
  long committedNow() {
    return maxCommitted;
  }

  /** Linearizability of a ReadIndex read: it must see everything committed before it started. */
  void onRead(NodeId id, long committedAtStart, long appliedAtRead) {
    readsChecked++;
    if (appliedAtRead < committedAtStart) {
      throw new Violation(
          "stale read on "
              + id
              + ": it read the state at "
              + appliedAtRead
              + " but "
              + committedAtStart
              + " was committed before it started");
    }
  }

  /** Log matching across every pair of logs. */
  void checkLogMatching(Map<NodeId, RaftNode> nodes) {
    List<Map.Entry<NodeId, RaftNode>> all = List.copyOf(nodes.entrySet());
    for (int a = 0; a < all.size(); a++) {
      for (int b = a + 1; b < all.size(); b++) {
        checkPair(
            all.get(a).getKey(), all.get(a).getValue(), all.get(b).getKey(), all.get(b).getValue());
      }
    }
  }

  private static void checkPair(NodeId ia, RaftNode a, NodeId ib, RaftNode b) {
    RaftNode.Status sa = a.status();
    RaftNode.Status sb = b.status();
    long lo = Math.max(sa.snapshotIndex(), sb.snapshotIndex()) + 1;
    long hi = Math.min(sa.lastIndex(), sb.lastIndex());
    // The highest index where the two agree on the term; below it they must agree entirely.
    long agree = 0;
    for (long i = hi; i >= lo; i--) {
      if (a.termAt(i) == b.termAt(i)) {
        agree = i;
        break;
      }
    }
    if (agree == 0) {
      return;
    }
    List<Entry> ea = a.entries(lo, agree);
    List<Entry> eb = b.entries(lo, agree);
    if (!ea.equals(eb)) {
      for (int i = 0; i < ea.size(); i++) {
        if (!ea.get(i).equals(eb.get(i))) {
          throw new Violation(
              "log matching: "
                  + ia
                  + " and "
                  + ib
                  + " agree on the term at "
                  + agree
                  + " but differ at "
                  + ea.get(i).index()
                  + ": "
                  + ea.get(i)
                  + " vs "
                  + eb.get(i));
        }
      }
    }
  }
}

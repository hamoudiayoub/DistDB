package io.distdb.raft;

import java.util.ArrayDeque;

/**
 * A leader's view of one follower's log.
 *
 * <ul>
 *   <li><b>PROBE</b>: the leader doesn't know where the follower's log matches. It sends one
 *       AppendEntries at a time and waits for the answer, backing off on rejection.
 *   <li><b>REPLICATE</b>: the logs match up to {@code match}. The leader streams entries without
 *       waiting, up to {@code maxInflightAppends} unacknowledged messages.
 *   <li><b>SNAPSHOT</b>: the follower needs entries the leader has compacted, so a snapshot is on
 *       its way. Nothing else is sent until it is acknowledged or presumed lost.
 * </ul>
 */
final class Progress {

  enum State {
    PROBE,
    REPLICATE,
    SNAPSHOT
  }

  /** The highest index known to be on the follower's disk and to match the leader's log. */
  long match;

  /** The next index to send. */
  long next;

  State state = State.PROBE;

  /** In PROBE: a message is out, wait for its answer (or a heartbeat answer) before sending. */
  boolean probeSent;

  /** In SNAPSHOT: the index of the snapshot sent, and ticks since sending it. */
  long pendingSnapshot;

  int snapshotTicks;

  /** In REPLICATE: the last index of each unacknowledged AppendEntries, oldest first. */
  final ArrayDeque<Long> inflight = new ArrayDeque<>();

  /** Heard from since the last check-quorum round. */
  boolean recentActive;

  /** The latest heartbeat round this follower has answered (for ReadIndex). */
  long ackedRound;

  Progress(long next) {
    this.next = next;
  }

  void becomeProbe() {
    state = State.PROBE;
    probeSent = false;
    inflight.clear();
    next = Math.max(next, match + 1);
  }

  void becomeReplicate() {
    state = State.REPLICATE;
    next = match + 1;
    inflight.clear();
  }

  void becomeSnapshot(long snapshotIndex) {
    state = State.SNAPSHOT;
    pendingSnapshot = snapshotIndex;
    snapshotTicks = 0;
    inflight.clear();
  }

  boolean paused(int maxInflight) {
    return switch (state) {
      case PROBE -> probeSent;
      case REPLICATE -> inflight.size() >= maxInflight;
      case SNAPSHOT -> true;
    };
  }

  /** Acknowledged up to {@code index}: the messages ending at or before it are no longer out. */
  void freeInflightTo(long index) {
    while (!inflight.isEmpty() && inflight.peekFirst() <= index) {
      inflight.pollFirst();
    }
  }

  @Override
  public String toString() {
    return state + " match=" + match + " next=" + next;
  }
}

package io.distdb.raft;

import io.distdb.common.NodeId;
import java.util.List;

/**
 * Everything Raft nodes say to each other. Every message carries the sender's term: a node that
 * sees a higher term than its own learns it is out of date and steps down (with the pre-vote and
 * lease exceptions described on {@link RaftNode#step}).
 */
public sealed interface Message
    permits Message.RequestVote,
        Message.VoteResponse,
        Message.AppendEntries,
        Message.AppendResponse,
        Message.Heartbeat,
        Message.HeartbeatResponse,
        Message.InstallSnapshot,
        Message.TimeoutNow {

  NodeId from();

  NodeId to();

  long term();

  /**
   * "Vote for me in {@code term}." With {@code preVote}, it is only a question ("would you?") asked
   * for {@code term} without the sender having moved to it, so a node cut off from the group cannot
   * push everyone's term up while it fails to win. With {@code transfer}, the leader asked for this
   * election ({@link TimeoutNow}), so voters grant it even while they still hear from that leader.
   */
  record RequestVote(
      NodeId from,
      NodeId to,
      long term,
      long lastIndex,
      long lastTerm,
      boolean preVote,
      boolean transfer)
      implements Message {}

  /** The answer to a {@link RequestVote}. */
  record VoteResponse(NodeId from, NodeId to, long term, boolean granted, boolean preVote)
      implements Message {}

  /**
   * "Here are entries following {@code prevIndex}, which in my log has term {@code prevTerm}." The
   * follower accepts only if its own log agrees at {@code prevIndex}: that consistency check,
   * applied inductively, is why two logs that agree on one entry agree on everything before it.
   */
  record AppendEntries(
      NodeId from,
      NodeId to,
      long term,
      long prevIndex,
      long prevTerm,
      List<Entry> entries,
      long commit)
      implements Message {
    public AppendEntries {
      entries = List.copyOf(entries);
    }
  }

  /**
   * The answer to an {@link AppendEntries} or {@link InstallSnapshot}. On success, {@code index} is
   * the highest index now known to match the leader. On rejection, {@code index} is the rejected
   * {@code prevIndex}, and the conflict hint lets the leader skip back a whole term at a time: the
   * follower's term at {@code prevIndex} ({@code conflictTerm}, 0 if its log is too short) and
   * where that term starts ({@code conflictIndex}).
   */
  record AppendResponse(
      NodeId from,
      NodeId to,
      long term,
      boolean success,
      long index,
      long conflictIndex,
      long conflictTerm)
      implements Message {}

  /**
   * "I am still the leader." Carries no entries and needs no log check. {@code commit} is capped at
   * what the follower is known to have, so it never commits entries it may not hold. {@code round}
   * numbers heartbeat rounds, which ReadIndex uses to confirm leadership.
   */
  record Heartbeat(NodeId from, NodeId to, long term, long commit, long round) implements Message {}

  /** The answer to a {@link Heartbeat}, echoing its round. */
  record HeartbeatResponse(NodeId from, NodeId to, long term, long round) implements Message {}

  /** "Replace your log with this state." Sent when the entries a follower needs are compacted. */
  record InstallSnapshot(NodeId from, NodeId to, long term, Snapshot snapshot) implements Message {}

  /**
   * "Start an election now." A leader sends it to hand over its leadership (thesis 3.10), once the
   * target's log is as complete as its own, so the target wins at once.
   */
  record TimeoutNow(NodeId from, NodeId to, long term) implements Message {}
}

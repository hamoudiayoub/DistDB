package io.distdb.raft;

import io.distdb.common.NodeId;
import java.util.Objects;

/**
 * How a {@link RaftNode} behaves. Time is counted in ticks: the host calls {@link RaftNode#tick()}
 * at a fixed interval, so Raft itself never reads a clock.
 *
 * @param id this node
 * @param electionTicks a follower that hears nothing from a leader for a random time between this
 *     and twice this starts an election. Randomizing it makes split votes rare.
 * @param heartbeatTicks how often a leader sends heartbeats; must be well below {@code
 *     electionTicks}
 * @param preVote whether to ask "would you vote for me?" before starting a real election
 * @param checkQuorum whether a leader steps down when it has not heard from a majority for {@code
 *     electionTicks}, and followers ignore vote requests while they still hear from their leader
 * @param maxEntriesPerAppend the most entries in one AppendEntries
 * @param maxInflightAppends how many AppendEntries a leader may have unacknowledged per follower
 */
public record RaftConfig(
    NodeId id,
    int electionTicks,
    int heartbeatTicks,
    boolean preVote,
    boolean checkQuorum,
    int maxEntriesPerAppend,
    int maxInflightAppends) {

  public RaftConfig {
    Objects.requireNonNull(id);
    if (heartbeatTicks < 1
        || electionTicks <= heartbeatTicks
        || maxEntriesPerAppend < 1
        || maxInflightAppends < 1) {
      throw new IllegalArgumentException("invalid RaftConfig: " + this);
    }
  }

  /** Elections after 10 to 20 ticks, heartbeats every 2, pre-vote and check-quorum on. */
  public static RaftConfig defaults(NodeId id) {
    return new RaftConfig(id, 10, 2, true, true, 64, 32);
  }

  /** A copy with pre-vote and check-quorum set as given. */
  public RaftConfig withPreVoteAndCheckQuorum(boolean preVote, boolean checkQuorum) {
    return new RaftConfig(
        id,
        electionTicks,
        heartbeatTicks,
        preVote,
        checkQuorum,
        maxEntriesPerAppend,
        maxInflightAppends);
  }
}

package io.distdb.raft;

import io.distdb.common.NodeId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Who is in a Raft group. Lists are kept sorted and duplicate-free, so iterating over them is
 * deterministic: the simulator depends on every node sending its messages in the same order on
 * every run of a seed.
 *
 * @param voters the members whose votes and acknowledgements count
 * @param learners members that receive the log but do not vote
 */
public record Membership(List<NodeId> voters, List<NodeId> learners) {

  // Declared before EMPTY: static fields initialize top to bottom, and EMPTY's constructor uses it.
  private static final Comparator<NodeId> BY_VALUE = Comparator.comparingInt(NodeId::value);

  /** A group nobody belongs to: what a node knows before it is added. */
  public static final Membership EMPTY = new Membership(List.of(), List.of());

  public Membership {
    voters = sorted(voters);
    learners = sorted(learners);
    for (NodeId v : voters) {
      if (learners.contains(v)) {
        throw new IllegalArgumentException(v + " is both a voter and a learner");
      }
    }
  }

  /** A group of voters only. */
  public static Membership ofVoters(NodeId... voters) {
    return new Membership(List.of(voters), List.of());
  }

  public boolean isVoter(NodeId id) {
    return voters.contains(id);
  }

  public boolean isLearner(NodeId id) {
    return learners.contains(id);
  }

  public boolean isMember(NodeId id) {
    return isVoter(id) || isLearner(id);
  }

  /** Voters and learners, sorted. */
  public List<NodeId> members() {
    return sorted(Stream.concat(voters.stream(), learners.stream()).toList());
  }

  /** How many voters make a majority. */
  public int quorum() {
    return voters.size() / 2 + 1;
  }

  /**
   * The membership after {@code change}.
   *
   * @throws IllegalArgumentException if the change makes no sense here: adding a member, promoting
   *     a voter, removing a stranger, or removing the last voter
   */
  public Membership apply(ConfigChange change) {
    NodeId n = change.node();
    List<NodeId> v = new ArrayList<>(voters);
    List<NodeId> l = new ArrayList<>(learners);
    switch (change.kind()) {
      case ADD_LEARNER -> {
        require(!isMember(n), n + " is already a member");
        l.add(n);
      }
      case PROMOTE -> {
        require(isLearner(n), n + " is not a learner");
        l.remove(n);
        v.add(n);
      }
      case REMOVE -> {
        require(isMember(n), n + " is not a member");
        require(!(isVoter(n) && voters.size() == 1), "cannot remove the last voter");
        v.remove(n);
        l.remove(n);
      }
    }
    return new Membership(v, l);
  }

  private static void require(boolean ok, String message) {
    if (!ok) {
      throw new IllegalArgumentException(message);
    }
  }

  private static List<NodeId> sorted(List<NodeId> ids) {
    return ids.stream().distinct().sorted(BY_VALUE).toList();
  }

  @Override
  public String toString() {
    return "voters" + voters + (learners.isEmpty() ? "" : " learners" + learners);
  }
}

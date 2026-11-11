package io.distdb.raft;

import java.util.Arrays;
import java.util.Objects;

/**
 * The state machine's state after applying every entry up to {@code index}; it replaces those
 * entries in the log. A leader sends it to a follower that needs entries the leader no longer has.
 *
 * @param index the last entry the state covers
 * @param term that entry's term, so the log consistency check still works at the boundary
 * @param membership the membership as of {@code index}
 * @param data the state machine's serialized state (opaque to Raft)
 */
public record Snapshot(long index, long term, Membership membership, byte[] data) {

  public Snapshot {
    Objects.requireNonNull(membership);
    Objects.requireNonNull(data);
    if (index < 0 || term < 0) {
      throw new IllegalArgumentException("bad snapshot index " + index + " term " + term);
    }
  }

  /** Where an empty log starts: nothing applied, with this initial membership. */
  public static Snapshot initial(Membership membership) {
    return new Snapshot(0, 0, membership, new byte[0]);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Snapshot s
        && index == s.index
        && term == s.term
        && membership.equals(s.membership)
        && Arrays.equals(data, s.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, term, membership, Arrays.hashCode(data));
  }

  @Override
  public String toString() {
    return "Snapshot[" + index + ":" + term + " " + membership + " " + data.length + "B]";
  }
}

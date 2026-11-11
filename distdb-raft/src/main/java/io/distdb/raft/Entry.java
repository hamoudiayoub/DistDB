package io.distdb.raft;

import java.util.Arrays;
import java.util.Objects;

/**
 * One slot of the replicated log.
 *
 * @param index position in the log, from 1
 * @param term the leader's term when the entry was created
 * @param type a client command, or a change of membership
 * @param data the command; for {@link Type#CONFIG}, the complete new {@link Membership}
 */
public record Entry(long index, long term, Type type, byte[] data) {

  /** What an entry carries. */
  public enum Type {
    /** A command for the state machine (empty for the no-op a new leader appends). */
    NORMAL,
    /** A membership change, which takes effect as soon as it is in a log, committed or not. */
    CONFIG
  }

  public Entry {
    Objects.requireNonNull(type);
    Objects.requireNonNull(data);
    if (index < 1 || term < 1) {
      throw new IllegalArgumentException("bad entry index " + index + " term " + term);
    }
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Entry e
        && index == e.index
        && term == e.term
        && type == e.type
        && Arrays.equals(data, e.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, term, type, Arrays.hashCode(data));
  }

  @Override
  public String toString() {
    return index + ":" + term + (type == Type.CONFIG ? ":config" : "") + "(" + data.length + "B)";
  }
}

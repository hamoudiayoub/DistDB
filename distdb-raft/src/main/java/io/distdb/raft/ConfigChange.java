package io.distdb.raft;

import io.distdb.common.NodeId;
import java.util.Objects;

/**
 * A change of one server (Ongaro's thesis, chapter 4). Changing one server at a time keeps every
 * majority of the old configuration overlapping every majority of the new one, so two leaders can
 * never be elected in the same term across a change.
 */
public record ConfigChange(Kind kind, NodeId node) {

  /** The kinds of change. */
  public enum Kind {
    /** Add a non-voting member, which receives the log but never counts towards a majority. */
    ADD_LEARNER,
    /** Turn a learner that has caught up into a voter. */
    PROMOTE,
    /** Remove a voter or a learner. */
    REMOVE
  }

  public ConfigChange {
    Objects.requireNonNull(kind);
    Objects.requireNonNull(node);
  }
}

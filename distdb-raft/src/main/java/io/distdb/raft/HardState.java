package io.distdb.raft;

import io.distdb.common.NodeId;

/**
 * What a node must have on disk before it answers anyone. Losing the term or the vote could let a
 * node vote twice in one term after a restart, and two leaders could then be elected in it.
 *
 * @param term the latest term this node has seen
 * @param vote whom it voted for in {@code term}, or null
 * @param commit the highest index it knows is committed (may lag; only a hint on restart)
 */
public record HardState(long term, NodeId vote, long commit) {

  /** A node that has never seen a term. */
  public static final HardState EMPTY = new HardState(0, null, 0);
}

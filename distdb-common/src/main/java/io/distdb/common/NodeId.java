package io.distdb.common;

/**
 * Identifies one data node or placement-driver process in the cluster. Ids are assigned by the
 * placement driver when a node first joins and are never reused.
 *
 * @param value a positive integer
 */
public record NodeId(int value) {
  public NodeId {
    if (value <= 0) {
      throw new IllegalArgumentException("node id must be positive: " + value);
    }
  }

  @Override
  public String toString() {
    return "n" + value;
  }
}

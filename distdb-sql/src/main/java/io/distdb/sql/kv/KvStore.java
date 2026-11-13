package io.distdb.sql.kv;

import java.util.List;

/**
 * The transactional key-value store under the SQL layer: in a cluster, Phase 7's transactions
 * ({@link TxnKvStore}); in unit tests, an in-memory store with the same isolation. Thread-safe.
 */
public interface KvStore {

  /** Starts a transaction that sees every transaction that committed before this call. */
  KvTxn begin();

  /** {@code n} fresh timestamps, unique cluster-wide and increasing (hidden row ids use them). */
  long[] timestamps(int n);

  /**
   * Ends every transaction that started below {@code ts}: afterwards none of them can commit, and
   * those that had locked keys have finished. Schema changes use it so that no writer with an old
   * schema survives them.
   */
  void fence(long ts);

  /** How many ranges hold {@code [start, end)}, as far as this node knows (for statistics). */
  default int rangeCount(byte[] start, byte[] end) {
    return 1;
  }

  /** A range as the placement driver last saw it; {@code end} null for the last one. */
  record RangeRow(
      long id,
      byte[] start,
      byte[] end,
      int leader,
      List<Integer> voters,
      List<Integer> learners) {}

  /** A data node as the placement driver sees it. */
  record NodeRow(int id, String address, boolean alive) {}

  /** The ranges and nodes of the cluster ({@code SHOW RANGES}, {@code SHOW NODES}). */
  record ClusterView(List<RangeRow> ranges, List<NodeRow> nodes) {}

  /** The cluster's layout; empty for a store that is not a cluster. */
  default ClusterView cluster() {
    return new ClusterView(List.of(), List.of());
  }
}

package io.distdb.sql.kv;

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
}

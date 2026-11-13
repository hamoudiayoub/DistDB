package io.distdb.sql.kv;

import java.util.List;
import java.util.Optional;

/**
 * One key-value transaction, as the SQL layer sees it: snapshot reads that include its own writes,
 * and writes buffered until {@link #commit()}. Synchronous; failures are {@link
 * io.distdb.sql.SqlException}s ({@code 40001} for those a retry may fix).
 */
public interface KvTxn {

  /** One key and value read by a scan. */
  record Entry(byte[] key, byte[] value) {}

  /** The transaction's snapshot timestamp. */
  long startTs();

  Optional<byte[]> get(byte[] key);

  /**
   * Up to {@code limit} entries of {@code [start, end)} in key order ({@code end} null: no bound;
   * {@code limit} 0: no limit).
   */
  List<Entry> scan(byte[] start, byte[] end, int limit);

  void put(byte[] key, byte[] value);

  void delete(byte[] key);

  /** Commits; returns the commit timestamp (the start timestamp if nothing was written). */
  long commit();

  /** Abandons the transaction. Safe to call after a failure, or twice. */
  void rollback();
}

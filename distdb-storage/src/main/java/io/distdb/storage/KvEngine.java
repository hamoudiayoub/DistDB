package io.distdb.storage;

import java.util.Optional;

/**
 * A local, ordered key-value store: the only storage interface the layers above use. Keys are
 * ordered as unsigned bytes ({@link io.distdb.common.Bytes#COMPARATOR}). Implementations are
 * thread-safe.
 */
public interface KvEngine extends AutoCloseable {

  /**
   * Applies every operation in {@code batch} atomically: after a crash, either all of them are
   * visible or none are. Batches are applied in the order they are written, and recovery always
   * restores a prefix of that order.
   *
   * @param sync if {@code true}, the batch (and every batch before it) is durable when this
   *     returns; if {@code false} it may be lost in a crash, but never out of order
   */
  void write(WriteBatch batch, boolean sync);

  /** The current value of {@code key}, or empty if it is absent or deleted. */
  Optional<byte[]> get(byte[] key);

  /**
   * The current entries in {@code [startInclusive, endExclusive)}, in key order. A null {@code
   * endExclusive} means no upper bound. The iterator sees a consistent snapshot taken now.
   */
  KvIterator scan(byte[] startInclusive, byte[] endExclusive);

  /** A consistent point-in-time view; see {@link EngineSnapshot}. */
  EngineSnapshot snapshot();

  /** Makes acknowledged writes durable and releases resources. */
  @Override
  void close();
}

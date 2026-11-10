package io.distdb.storage.lsm;

/**
 * A cursor over internal keys in {@link InternalKey#COMPARATOR} order. Memtables, SSTable blocks,
 * whole SSTables and merges of all of them share this interface, so they compose freely.
 *
 * <p>{@link #key()} and {@link #value()} return arrays that callers must not modify. Methods throw
 * {@link io.distdb.storage.StorageException} on I/O errors or corruption.
 */
interface InternalIterator extends AutoCloseable {

  boolean valid();

  void seekToFirst();

  /** Positions at the first entry whose key is at least {@code target}. */
  void seek(byte[] target);

  void next();

  byte[] key();

  byte[] value();

  @Override
  default void close() {}
}

package io.distdb.storage;

import java.util.Iterator;

/**
 * Entries of a scan in key order. It pins the engine state it was created from (so files it reads
 * are not deleted under it): always close it, typically with try-with-resources.
 */
public interface KvIterator extends Iterator<KvEntry>, AutoCloseable {
  @Override
  void close();
}

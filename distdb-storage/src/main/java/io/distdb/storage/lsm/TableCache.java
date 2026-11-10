package io.distdb.storage.lsm;

import io.distdb.env.FileSystem;
import io.distdb.storage.StorageException;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

/** Keeps one open {@link TableReader} per live SSTable, opening them lazily. */
final class TableCache implements Closeable {

  private final FileSystem fs;
  private final Path dir;
  private final ConcurrentHashMap<Long, TableReader> open = new ConcurrentHashMap<>();

  TableCache(FileSystem fs, Path dir) {
    this.fs = fs;
    this.dir = dir;
  }

  TableReader get(long number) {
    return open.computeIfAbsent(
        number,
        n -> {
          try {
            return TableReader.open(fs.openReadOnly(LsmFiles.table(dir, n)), n);
          } catch (IOException e) {
            throw new StorageException("opening table " + n, e);
          }
        });
  }

  /** Closes the reader for a table about to be deleted (Windows cannot delete open files). */
  void evict(long number) {
    TableReader r = open.remove(number);
    if (r != null) {
      try {
        r.close();
      } catch (IOException ignored) {
        // The file is being deleted anyway.
      }
    }
  }

  @Override
  public void close() {
    for (Long n : open.keySet()) {
      evict(n);
    }
  }
}

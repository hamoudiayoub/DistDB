package io.distdb.env;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * The only way core logic touches disk. The durability contract below is exactly what the simulated
 * file system models when it crashes a node, so code that is correct against this contract survives
 * real power loss too.
 *
 * <p><b>Durability contract.</b>
 *
 * <ul>
 *   <li>Bytes written to a file but not yet covered by {@link FileHandle#sync()} may be lost on a
 *       crash, partly or wholly, and an unsynced region may come back torn (some pages new, some
 *       old, some zeroed).
 *   <li>After {@code sync()} returns, every earlier write to that file is durable.
 *   <li>Creating, renaming or deleting a file is durable only after {@link #syncDirectory} on its
 *       parent directory. Until then a crash may undo it.
 *   <li>{@link #rename} is atomic: after a crash the target holds either the old file or the new
 *       one, never a mix.
 * </ul>
 */
public interface FileSystem {

  /** Creates a new, empty file open for reading and writing; fails if it already exists. */
  FileHandle create(Path path) throws IOException;

  /** Opens an existing file for reading and writing (for example a WAL being recovered). */
  FileHandle open(Path path) throws IOException;

  /** Opens an existing file for reading only. */
  FileHandle openReadOnly(Path path) throws IOException;

  /** Whether a file or directory exists at {@code path}. */
  boolean exists(Path path);

  /**
   * Deletes the file at {@code path} if it exists.
   *
   * @return {@code true} if a file was deleted
   */
  boolean deleteIfExists(Path path) throws IOException;

  /** Atomically renames {@code from} to {@code to}, replacing {@code to} if it exists. */
  void rename(Path from, Path to) throws IOException;

  /** The entries of {@code dir}, sorted by name. */
  List<Path> list(Path dir) throws IOException;

  /** Creates {@code dir} and any missing parents; does nothing if it already exists. */
  void createDirectories(Path dir) throws IOException;

  /** Makes earlier creates, renames and deletes of entries in {@code dir} durable. */
  void syncDirectory(Path dir) throws IOException;
}

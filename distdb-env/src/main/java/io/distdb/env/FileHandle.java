package io.distdb.env;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An open file with positional reads and writes. See {@link FileSystem} for what survives a crash.
 * Positional I/O (rather than a file pointer) lets many readers share one handle safely.
 */
public interface FileHandle extends Closeable {

  /**
   * Reads up to {@code dst.remaining()} bytes starting at {@code position}.
   *
   * @return the number of bytes read, or {@code -1} if {@code position} is at or past the end
   */
  int read(ByteBuffer dst, long position) throws IOException;

  /** Writes all of {@code src} starting at {@code position}, growing the file if needed. */
  void write(ByteBuffer src, long position) throws IOException;

  /** The current size of the file in bytes, including unsynced writes. */
  long size() throws IOException;

  /** Shrinks the file to {@code size} bytes; does nothing if it is already that small. */
  void truncate(long size) throws IOException;

  /** Blocks until every earlier write to this file is durable. */
  void sync() throws IOException;

  /** Fills {@code dst} from {@code position}, failing if the file ends first. */
  default void readFully(ByteBuffer dst, long position) throws IOException {
    long pos = position;
    while (dst.hasRemaining()) {
      int n = read(dst, pos);
      if (n < 0) {
        throw new EOFException(
            "end of file at " + pos + " with " + dst.remaining() + " bytes still to read");
      }
      pos += n;
    }
  }
}

package io.distdb.storage.lsm;

import io.distdb.env.FileHandle;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Appends checksummed records to a log file; used for both the write-ahead log and the manifest.
 *
 * <p>Record format: {@code crc32c (4) | length (4) | payload}, where the checksum covers the length
 * and the payload. A record is written with a single positional write, so a crash can only tear the
 * record being written or ones after it, never one before. {@link LogReader} stops at the first
 * record that does not check out.
 */
final class LogWriter implements Closeable {

  static final int HEADER_BYTES = 8;

  private final FileHandle file;
  private long offset;

  LogWriter(FileHandle file, long startOffset) {
    this.file = file;
    this.offset = startOffset;
  }

  void add(byte[] payload) throws IOException {
    byte[] record = new byte[HEADER_BYTES + payload.length];
    putInt(record, 4, payload.length);
    System.arraycopy(payload, 0, record, HEADER_BYTES, payload.length);
    putInt(record, 0, Checksums.crc32c(record, 4, record.length - 4));
    file.write(ByteBuffer.wrap(record), offset);
    offset += record.length;
  }

  void sync() throws IOException {
    file.sync();
  }

  long size() {
    return offset;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  private static void putInt(byte[] b, int off, int v) {
    b[off] = (byte) (v >>> 24);
    b[off + 1] = (byte) (v >>> 16);
    b[off + 2] = (byte) (v >>> 8);
    b[off + 3] = (byte) v;
  }
}

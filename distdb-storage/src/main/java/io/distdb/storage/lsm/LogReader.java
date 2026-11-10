package io.distdb.storage.lsm;

import io.distdb.env.FileHandle;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Reads back the records a {@link LogWriter} wrote, stopping at the first damaged one. */
final class LogReader {

  /**
   * The records that checked out, in order.
   *
   * @param records payloads of every intact record before the first damaged one
   * @param validBytes length of the intact prefix of the file
   * @param damagedTail whether bytes after the intact prefix were discarded
   */
  record Result(List<byte[]> records, long validBytes, boolean damagedTail) {}

  private LogReader() {}

  /**
   * Reads every intact record. A damaged tail is expected after a crash (it holds writes that were
   * never synced) and is reported, not thrown: everything after the first bad record is ignored,
   * because accepting a later record without an earlier one would break write ordering.
   */
  static Result readAll(FileHandle file) throws IOException {
    long size = file.size();
    if (size > Integer.MAX_VALUE) {
      throw new IOException("log too large: " + size);
    }
    byte[] data = new byte[(int) size];
    file.readFully(ByteBuffer.wrap(data), 0);

    List<byte[]> records = new ArrayList<>();
    int pos = 0;
    while (pos + LogWriter.HEADER_BYTES <= data.length) {
      int crc = ByteSource.readInt(data, pos);
      int len = ByteSource.readInt(data, pos + 4);
      if (len <= 0 || len > data.length - pos - LogWriter.HEADER_BYTES) {
        break;
      }
      int actual = Checksums.crc32c(data, pos + 4, len + 4);
      if (actual != crc) {
        break;
      }
      byte[] payload = new byte[len];
      System.arraycopy(data, pos + LogWriter.HEADER_BYTES, payload, 0, len);
      records.add(payload);
      pos += LogWriter.HEADER_BYTES + len;
    }
    return new Result(records, pos, pos != data.length);
  }
}

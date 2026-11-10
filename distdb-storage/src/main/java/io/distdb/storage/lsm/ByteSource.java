package io.distdb.storage.lsm;

import io.distdb.storage.CorruptionException;
import java.util.Arrays;

/** Reads what {@link ByteSink} writes; every read is bounds-checked. Not thread-safe. */
final class ByteSource {

  private final byte[] buf;
  private final int end;
  private int pos;

  ByteSource(byte[] buf) {
    this(buf, 0, buf.length);
  }

  ByteSource(byte[] buf, int offset, int length) {
    this.buf = buf;
    this.pos = offset;
    this.end = offset + length;
  }

  boolean hasRemaining() {
    return pos < end;
  }

  int remaining() {
    return end - pos;
  }

  int position() {
    return pos;
  }

  int readByte() {
    need(1);
    return buf[pos++] & 0xFF;
  }

  int readInt() {
    need(4);
    int v =
        ((buf[pos] & 0xFF) << 24)
            | ((buf[pos + 1] & 0xFF) << 16)
            | ((buf[pos + 2] & 0xFF) << 8)
            | (buf[pos + 3] & 0xFF);
    pos += 4;
    return v;
  }

  long readLong() {
    long hi = readInt() & 0xFFFFFFFFL;
    long lo = readInt() & 0xFFFFFFFFL;
    return (hi << 32) | lo;
  }

  int readVarint() {
    int result = 0;
    for (int shift = 0; shift < 35; shift += 7) {
      int b = readByte();
      result |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        if (result < 0) {
          throw new CorruptionException("varint overflow");
        }
        return result;
      }
    }
    throw new CorruptionException("varint too long");
  }

  byte[] readBytes(int n) {
    if (n < 0) {
      throw new CorruptionException("negative length " + n);
    }
    need(n);
    byte[] out = Arrays.copyOfRange(buf, pos, pos + n);
    pos += n;
    return out;
  }

  byte[] readLengthPrefixed() {
    return readBytes(readInt());
  }

  private void need(int n) {
    if (end - pos < n) {
      throw new CorruptionException("truncated: need " + n + " bytes, have " + (end - pos));
    }
  }

  static int readInt(byte[] b, int off) {
    return ((b[off] & 0xFF) << 24)
        | ((b[off + 1] & 0xFF) << 16)
        | ((b[off + 2] & 0xFF) << 8)
        | (b[off + 3] & 0xFF);
  }
}

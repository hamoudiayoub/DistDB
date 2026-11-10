package io.distdb.storage.lsm;

import java.util.Arrays;

/** A growable byte buffer with big-endian fixed-width and varint writers. Not thread-safe. */
final class ByteSink {

  private byte[] buf;
  private int size;

  ByteSink(int initialCapacity) {
    buf = new byte[Math.max(16, initialCapacity)];
  }

  int size() {
    return size;
  }

  void reset() {
    size = 0;
  }

  byte[] toByteArray() {
    return Arrays.copyOf(buf, size);
  }

  void writeByte(int b) {
    ensure(1);
    buf[size++] = (byte) b;
  }

  void writeInt(int v) {
    ensure(4);
    buf[size++] = (byte) (v >>> 24);
    buf[size++] = (byte) (v >>> 16);
    buf[size++] = (byte) (v >>> 8);
    buf[size++] = (byte) v;
  }

  void writeLong(long v) {
    writeInt((int) (v >>> 32));
    writeInt((int) v);
  }

  /** Unsigned LEB128: 7 bits per byte, high bit set on all but the last byte. */
  void writeVarint(int v) {
    if (v < 0) {
      throw new IllegalArgumentException("varint must be non-negative: " + v);
    }
    while (v >= 0x80) {
      writeByte((v & 0x7F) | 0x80);
      v >>>= 7;
    }
    writeByte(v);
  }

  void writeBytes(byte[] b) {
    writeBytes(b, 0, b.length);
  }

  void writeBytes(byte[] b, int off, int len) {
    ensure(len);
    System.arraycopy(b, off, buf, size, len);
    size += len;
  }

  /** Writes a 4-byte length followed by the bytes. */
  void writeLengthPrefixed(byte[] b) {
    writeInt(b.length);
    writeBytes(b);
  }

  private void ensure(int extra) {
    if (size + extra > buf.length) {
      buf = Arrays.copyOf(buf, Math.max(buf.length * 2, size + extra));
    }
  }
}

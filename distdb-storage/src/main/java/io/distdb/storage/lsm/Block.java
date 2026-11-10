package io.distdb.storage.lsm;

import io.distdb.storage.CorruptionException;
import java.util.Arrays;
import java.util.Comparator;

/** A parsed SSTable block (see {@link BlockBuilder} for the format). Immutable and thread-safe. */
final class Block {

  private final byte[] data;
  private final int restartsOffset;
  private final int numRestarts;

  Block(byte[] data) {
    if (data.length < 4) {
      throw new CorruptionException("block too short: " + data.length);
    }
    this.data = data;
    this.numRestarts = ByteSource.readInt(data, data.length - 4);
    long restartsStart = (long) data.length - 4 - 4L * numRestarts;
    if (numRestarts <= 0 || restartsStart < 0) {
      throw new CorruptionException("bad restart count " + numRestarts);
    }
    this.restartsOffset = (int) restartsStart;
  }

  InternalIterator iterator(Comparator<byte[]> comparator) {
    return new Iter(comparator);
  }

  private int restartPoint(int i) {
    return ByteSource.readInt(data, restartsOffset + 4 * i);
  }

  private final class Iter implements InternalIterator {
    private final Comparator<byte[]> comparator;
    private int nextOffset = restartsOffset; // where the entry after the current one starts
    private byte[] key;
    private int valueOffset;
    private int valueLength;

    Iter(Comparator<byte[]> comparator) {
      this.comparator = comparator;
    }

    @Override
    public boolean valid() {
      return key != null;
    }

    @Override
    public void seekToFirst() {
      seekToRestart(0);
      parseNext();
    }

    @Override
    public void seek(byte[] target) {
      // Find the last restart point whose key is < target, then scan forward.
      int left = 0;
      int right = numRestarts - 1;
      while (left < right) {
        int mid = (left + right + 1) >>> 1;
        if (comparator.compare(keyAtRestart(mid), target) < 0) {
          left = mid;
        } else {
          right = mid - 1;
        }
      }
      seekToRestart(left);
      while (parseNext()) {
        if (comparator.compare(key, target) >= 0) {
          return;
        }
      }
    }

    @Override
    public void next() {
      parseNext();
    }

    @Override
    public byte[] key() {
      return key;
    }

    @Override
    public byte[] value() {
      return Arrays.copyOfRange(data, valueOffset, valueOffset + valueLength);
    }

    private void seekToRestart(int i) {
      nextOffset = restartPoint(i);
      key = new byte[0];
    }

    private byte[] keyAtRestart(int i) {
      ByteSource in = new ByteSource(data, restartPoint(i), restartsOffset - restartPoint(i));
      int shared = in.readVarint();
      int unshared = in.readVarint();
      in.readVarint();
      if (shared != 0) {
        throw new CorruptionException("restart entry with shared prefix");
      }
      return in.readBytes(unshared);
    }

    /** Decodes the entry at nextOffset; returns false (and invalidates) at the end. */
    private boolean parseNext() {
      if (nextOffset >= restartsOffset) {
        key = null;
        return false;
      }
      byte[] prev = key == null ? new byte[0] : key;
      ByteSource in = new ByteSource(data, nextOffset, restartsOffset - nextOffset);
      int shared = in.readVarint();
      int unshared = in.readVarint();
      int vlen = in.readVarint();
      if (shared > prev.length) {
        throw new CorruptionException("shared prefix longer than previous key");
      }
      byte[] k = Arrays.copyOf(prev, shared + unshared);
      byte[] suffix = in.readBytes(unshared);
      System.arraycopy(suffix, 0, k, shared, unshared);
      valueOffset = in.position();
      valueLength = vlen;
      if (valueOffset + vlen > restartsOffset) {
        throw new CorruptionException("value runs past end of block");
      }
      nextOffset = valueOffset + vlen;
      key = k;
      return true;
    }
  }
}

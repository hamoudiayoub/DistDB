package io.distdb.storage.lsm;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The in-memory write buffer: a concurrent skip list of internal keys. One writer at a time adds to
 * it (under the engine's write lock) while any number of readers read it without locks; readers
 * ignore entries newer than their sequence number, so a half-applied batch is never visible.
 */
final class Memtable {

  private static final int ENTRY_OVERHEAD = 48;

  private final ConcurrentSkipListMap<byte[], byte[]> map =
      new ConcurrentSkipListMap<>(InternalKey.COMPARATOR);
  private final AtomicLong approximateBytes = new AtomicLong();
  private volatile long maxSequence;

  void add(long sequence, byte kind, byte[] userKey, byte[] value) {
    byte[] ik = InternalKey.encode(userKey, sequence, kind);
    byte[] v = value == null ? new byte[0] : value;
    map.put(ik, v);
    approximateBytes.addAndGet(ik.length + v.length + ENTRY_OVERHEAD);
    if (sequence > maxSequence) {
      maxSequence = sequence;
    }
  }

  /** The newest entry for {@code userKey} visible at {@code sequence}, or null if none. */
  Hit get(byte[] userKey, long sequence) {
    Map.Entry<byte[], byte[]> e = map.ceilingEntry(InternalKey.seekKey(userKey, sequence));
    if (e == null || InternalKey.compareUserKey(e.getKey(), userKey) != 0) {
      return null;
    }
    return InternalKey.kind(e.getKey()) == InternalKey.PUT ? new Hit(e.getValue()) : Hit.DELETED;
  }

  long approximateBytes() {
    return approximateBytes.get();
  }

  boolean isEmpty() {
    return map.isEmpty();
  }

  long maxSequence() {
    return maxSequence;
  }

  InternalIterator iterator() {
    return new InternalIterator() {
      private Iterator<Map.Entry<byte[], byte[]>> it = map.entrySet().iterator();
      private Map.Entry<byte[], byte[]> current;

      @Override
      public boolean valid() {
        return current != null;
      }

      @Override
      public void seekToFirst() {
        it = map.entrySet().iterator();
        advance();
      }

      @Override
      public void seek(byte[] target) {
        it = map.tailMap(target, true).entrySet().iterator();
        advance();
      }

      @Override
      public void next() {
        advance();
      }

      private void advance() {
        current = it.hasNext() ? it.next() : null;
      }

      @Override
      public byte[] key() {
        return current.getKey();
      }

      @Override
      public byte[] value() {
        return current.getValue();
      }
    };
  }
}

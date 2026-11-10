package io.distdb.storage.lsm;

import java.util.Arrays;

/**
 * What the manifest records about one SSTable: its number, size and the internal-key range it
 * covers. Immutable.
 */
record FileMetadata(long number, long size, byte[] smallest, byte[] largest) {

  byte[] smallestUserKey() {
    return InternalKey.userKey(smallest);
  }

  byte[] largestUserKey() {
    return InternalKey.userKey(largest);
  }

  /** Whether {@code userKey} lies within this file's user-key range (inclusive). */
  boolean containsUserKey(byte[] userKey) {
    return InternalKey.compareUserKey(smallest, userKey) <= 0
        && InternalKey.compareUserKey(largest, userKey) >= 0;
  }

  /** Whether this file's user-key range intersects {@code [lo, hi]} (inclusive; null = open). */
  boolean overlapsInclusive(byte[] lo, byte[] hi) {
    boolean beforeLo = lo != null && InternalKey.compareUserKey(largest, lo) < 0;
    boolean afterHi = hi != null && InternalKey.compareUserKey(smallest, hi) > 0;
    return !beforeLo && !afterHi;
  }

  /** Whether this file's user-key range intersects {@code [start, end)} (null end = open). */
  boolean overlapsHalfOpen(byte[] start, byte[] end) {
    boolean beforeStart = InternalKey.compareUserKey(largest, start) < 0;
    boolean atOrAfterEnd = end != null && InternalKey.compareUserKey(smallest, end) >= 0;
    return !beforeStart && !atOrAfterEnd;
  }

  @Override
  public String toString() {
    return "#" + number + "(" + size + "B)";
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof FileMetadata f
        && f.number == number
        && f.size == size
        && Arrays.equals(f.smallest, smallest)
        && Arrays.equals(f.largest, largest);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(number);
  }
}

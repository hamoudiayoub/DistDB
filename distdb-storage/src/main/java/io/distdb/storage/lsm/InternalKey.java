package io.distdb.storage.lsm;

import java.util.Arrays;
import java.util.Comparator;

/**
 * The key the engine stores internally: the user key followed by an 8-byte trailer holding a
 * sequence number and an operation kind, {@code (seq << 8) | kind}.
 *
 * <p>Internal keys order by user key ascending, then by sequence number <em>descending</em>, so the
 * newest version of a user key comes first. Every write gets a fresh sequence number, so internal
 * keys are unique. A read at sequence {@code s} seeks to {@code (key, s)} and the first entry with
 * the same user key is the newest version visible at {@code s}. This is what makes snapshots,
 * lock-free reads alongside writes, and safe compaction possible.
 */
final class InternalKey {

  static final int TRAILER_BYTES = 8;
  static final byte DELETE = 0;
  static final byte PUT = 1;
  static final long MAX_SEQUENCE = (1L << 56) - 1;

  /** Unsigned order on the user-key part, then newest sequence first. */
  static final Comparator<byte[]> COMPARATOR =
      (a, b) -> {
        int c =
            Arrays.compareUnsigned(a, 0, a.length - TRAILER_BYTES, b, 0, b.length - TRAILER_BYTES);
        if (c != 0) {
          return c;
        }
        return Long.compare(trailer(b), trailer(a));
      };

  private InternalKey() {}

  static byte[] encode(byte[] userKey, long sequence, byte kind) {
    if (sequence < 0 || sequence > MAX_SEQUENCE) {
      throw new IllegalArgumentException("sequence out of range: " + sequence);
    }
    byte[] out = Arrays.copyOf(userKey, userKey.length + TRAILER_BYTES);
    long t = (sequence << 8) | (kind & 0xFF);
    for (int i = 0; i < 8; i++) {
      out[userKey.length + i] = (byte) (t >>> (56 - 8 * i));
    }
    return out;
  }

  /**
   * The smallest internal key that is at least as new as nothing past {@code sequence}: seeking to
   * it lands on the newest version of {@code userKey} with a sequence number at most {@code
   * sequence}, or on the next user key if there is none.
   */
  static byte[] seekKey(byte[] userKey, long sequence) {
    return encode(userKey, sequence, PUT);
  }

  static long trailer(byte[] internalKey) {
    long t = 0;
    for (int i = internalKey.length - TRAILER_BYTES; i < internalKey.length; i++) {
      t = (t << 8) | (internalKey[i] & 0xFF);
    }
    return t;
  }

  static long sequence(byte[] internalKey) {
    return trailer(internalKey) >>> 8;
  }

  static byte kind(byte[] internalKey) {
    return internalKey[internalKey.length - 1];
  }

  static byte[] userKey(byte[] internalKey) {
    return Arrays.copyOf(internalKey, internalKey.length - TRAILER_BYTES);
  }

  /** Compares the user-key part of {@code internalKey} with {@code userKey}. */
  static int compareUserKey(byte[] internalKey, byte[] userKey) {
    return Arrays.compareUnsigned(
        internalKey, 0, internalKey.length - TRAILER_BYTES, userKey, 0, userKey.length);
  }

  /** Whether two internal keys have the same user key. */
  static boolean sameUserKey(byte[] a, byte[] b) {
    return Arrays.equals(a, 0, a.length - TRAILER_BYTES, b, 0, b.length - TRAILER_BYTES);
  }
}

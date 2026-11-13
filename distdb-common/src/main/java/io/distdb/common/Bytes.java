package io.distdb.common;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Optional;

/**
 * Helpers for keys as byte arrays. Throughout DistDB, keys compare as unsigned bytes,
 * lexicographically, with a shorter key ordered before any longer key it is a prefix of. That is
 * the order of the storage engine, of ranges and of scans.
 */
public final class Bytes {

  /** The empty key: the smallest key there is. */
  public static final byte[] EMPTY = new byte[0];

  /** Unsigned lexicographic order, the order of every key in the system. */
  public static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;

  private static final HexFormat HEX = HexFormat.of();

  private Bytes() {}

  /** Compares two keys in unsigned lexicographic order. */
  public static int compare(byte[] a, byte[] b) {
    return Arrays.compareUnsigned(a, b);
  }

  /** Whether {@code key} begins with {@code prefix}. */
  public static boolean startsWith(byte[] key, byte[] prefix) {
    return key.length >= prefix.length
        && Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
  }

  /**
   * The smallest key greater than every key that starts with {@code prefix}, so that {@code
   * [prefix, prefixEnd(prefix))} is exactly the set of keys with that prefix. Empty when no such
   * key exists, because the prefix is empty or all {@code 0xFF}; a scan then has no upper bound.
   */
  public static Optional<byte[]> prefixEnd(byte[] prefix) {
    for (int i = prefix.length - 1; i >= 0; i--) {
      if (prefix[i] != (byte) 0xFF) {
        byte[] end = Arrays.copyOf(prefix, i + 1);
        end[i]++;
        return Optional.of(end);
      }
    }
    return Optional.empty();
  }

  /** The smallest key greater than {@code key}: {@code key} followed by a zero byte. */
  public static byte[] successor(byte[] key) {
    return Arrays.copyOf(key, key.length + 1);
  }

  /** Lowercase hexadecimal, for logs and debugging. */
  public static String toHex(byte[] bytes) {
    return HEX.formatHex(bytes);
  }
}

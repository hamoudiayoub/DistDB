package io.distdb.storage.lsm;

/**
 * A bloom filter over the user keys of one SSTable, so a point lookup can skip files that cannot
 * contain the key without reading any of their blocks.
 *
 * <p>Each key sets {@code k} bits chosen by double hashing ({@code h1 + i * h2}) from one 64-bit
 * hash. With {@code b} bits per key the false-positive rate is about {@code 0.6185^b}: 10 bits
 * gives about 1%. False negatives are impossible. Format: the bit array followed by one byte
 * holding {@code k}.
 */
final class BloomFilter {

  private BloomFilter() {}

  /** FNV-1a over the bytes, then a 64-bit finalizer (from MurmurHash3) to spread the bits. */
  static long hash(byte[] b, int off, int len) {
    long h = 0xcbf29ce484222325L;
    for (int i = off; i < off + len; i++) {
      h ^= b[i] & 0xFF;
      h *= 0x100000001b3L;
    }
    h ^= len;
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    return h;
  }

  static byte[] build(long[] hashes, int count, int bitsPerKey) {
    int k = (int) Math.max(1, Math.min(30, Math.round(bitsPerKey * 0.69)));
    long bits = Math.max(64, (long) count * bitsPerKey);
    int bytes = (int) ((bits + 7) / 8);
    bits = bytes * 8L;
    byte[] filter = new byte[bytes + 1];
    filter[bytes] = (byte) k;
    for (int n = 0; n < count; n++) {
      long h = hashes[n];
      int h1 = (int) h;
      int h2 = (int) (h >>> 32);
      for (int i = 0; i < k; i++) {
        int bit = (int) Math.floorMod(h1 + (long) i * h2, bits);
        filter[bit >>> 3] |= (byte) (1 << (bit & 7));
      }
    }
    return filter;
  }

  static boolean mayContain(byte[] filter, long hash) {
    if (filter.length < 2) {
      return true;
    }
    int k = filter[filter.length - 1];
    long bits = (filter.length - 1) * 8L;
    int h1 = (int) hash;
    int h2 = (int) (hash >>> 32);
    for (int i = 0; i < k; i++) {
      int bit = (int) Math.floorMod(h1 + (long) i * h2, bits);
      if ((filter[bit >>> 3] & (1 << (bit & 7))) == 0) {
        return false;
      }
    }
    return true;
  }
}

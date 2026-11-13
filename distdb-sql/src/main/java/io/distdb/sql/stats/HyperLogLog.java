package io.distdb.sql.stats;

/**
 * Counts distinct values in fixed memory (Flajolet et al., 2007). Each value's 64-bit hash picks
 * one of 2^12 registers with its first 12 bits; the register keeps the largest "rank" seen, the
 * position of the first 1 bit in the rest. A register that saw rank r likely saw about 2^r values,
 * and the harmonic mean over all registers gives the estimate, with a standard error of about
 * 1.04/sqrt(4096), 1.6%. For small counts, empty registers give a better estimate (linear
 * counting). Two sketches merge by taking each register's maximum, so ranges could build their own.
 */
public final class HyperLogLog {

  static final int P = 12;
  static final int M = 1 << P;

  private final byte[] registers = new byte[M];

  public void add(long hash) {
    int index = (int) (hash >>> (64 - P));
    // A 1 below the bits used caps the rank, so a hash of all zeros still terminates.
    long rest = (hash << P) | (1L << (P - 1));
    int rank = Long.numberOfLeadingZeros(rest) + 1;
    if (rank > registers[index]) {
      registers[index] = (byte) rank;
    }
  }

  public void merge(HyperLogLog other) {
    for (int i = 0; i < M; i++) {
      registers[i] = (byte) Math.max(registers[i], other.registers[i]);
    }
  }

  public long estimate() {
    double alpha = 0.7213 / (1 + 1.079 / M);
    double sum = 0;
    int zeros = 0;
    for (byte r : registers) {
      sum += Math.scalb(1.0, -r);
      if (r == 0) {
        zeros++;
      }
    }
    double e = alpha * M * M / sum;
    if (e <= 2.5 * M && zeros > 0) {
      e = M * Math.log((double) M / zeros); // linear counting, for small cardinalities
    }
    return Math.round(e);
  }

  /** A 64-bit hash of {@code bytes} (FNV-1a, then MurmurHash3's finalizer to spread the bits). */
  public static long hash(byte[] bytes) {
    long h = 0xcbf29ce484222325L;
    for (byte b : bytes) {
      h ^= b & 0xFF;
      h *= 0x100000001b3L;
    }
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    return h;
  }
}

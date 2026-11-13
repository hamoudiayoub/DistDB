package io.distdb.bench;

/**
 * YCSB's scrambled Zipfian key chooser (Gray et al., "Quickly generating billion-record synthetic
 * databases"): item ranks follow Zipf with theta 0.99, so a few keys are hot, and the ranks are
 * hashed so that the hot keys are spread over the key space (and so over the ranges) instead of all
 * sitting at its start.
 */
final class Zipf {

  private static final double THETA = 0.99;

  private final long items;
  private final double zetan;
  private final double alpha;
  private final double eta;

  Zipf(long items) {
    this.items = items;
    double z = 0;
    for (long i = 1; i <= items; i++) {
      z += 1 / Math.pow(i, THETA);
    }
    this.zetan = z;
    double zeta2 = 1 + 1 / Math.pow(2, THETA);
    this.alpha = 1 / (1 - THETA);
    this.eta = (1 - Math.pow(2.0 / items, 1 - THETA)) / (1 - zeta2 / zetan);
  }

  /** The next key, 0 to items - 1, from a uniform {@code u} in [0, 1). */
  long next(double u) {
    double uz = u * zetan;
    long rank;
    if (uz < 1) {
      rank = 0;
    } else if (uz < 1 + Math.pow(0.5, THETA)) {
      rank = 1;
    } else {
      rank = (long) (items * Math.pow(eta * u - eta + 1, alpha));
    }
    return Math.floorMod(fnv(Math.min(rank, items - 1)), items);
  }

  private static long fnv(long v) {
    long h = 0xcbf29ce484222325L;
    for (int i = 0; i < 8; i++) {
      h ^= (v >>> (i * 8)) & 0xFF;
      h *= 0x100000001b3L;
    }
    return h;
  }
}

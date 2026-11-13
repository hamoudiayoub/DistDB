package io.distdb.bench;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Latencies with 1% precision at any scale, in the manner of HdrHistogram: a value's bucket is its
 * power of two and its next 7 bits, so 128 buckets per doubling (0.8% wide). Thread-safe and
 * lock-free; nanoseconds from 1 to 2^40 (18 minutes).
 */
final class Histogram {

  private static final int SUB_BITS = 7;
  private static final int SUB = 1 << SUB_BITS;
  private static final int MAX_EXP = 41;

  private final AtomicLongArray counts = new AtomicLongArray(MAX_EXP * SUB);
  private final LongAdder total = new LongAdder();
  private final LongAdder sum = new LongAdder();
  private final java.util.concurrent.atomic.AtomicLong max =
      new java.util.concurrent.atomic.AtomicLong();

  void record(long nanos) {
    long v = Math.max(1, nanos);
    counts.incrementAndGet(index(v));
    total.increment();
    sum.add(v);
    max.accumulateAndGet(v, Math::max);
  }

  static int index(long v) {
    int exp = 63 - Long.numberOfLeadingZeros(v);
    if (exp < SUB_BITS) {
      return (int) v; // small values are exact
    }
    int sub = (int) ((v >>> (exp - SUB_BITS)) & (SUB - 1));
    return Math.min((exp - SUB_BITS + 1) * SUB + sub, MAX_EXP * SUB - 1);
  }

  /** The smallest value that falls in bucket {@code i}. */
  static long lowest(int i) {
    if (i < SUB) {
      return i;
    }
    int exp = i / SUB + SUB_BITS - 1;
    long sub = i % SUB;
    return (1L << exp) | (sub << (exp - SUB_BITS));
  }

  long count() {
    return total.sum();
  }

  double meanNanos() {
    long n = count();
    return n == 0 ? 0 : (double) sum.sum() / n;
  }

  long maxNanos() {
    return max.get();
  }

  /** The {@code q}-quantile (0..1) in nanoseconds: the bucket's midpoint. */
  long quantile(double q) {
    long n = count();
    if (n == 0) {
      return 0;
    }
    long rank = Math.max(1, (long) Math.ceil(q * n));
    long seen = 0;
    for (int i = 0; i < counts.length(); i++) {
      seen += counts.get(i);
      if (seen >= rank) {
        long lo = lowest(i);
        long hi = i + 1 < counts.length() ? lowest(i + 1) : lo;
        return Math.min(max.get(), (lo + hi) / 2);
      }
    }
    return max.get();
  }

  void add(Histogram other) {
    for (int i = 0; i < counts.length(); i++) {
      long c = other.counts.get(i);
      if (c != 0) {
        counts.addAndGet(i, c);
      }
    }
    total.add(other.total.sum());
    sum.add(other.sum.sum());
    max.accumulateAndGet(other.max.get(), Math::max);
  }
}

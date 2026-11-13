package io.distdb.common.metrics;

import java.util.concurrent.atomic.LongAdder;

/**
 * Latencies in fixed buckets, the way Prometheus histograms count them: per bucket, how many
 * observations were at most its upper bound. Cheap and thread-safe (one {@link LongAdder} per
 * bucket); quantiles are estimated by interpolating inside a bucket.
 */
public final class LatencyHistogram {

  /** Bucket upper bounds, in seconds: 100 µs to 10 s. */
  public static final double[] BOUNDS = {
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
  };

  private final LongAdder[] buckets = new LongAdder[BOUNDS.length + 1];
  private final LongAdder count = new LongAdder();
  private final LongAdder sumNanos = new LongAdder();

  public LatencyHistogram() {
    for (int i = 0; i < buckets.length; i++) {
      buckets[i] = new LongAdder();
    }
  }

  public void record(long nanos) {
    double seconds = nanos / 1e9;
    int i = 0;
    while (i < BOUNDS.length && seconds > BOUNDS[i]) {
      i++;
    }
    buckets[i].increment();
    count.increment();
    sumNanos.add(Math.max(0, nanos));
  }

  /** A consistent-enough copy: per-bucket counts (not cumulative), total count, sum. */
  public record Snapshot(long[] buckets, long count, double sumSeconds) {

    /** Observations at most {@code BOUNDS[i]}, as Prometheus' {@code le} buckets report. */
    public long cumulative(int i) {
      long c = 0;
      for (int j = 0; j <= i && j < buckets.length; j++) {
        c += buckets[j];
      }
      return c;
    }

    /** The {@code q}-quantile in seconds, interpolated inside its bucket; 0 if empty. */
    public double quantile(double q) {
      if (count == 0) {
        return 0;
      }
      double rank = q * count;
      long seen = 0;
      for (int i = 0; i < buckets.length; i++) {
        if (seen + buckets[i] >= rank) {
          double lo = i == 0 ? 0 : BOUNDS[i - 1];
          double hi = i < BOUNDS.length ? BOUNDS[i] : BOUNDS[BOUNDS.length - 1] * 2;
          double within = buckets[i] == 0 ? 0 : (rank - seen) / buckets[i];
          return lo + (hi - lo) * within;
        }
        seen += buckets[i];
      }
      return BOUNDS[BOUNDS.length - 1];
    }
  }

  public Snapshot snapshot() {
    long[] b = new long[buckets.length];
    for (int i = 0; i < b.length; i++) {
      b[i] = buckets[i].sum();
    }
    return new Snapshot(b, count.sum(), sumNanos.sum() / 1e9);
  }
}

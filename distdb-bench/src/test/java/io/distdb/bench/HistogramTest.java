package io.distdb.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.Arrays;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/** The benchmark histogram's precision, and the Zipfian key chooser's skew. */
class HistogramTest {

  @Test
  void quantilesAreWithinOnePercentAtEveryScale() {
    SplittableRandom r = new SplittableRandom(1);
    long[] values = new long[200_000];
    Histogram h = new Histogram();
    for (int i = 0; i < values.length; i++) {
      // log-uniform from 1 µs to 10 s
      values[i] = (long) Math.pow(10, 3 + r.nextDouble() * 7);
      h.record(values[i]);
    }
    Arrays.sort(values);
    for (double q : new double[] {0.5, 0.9, 0.99, 0.999}) {
      long exact = values[(int) Math.ceil(q * values.length) - 1];
      assertThat((double) h.quantile(q)).as("q=" + q).isCloseTo(exact, within(exact * 0.01));
    }
    assertThat(h.maxNanos()).isEqualTo(values[values.length - 1]);
    assertThat(h.count()).isEqualTo(values.length);
  }

  @Test
  void bucketsAreContiguous() {
    for (int i = 1; i < 2000; i++) {
      assertThat(Histogram.index(Histogram.lowest(i))).isEqualTo(i);
      assertThat(Histogram.lowest(i)).isGreaterThan(Histogram.lowest(i - 1));
    }
  }

  @Test
  void zipfianKeysAreSkewedButSpreadOverTheKeySpace() {
    Zipf z = new Zipf(10_000);
    SplittableRandom r = new SplittableRandom(2);
    int[] counts = new int[10_000];
    for (int i = 0; i < 200_000; i++) {
      long k = z.next(r.nextDouble());
      assertThat(k).isBetween(0L, 9_999L);
      counts[(int) k]++;
    }
    int[] sorted = counts.clone();
    Arrays.sort(sorted);
    // The hottest key gets several percent of the requests; most keys get very few.
    assertThat(sorted[sorted.length - 1]).isGreaterThan(200_000 / 50);
    assertThat(sorted[sorted.length / 2]).isLessThan(20);
    // Scrambled: the hottest keys are not all at the start of the key space.
    int hottest = 0;
    for (int i = 1; i < counts.length; i++) {
      if (counts[i] > counts[hottest]) {
        hottest = i;
      }
    }
    assertThat(hottest).isNotZero();
  }
}

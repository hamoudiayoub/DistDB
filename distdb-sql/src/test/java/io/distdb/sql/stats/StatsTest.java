package io.distdb.sql.stats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.distdb.sql.types.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** HyperLogLog's distinct counts and the histogram's fractions. */
class StatsTest {

  private static long hash(long v) {
    return HyperLogLog.hash(ByteBuffer.allocate(8).putLong(v).array());
  }

  private static HyperLogLog sketch(long from, long to) {
    HyperLogLog h = new HyperLogLog();
    for (long v = from; v < to; v++) {
      h.add(hash(v));
    }
    return h;
  }

  @Test
  void distinctCountsAreWithinAFewPercent() {
    // The standard error is 1.04 / sqrt(4096), about 1.6%; three of them is 5%.
    for (long n : new long[] {1_000, 10_000, 100_000, 1_000_000}) {
      double e = sketch(0, n).estimate();
      assertThat(e / n).as("n=" + n).isCloseTo(1.0, within(0.05));
    }
  }

  @Test
  void smallCountsAreNearlyExact() {
    assertThat(sketch(0, 10).estimate()).isEqualTo(10);
    assertThat(sketch(0, 0).estimate()).isZero();
  }

  @Test
  void duplicatesDoNotCount() {
    HyperLogLog h = new HyperLogLog();
    for (int round = 0; round < 10; round++) {
      for (long v = 0; v < 5_000; v++) {
        h.add(hash(v));
      }
    }
    assertThat((double) h.estimate()).isCloseTo(5_000, within(250.0));
  }

  @Test
  void mergingIsTheUnion() {
    // [0, 60k) and [40k, 100k) overlap: the union has 100k values, not 120k.
    HyperLogLog a = sketch(0, 60_000);
    a.merge(sketch(40_000, 100_000));
    assertThat((double) a.estimate()).isCloseTo(100_000, within(5_000.0));
  }

  @Test
  void theHistogramGivesFractionsBelowAValue() {
    // Equi-depth bounds over 0..1000: each of the 32 buckets holds a 32nd of the values.
    List<Object> bounds = new ArrayList<>();
    for (int i = 0; i <= 32; i++) {
      bounds.add((long) (i * 1000 / 32));
    }
    TableStats.ColumnStats c = new TableStats.ColumnStats(1, Type.INT, 0, 1000, bounds);
    assertThat(c.fractionBelow(-5L, false)).isZero();
    assertThat(c.fractionBelow(5_000L, false)).isEqualTo(1.0);
    assertThat(c.fractionBelow(250L, false)).isCloseTo(0.25, within(0.04));
    assertThat(c.fractionBelow(900L, true)).isCloseTo(0.9, within(0.04));
  }

  @Test
  void statisticsSurviveTheirEncoding() {
    TableStats s =
        new TableStats(
            42,
            3,
            List.of(
                new TableStats.ColumnStats(1, Type.INT, 0, 42, List.of(1L, 20L, 42L)),
                new TableStats.ColumnStats(2, Type.TEXT, 5, 7, List.of("a", "m", "z"))));
    assertThat(TableStats.decode(s.encode())).isEqualTo(s);
  }
}

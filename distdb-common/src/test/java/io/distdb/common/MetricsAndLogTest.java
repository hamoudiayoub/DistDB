package io.distdb.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.distdb.common.metrics.LatencyHistogram;
import org.junit.jupiter.api.Test;

/** The latency histogram's buckets and quantiles, and the structured log format. */
class MetricsAndLogTest {

  @Test
  void bucketsCountCumulativelyAndQuantilesInterpolate() {
    LatencyHistogram h = new LatencyHistogram();
    for (int i = 0; i < 90; i++) {
      h.record(2_000_000); // 2 ms: the (1 ms, 2.5 ms] bucket
    }
    for (int i = 0; i < 10; i++) {
      h.record(40_000_000); // 40 ms: the (25 ms, 50 ms] bucket
    }
    LatencyHistogram.Snapshot s = h.snapshot();
    assertThat(s.count()).isEqualTo(100);
    assertThat(s.sumSeconds()).isCloseTo(0.58, within(1e-9));
    int le1ms = 3;
    int le2500us = 4;
    assertThat(LatencyHistogram.BOUNDS[le2500us]).isEqualTo(0.0025);
    assertThat(s.cumulative(le1ms)).isZero();
    assertThat(s.cumulative(le2500us)).isEqualTo(90);
    assertThat(s.cumulative(LatencyHistogram.BOUNDS.length)).isEqualTo(100);
    assertThat(s.quantile(0.5)).isBetween(0.001, 0.0025);
    assertThat(s.quantile(0.95)).isBetween(0.025, 0.05);
    assertThat(new LatencyHistogram().snapshot().quantile(0.99)).isZero();
  }

  @Test
  void eventsAreKeyValuePairsQuotedWhenNeeded() {
    assertThat(Log.format("became leader", "range", 12, "term", 3))
        .isEqualTo("\"became leader\" range=12 term=3");
    assertThat(Log.format("x", "msg", "say \"hi\"", "empty", "", "eq", "a=b"))
        .isEqualTo("x msg=\"say \\\"hi\\\"\" empty=\"\" eq=\"a=b\"");
  }
}

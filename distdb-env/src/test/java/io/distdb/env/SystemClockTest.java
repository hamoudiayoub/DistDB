package io.distdb.env;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SystemClockTest {

  @Test
  void nanoTimeNeverGoesBackwards() {
    Clock clock = SystemClock.INSTANCE;
    long previous = clock.nanoTime();
    for (int i = 0; i < 100_000; i++) {
      long now = clock.nanoTime();
      assertThat(now).isGreaterThanOrEqualTo(previous);
      previous = now;
    }
  }

  @Test
  void wallTimeTracksTheSystemClock() {
    long before = System.currentTimeMillis();
    long wall = SystemClock.INSTANCE.wallTimeMillis();
    long after = System.currentTimeMillis();
    assertThat(wall).isBetween(before, after);
  }
}

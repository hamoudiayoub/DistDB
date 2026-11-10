package io.distdb.env.sim;

import io.distdb.env.Clock;
import java.time.Duration;

/**
 * Virtual time for simulation. Time stands still until the simulator advances it, so a run is a
 * pure function of its inputs. The wall clock is the monotonic clock plus a fixed epoch plus an
 * adjustable skew, so tests can make one node's wall clock wrong.
 */
public final class SimClock implements Clock {

  private static final long DEFAULT_EPOCH_MILLIS = 1_767_225_600_000L; // 2026-01-01T00:00:00Z

  private long nanos;
  private long wallSkewMillis;

  @Override
  public long nanoTime() {
    return nanos;
  }

  @Override
  public long wallTimeMillis() {
    return DEFAULT_EPOCH_MILLIS + nanos / 1_000_000 + wallSkewMillis;
  }

  /** Moves time forward by {@code d}. */
  public void advance(Duration d) {
    if (d.isNegative()) {
      throw new IllegalArgumentException("time cannot go backwards: " + d);
    }
    nanos += d.toNanos();
  }

  /** Moves time forward to {@code targetNanos}; does nothing if time is already past it. */
  public void advanceTo(long targetNanos) {
    nanos = Math.max(nanos, targetNanos);
  }

  /** Offsets the wall clock (not the monotonic clock) by {@code skewMillis}, e.g. to model NTP. */
  public void setWallSkewMillis(long skewMillis) {
    this.wallSkewMillis = skewMillis;
  }
}

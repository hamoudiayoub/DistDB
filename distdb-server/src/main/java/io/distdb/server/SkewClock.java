package io.distdb.server;

import io.distdb.env.Clock;
import io.distdb.env.SystemClock;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The real clock, with an adjustable offset on wall-clock time: how the chaos suite skews a node's
 * clock, since containers share the host kernel's clock (Jepsen uses libfaketime for this).
 * Monotonic time is never shifted: leases and timeouts measure durations, which skew does not
 * change.
 */
final class SkewClock implements Clock {

  private final AtomicLong offsetMillis = new AtomicLong();

  @Override
  public long nanoTime() {
    return SystemClock.INSTANCE.nanoTime();
  }

  @Override
  public long wallTimeMillis() {
    return SystemClock.INSTANCE.wallTimeMillis() + offsetMillis.get();
  }

  long offsetMillis() {
    return offsetMillis.get();
  }

  void offsetMillis(long offset) {
    offsetMillis.set(offset);
  }
}

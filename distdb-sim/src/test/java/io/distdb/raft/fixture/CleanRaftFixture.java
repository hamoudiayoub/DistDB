package io.distdb.raft.fixture;

import io.distdb.env.Clock;
import io.distdb.env.RandomSource;

/** Core-style code that uses only the seams; the determinism rule must accept it. */
public final class CleanRaftFixture {
  private CleanRaftFixture() {}

  static long electionDeadline(Clock clock, RandomSource random) {
    return clock.nanoTime() + random.nextLong(150, 300) * 1_000_000;
  }
}

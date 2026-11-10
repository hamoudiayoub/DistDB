package io.distdb.raft.fixture;

import java.time.Instant;
import java.util.Random;

/** A deliberate violation of the determinism rule, used only by ArchitectureRulesTest. */
public final class NondeterministicRaftFixture {
  private NondeterministicRaftFixture() {}

  static long electionTimeout() throws InterruptedException {
    long wall = System.currentTimeMillis();
    long mono = System.nanoTime();
    Thread.sleep(1);
    new Thread(() -> {}).start();
    return wall + mono + new Random().nextInt(150) + Instant.now().getNano();
  }
}

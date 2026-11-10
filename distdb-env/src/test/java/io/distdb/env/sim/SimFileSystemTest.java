package io.distdb.env.sim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.env.FileHandle;
import io.distdb.env.SplittableRandomSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** The simulated disk must model the durability contract exactly, or crash tests prove nothing. */
class SimFileSystemTest {

  private static final Path DIR = Path.of("data");

  private static SimFileSystem fs(long seed) throws IOException {
    SimFileSystem fs = new SimFileSystem(SplittableRandomSource.seeded(seed));
    fs.createDirectories(DIR);
    return fs;
  }

  private static void write(FileHandle f, String s, long pos) throws IOException {
    f.write(ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)), pos);
  }

  private static String read(SimFileSystem fs, Path p) throws IOException {
    try (FileHandle f = fs.openReadOnly(p)) {
      ByteBuffer buf = ByteBuffer.allocate((int) f.size());
      f.readFully(buf, 0);
      return new String(buf.array(), StandardCharsets.UTF_8);
    }
  }

  @Test
  void syncedFileWithSyncedDirectorySurvivesEveryCrash() throws IOException {
    for (long seed = 0; seed < 50; seed++) {
      SimFileSystem fs = fs(seed);
      Path p = DIR.resolve("a");
      try (FileHandle f = fs.create(p)) {
        write(f, "durable", 0);
        f.sync();
      }
      fs.syncDirectory(DIR);
      fs.crash();
      assertThat(read(fs, p)).isEqualTo("durable");
    }
  }

  @Test
  void fileWhoseDirectoryEntryWasNeverSyncedVanishes() throws IOException {
    SimFileSystem fs = fs(1);
    Path p = DIR.resolve("a");
    try (FileHandle f = fs.create(p)) {
      write(f, "data", 0);
      f.sync(); // the data is synced, but the name is not
    }
    fs.crash();
    assertThat(fs.exists(p)).isFalse();
  }

  @Test
  void unsyncedWritesMayBeLostOrTornButSyncedPrefixSurvives() throws IOException {
    boolean sawLoss = false;
    boolean sawSurvival = false;
    String synced = "x".repeat(SimFileSystem.PAGE_SIZE);
    String unsynced = "y".repeat(SimFileSystem.PAGE_SIZE * 3);
    for (long seed = 0; seed < 200; seed++) {
      SimFileSystem fs = fs(seed);
      Path p = DIR.resolve("log");
      try (FileHandle f = fs.create(p)) {
        write(f, synced, 0);
        f.sync();
        fs.syncDirectory(DIR);
        write(f, unsynced, synced.length());
      }
      fs.crash();
      String after = read(fs, p);
      assertThat(after).startsWith(synced);
      if (after.length() == synced.length()) {
        sawLoss = true;
      }
      if (after.equals(synced + unsynced)) {
        sawSurvival = true;
      }
    }
    assertThat(sawLoss).as("some crash lost the unsynced tail").isTrue();
    assertThat(sawSurvival).as("some crash kept the unsynced tail").isTrue();
  }

  /**
   * A log's synced end is rarely page-aligned, so the page being appended to holds both synced and
   * unsynced bytes. Regression test: tearing that page once zeroed synced bytes.
   */
  @Test
  void syncedBytesSharingAPageWithUnsyncedOnesSurvive() throws IOException {
    for (long seed = 0; seed < 500; seed++) {
      SimFileSystem fs = fs(seed);
      Path p = DIR.resolve("log");
      String synced = "s".repeat(100 + (int) (seed % 900));
      try (FileHandle f = fs.create(p)) {
        write(f, synced, 0);
        f.sync();
        fs.syncDirectory(DIR);
        write(f, "u".repeat(5000), synced.length());
      }
      fs.crash();
      assertThat(read(fs, p)).as("seed %d", seed).startsWith(synced);
    }
  }

  @Test
  void unsyncedRenameMayRevertButIsNeverAMix() throws IOException {
    boolean sawOld = false;
    boolean sawNew = false;
    for (long seed = 0; seed < 100; seed++) {
      SimFileSystem fs = fs(seed);
      Path current = DIR.resolve("CURRENT");
      Path tmp = DIR.resolve("CURRENT.tmp");
      try (FileHandle f = fs.create(current)) {
        write(f, "old", 0);
        f.sync();
      }
      fs.syncDirectory(DIR);
      try (FileHandle f = fs.create(tmp)) {
        write(f, "new", 0);
        f.sync();
      }
      fs.rename(tmp, current);
      if (seed % 2 == 0) {
        fs.syncDirectory(DIR);
      }
      fs.crash();
      String after = read(fs, current);
      assertThat(after).isIn("old", "new");
      if (seed % 2 == 0) {
        assertThat(after).isEqualTo("new");
      }
      sawOld |= after.equals("old");
      sawNew |= after.equals("new");
    }
    assertThat(sawOld).isTrue();
    assertThat(sawNew).isTrue();
  }

  @Test
  void armedCrashFailsTheNthMutationAndPlaysDeadUntilCrash() throws IOException {
    SimFileSystem fs = fs(3);
    fs.crashAfter(2);
    FileHandle f = fs.create(DIR.resolve("a")); // mutation 1
    assertThatThrownBy(() -> write(f, "x", 0)).isInstanceOf(SimulatedCrashException.class);
    assertThat(fs.isDead()).isTrue();
    assertThatThrownBy(() -> fs.list(DIR)).isInstanceOf(SimulatedCrashException.class);
    fs.crash();
    assertThat(fs.isDead()).isFalse();
    assertThatThrownBy(() -> f.size())
        .as("handles from before the crash are dead")
        .isInstanceOf(SimulatedCrashException.class);
    assertThat(fs.exists(DIR.resolve("a"))).isFalse();
  }

  @Test
  void deletionNotYetSyncedCanBeUndone() throws IOException {
    SimFileSystem fs = fs(4);
    Path p = DIR.resolve("a");
    try (FileHandle f = fs.create(p)) {
      write(f, "keep", 0);
      f.sync();
    }
    fs.syncDirectory(DIR);
    fs.deleteIfExists(p);
    assertThat(fs.exists(p)).isFalse();
    fs.crash();
    assertThat(read(fs, p)).isEqualTo("keep");
  }

  @Test
  void schedulerRunsTasksInDueOrderAndAdvancesVirtualTime() {
    SimClock clock = new SimClock();
    SimScheduler s = new SimScheduler(clock);
    StringBuilder order = new StringBuilder();
    s.schedule(java.time.Duration.ofMillis(30), () -> order.append('c'));
    s.schedule(java.time.Duration.ofMillis(10), () -> order.append('a'));
    var cancelled = s.schedule(java.time.Duration.ofMillis(20), () -> order.append('x'));
    s.execute(() -> order.append('0'));
    assertThat(cancelled.cancel()).isTrue();
    s.runUntilIdle();
    assertThat(order).hasToString("0ac");
    assertThat(clock.nanoTime()).isEqualTo(30_000_000L);
  }
}

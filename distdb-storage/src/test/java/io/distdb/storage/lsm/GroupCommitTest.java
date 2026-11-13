package io.distdb.storage.lsm;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.env.ExecutorScheduler;
import io.distdb.env.LocalFileSystem;
import io.distdb.env.Scheduler;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.storage.WriteBatch;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Concurrent synced writers share fsyncs, and none returns before its write is durable. */
class GroupCommitTest {

  private static final EngineOptions SMALL =
      new EngineOptions(64 << 10, 4096, 10, 128 << 10, 2, 8, 256 << 10, 4);

  private static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /** {@code threads} writers each make {@code each} synced writes; returns the keys written. */
  private static List<String> hammer(LsmEngine db, int threads, int each) throws Exception {
    ConcurrentLinkedQueue<String> written = new ConcurrentLinkedQueue<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    List<Thread> ts = new ArrayList<>();
    for (int w = 0; w < threads; w++) {
      int writer = w;
      ts.add(
          Thread.ofPlatform()
              .start(() -> {
                try {
                  for (int i = 0; i < each; i++) {
                    String k = "w%02d-%05d".formatted(writer, i);
                    db.write(new WriteBatch().put(b(k), b("v" + i)), true);
                    written.add(k);
                  }
                } catch (Throwable t) {
                  failure.compareAndSet(null, t);
                }
              }));
    }
    for (Thread t : ts) {
      t.join();
    }
    assertThat(failure.get()).isNull();
    return new ArrayList<>(written);
  }

  @Test
  void concurrentSyncedWritersShareFsyncs(@TempDir Path tmp) throws Exception {
    LsmEngine.Stats grouped;
    LsmEngine.Stats alone;
    try (ExecutorScheduler s = new ExecutorScheduler();
        LsmEngine db = LsmEngine.open(LocalFileSystem.INSTANCE, s, tmp.resolve("a"), SMALL)) {
      hammer(db, 8, 200);
      grouped = db.stats();
    }
    try (ExecutorScheduler s = new ExecutorScheduler();
        LsmEngine db =
            LsmEngine.open(
                LocalFileSystem.INSTANCE, s, tmp.resolve("b"), SMALL.withGroupCommit(false))) {
      hammer(db, 8, 200);
      alone = db.stats();
    }
    assertThat(grouped.writes()).isEqualTo(1600);
    assertThat(grouped.groupedWrites()).isEqualTo(1600);
    // Eight writers waiting on fsyncs: most fsyncs carry several writes.
    assertThat(grouped.walSyncs()).isLessThan(1600 * 3 / 4);
    assertThat(alone.walSyncs()).isEqualTo(1600);
  }

  @Test
  void everyWriteThatReturnedSurvivesACrash() throws Exception {
    for (long seed = 1; seed <= 5; seed++) {
      SimFileSystem fs = new SimFileSystem(SplittableRandomSource.seeded(seed));
      Path dir = Path.of("db");
      List<String> written;
      LsmEngine db = LsmEngine.open(fs, scheduler(), dir, SMALL);
      written = hammer(db, 6, 150);
      fs.crash(); // power loss: everything not fsynced is gone
      LsmEngine reopened = LsmEngine.open(fs, scheduler(), dir, SMALL);
      for (String k : written) {
        Optional<byte[]> v = reopened.get(b(k));
        assertThat(v).as("seed " + seed + ": " + k).isPresent();
      }
      reopened.close();
      assertThat(db.stats().walSyncs()).isLessThanOrEqualTo(900);
    }
  }

  private static Scheduler scheduler() {
    return new SimScheduler(new SimClock());
  }
}

package io.distdb.storage.lsm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.common.Bytes;
import io.distdb.env.ExecutorScheduler;
import io.distdb.env.LocalFileSystem;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.storage.EngineSnapshot;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import io.distdb.storage.StorageException;
import io.distdb.storage.WriteBatch;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Example-based behaviour of the engine. The randomized gate tests are in LsmModelTest. */
class LsmEngineTest {

  static final EngineOptions TINY = new EngineOptions(2048, 256, 10, 4096, 2, 6, 8192, 4);
  private static final Path DIR = Path.of("db");

  private SimFileSystem fs = new SimFileSystem(SplittableRandomSource.seeded(7));
  private SimScheduler scheduler = new SimScheduler(new SimClock());

  private static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static String s(byte[] b) {
    return new String(b, StandardCharsets.UTF_8);
  }

  private LsmEngine open() {
    scheduler = new SimScheduler(new SimClock());
    return LsmEngine.open(fs, scheduler, DIR, TINY);
  }

  private static List<String> keys(KvIterator it) {
    List<String> out = new ArrayList<>();
    try (it) {
      while (it.hasNext()) {
        out.add(s(it.next().key()));
      }
    }
    return out;
  }

  @Test
  void putGetDeleteAndEmptyValues() {
    try (LsmEngine db = open()) {
      db.write(new WriteBatch().put(b("a"), b("1")).put(b("empty"), new byte[0]), false);
      assertThat(db.get(b("a"))).hasValueSatisfying(v -> assertThat(s(v)).isEqualTo("1"));
      assertThat(db.get(b("empty"))).hasValueSatisfying(v -> assertThat(v).isEmpty());
      db.write(new WriteBatch().delete(b("a")), false);
      assertThat(db.get(b("a"))).isEmpty();
      assertThat(db.get(b("never"))).isEmpty();
    }
  }

  @Test
  void lastWriteInABatchWins() {
    try (LsmEngine db = open()) {
      db.write(new WriteBatch().put(b("k"), b("1")).delete(b("k")).put(b("k"), b("3")), false);
      assertThat(db.get(b("k"))).hasValueSatisfying(v -> assertThat(s(v)).isEqualTo("3"));
    }
  }

  @Test
  void dataSurvivesFlushesCompactionsAndReopen() {
    try (LsmEngine db = open()) {
      for (int i = 0; i < 3000; i++) {
        db.write(new WriteBatch().put(b("key%05d".formatted(i)), b("value-" + i)), false);
        scheduler.runReady();
      }
      scheduler.runUntilIdle();
      int deep = 0;
      for (int level = 1; level < Version.NUM_LEVELS; level++) {
        deep += db.numFilesAtLevel(level);
      }
      assertThat(deep)
          .as("compaction moved data below level 0: %s", db.describeLevels())
          .isPositive();
    }
    try (LsmEngine db = open()) {
      for (int i = 0; i < 3000; i += 97) {
        assertThat(db.get(b("key%05d".formatted(i))))
            .hasValueSatisfying(v -> assertThat(s(v)).startsWith("value-"));
      }
      assertThat(keys(db.scan(Bytes.EMPTY, null))).hasSize(3000);
    }
  }

  @Test
  void scanRespectsBoundsAndHidesDeletedKeys() {
    try (LsmEngine db = open()) {
      WriteBatch batch = new WriteBatch();
      for (String k : List.of("a", "b", "c", "d", "e")) {
        batch.put(b(k), b(k));
      }
      db.write(batch, false);
      db.write(new WriteBatch().delete(b("c")), false);
      assertThat(keys(db.scan(b("b"), b("e")))).containsExactly("b", "d");
      assertThat(keys(db.scan(b("b"), null))).containsExactly("b", "d", "e");
      assertThat(keys(db.scan(Bytes.EMPTY, b("a")))).isEmpty();
    }
  }

  @Test
  void snapshotIgnoresLaterWritesEvenAcrossCompaction() {
    try (LsmEngine db = open()) {
      db.write(new WriteBatch().put(b("k"), b("old")), false);
      try (EngineSnapshot snap = db.snapshot()) {
        for (int i = 0; i < 2000; i++) {
          db.write(new WriteBatch().put(b("k"), b("new" + i)).put(b("filler" + i), b("x")), false);
        }
        db.write(new WriteBatch().delete(b("k")), false);
        scheduler.runUntilIdle();
        assertThat(db.get(b("k"))).isEmpty();
        assertThat(snap.get(b("k"))).hasValueSatisfying(v -> assertThat(s(v)).isEqualTo("old"));
        assertThat(keys(snap.scan(Bytes.EMPTY, null))).containsExactly("k");
      }
    }
  }

  @Test
  void iteratorKeepsWorkingWhileCompactionReplacesItsFiles() {
    try (LsmEngine db = open()) {
      for (int i = 0; i < 1000; i++) {
        db.write(new WriteBatch().put(b("key%04d".formatted(i)), b("v")), false);
      }
      scheduler.runUntilIdle();
      try (KvIterator it = db.scan(Bytes.EMPTY, null)) {
        for (int i = 0; i < 1000; i++) {
          db.write(new WriteBatch().delete(b("key%04d".formatted(i))), false);
        }
        scheduler.runUntilIdle(); // compactions delete the files the iterator started on
        int n = 0;
        while (it.hasNext()) {
          it.next();
          n++;
        }
        assertThat(n).isEqualTo(1000);
      }
      assertThat(keys(db.scan(Bytes.EMPTY, null))).isEmpty();
    }
  }

  @Test
  void closedEngineRejectsCalls() {
    LsmEngine db = open();
    db.close();
    assertThatThrownBy(() -> db.get(b("a"))).isInstanceOf(StorageException.class);
    assertThatThrownBy(() -> db.write(new WriteBatch().put(b("a"), b("1")), true))
        .isInstanceOf(StorageException.class);
  }

  @Test
  void obsoleteFilesAreDeleted() throws IOException {
    try (LsmEngine db = open()) {
      for (int i = 0; i < 5000; i++) {
        db.write(new WriteBatch().put(b("k" + (i % 50)), b("v" + i)), false);
        scheduler.runReady();
      }
      scheduler.runUntilIdle();
      long tables = fs.list(DIR).stream().filter(p -> p.toString().endsWith(".sst")).count();
      int referenced = 0;
      for (int level = 0; level < Version.NUM_LEVELS; level++) {
        referenced += db.numFilesAtLevel(level);
      }
      assertThat(tables).isEqualTo(referenced);
      long logs = fs.list(DIR).stream().filter(p -> p.toString().endsWith(".log")).count();
      assertThat(logs).isLessThanOrEqualTo(2);
    }
  }

  /** The real file system and real threads: concurrent writers and readers, then a reopen. */
  @Test
  void concurrentUseOnTheRealFileSystem(@TempDir Path tmp) throws Exception {
    EngineOptions small = new EngineOptions(64 << 10, 4096, 10, 128 << 10, 2, 8, 256 << 10, 4);
    Path dir = tmp.resolve("db");
    ConcurrentHashMap<String, String> expected = new ConcurrentHashMap<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    try (ExecutorScheduler sched = new ExecutorScheduler(failure::set);
        LsmEngine db = LsmEngine.open(LocalFileSystem.INSTANCE, sched, dir, small)) {
      List<Thread> threads = new ArrayList<>();
      for (int w = 0; w < 4; w++) {
        int writer = w;
        threads.add(
            Thread.ofPlatform()
                .start(() -> {
                  for (int i = 0; i < 3000; i++) {
                    String k = "w%d-%05d".formatted(writer, i);
                    String v = "value-" + i + "-" + "x".repeat(i % 40);
                    db.write(new WriteBatch().put(b(k), b(v)), i % 100 == 0);
                    expected.put(k, v);
                  }
                }));
      }
      Thread reader =
          Thread.ofPlatform()
              .start(() -> {
                for (int round = 0; round < 50; round++) {
                  byte[] prev = null;
                  try (KvIterator it = db.scan(Bytes.EMPTY, null)) {
                    while (it.hasNext()) {
                      KvEntry e = it.next();
                      if (prev != null && Bytes.compare(prev, e.key()) >= 0) {
                        failure.set(new AssertionError("scan out of order"));
                      }
                      prev = e.key();
                    }
                  }
                }
              });
      for (Thread t : threads) {
        t.join();
      }
      reader.join();
    }
    assertThat(failure.get()).isNull();
    try (ExecutorScheduler sched = new ExecutorScheduler(failure::set);
        LsmEngine db = LsmEngine.open(LocalFileSystem.INSTANCE, sched, dir, small)) {
      assertThat(keys(db.scan(Bytes.EMPTY, null))).hasSize(expected.size());
      expected.forEach((k, v) ->
          assertThat(db.get(b(k))).hasValueSatisfying(got -> assertThat(s(got)).isEqualTo(v)));
    }
    assertThat(failure.get()).isNull();
  }
}

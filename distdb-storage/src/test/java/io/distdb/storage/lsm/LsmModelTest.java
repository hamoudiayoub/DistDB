package io.distdb.storage.lsm;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.common.Bytes;
import io.distdb.env.RandomSource;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.storage.EngineSnapshot;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import io.distdb.storage.WriteBatch;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * Phase 2 gate, part 1: the engine must behave exactly like a {@link TreeMap}.
 *
 * <p>Each seed runs a random mix of puts, deletes, multi-key batches, point reads, range scans,
 * snapshot reads, background steps (flush or compaction, interleaved at random points with the
 * foreground) and clean reopens, against tiny size limits so thousands of flushes and compactions
 * happen. After every read the result is compared with the model. A failure message names the seed
 * and operation index; rerunning that seed replays the exact same run.
 *
 * <p>Scale with {@code -Ddistdb.storage.modelOps=N} (total operations across all seeds).
 */
class LsmModelTest {

  private static final Path DIR = Path.of("db");
  private static final int SEEDS = 20;

  private static final byte[][] PREFIXES = {
    {}, {0x00}, {(byte) 0xFF}, "user".getBytes(), "user/".getBytes(), {0x00, 0x00}
  };

  @Test
  void engineMatchesTreeMapModel() {
    long totalOps = Long.getLong("distdb.storage.modelOps", 300_000);
    long perSeed = Math.max(1, totalOps / SEEDS);
    long flushes = 0;
    long compactions = 0;
    for (long seed = 1; seed <= SEEDS; seed++) {
      Run run = new Run(seed);
      run.execute(perSeed);
      flushes += run.levelZeroFilesSeen;
      compactions += run.deepFilesSeen;
    }
    assertThat(flushes).as("level-0 tables were produced").isPositive();
    assertThat(compactions).as("compactions produced deeper tables").isPositive();
  }

  private static final class Run {
    final long seed;
    final RandomSource random;
    final SimFileSystem fs;
    SimScheduler scheduler;
    LsmEngine db;
    final TreeMap<byte[], byte[]> model = new TreeMap<>(Bytes.COMPARATOR);
    final List<Snap> snapshots = new ArrayList<>();
    long op;
    long levelZeroFilesSeen;
    long deepFilesSeen;

    record Snap(EngineSnapshot snapshot, TreeMap<byte[], byte[]> model) {}

    Run(long seed) {
      this.seed = seed;
      this.random = SplittableRandomSource.seeded(seed);
      this.fs = new SimFileSystem(random.split());
    }

    void execute(long ops) {
      try {
        open();
        for (op = 0; op < ops; op++) {
          step();
        }
        scheduler.runUntilIdle();
        checkFullScan(db.scan(Bytes.EMPTY, null), model);
        closeAll();
      } catch (AssertionError | RuntimeException e) {
        throw new AssertionError(
            "seed "
                + seed
                + ", operation "
                + op
                + ", levels "
                + (db == null ? "-" : db.describeLevels()),
            e);
      }
    }

    void open() {
      scheduler = new SimScheduler(new SimClock());
      db = LsmEngine.open(fs, scheduler, DIR, LsmEngineTest.TINY);
    }

    void closeAll() {
      for (Snap s : snapshots) {
        s.snapshot().close();
      }
      snapshots.clear();
      db.close();
    }

    void step() {
      int dice = random.nextInt(1000);
      if (dice < 330) {
        byte[] k = key();
        byte[] v = value();
        db.write(new WriteBatch().put(k, v), random.nextInt(10) == 0);
        model.put(k, v);
      } else if (dice < 430) {
        byte[] k = key();
        db.write(new WriteBatch().delete(k), false);
        model.remove(k);
      } else if (dice < 500) {
        WriteBatch batch = new WriteBatch();
        TreeMap<byte[], byte[]> after = new TreeMap<>(model);
        int n = random.nextInt(2, 8);
        for (int i = 0; i < n; i++) {
          byte[] k = key();
          if (random.nextInt(4) == 0) {
            batch.delete(k);
            after.remove(k);
          } else {
            byte[] v = value();
            batch.put(k, v);
            after.put(k, v);
          }
        }
        db.write(batch, random.nextBoolean());
        model.clear();
        model.putAll(after);
      } else if (dice < 680) {
        byte[] k = key();
        assertSameValue(db.get(k), model.get(k), k);
      } else if (dice < 720) {
        byte[][] range = range();
        checkFullScan(db.scan(range[0], range[1]), slice(model, range[0], range[1]));
      } else if (dice < 735) {
        if (snapshots.size() < 4) {
          snapshots.add(new Snap(db.snapshot(), new TreeMap<>(model)));
        }
      } else if (dice < 770) {
        if (!snapshots.isEmpty()) {
          Snap s = snapshots.get(random.nextInt(snapshots.size()));
          if (random.nextBoolean()) {
            byte[] k = key();
            assertSameValue(s.snapshot().get(k), s.model().get(k), k);
          } else {
            byte[][] range = range();
            checkFullScan(
                s.snapshot().scan(range[0], range[1]), slice(s.model(), range[0], range[1]));
          }
        }
      } else if (dice < 780) {
        if (!snapshots.isEmpty()) {
          snapshots.remove(random.nextInt(snapshots.size())).snapshot().close();
        }
      } else if (dice < 970) {
        scheduler.runNext();
        observeLevels();
      } else if (dice < 998) {
        scheduler.runUntilIdle();
        observeLevels();
      } else {
        closeAll();
        open();
      }
    }

    void observeLevels() {
      levelZeroFilesSeen += db.numFilesAtLevel(0);
      for (int level = 1; level < Version.NUM_LEVELS; level++) {
        deepFilesSeen += db.numFilesAtLevel(level);
      }
    }

    /**
     * Keys from a small space with shared prefixes and 0x00/0xFF bytes, so collisions are common.
     */
    byte[] key() {
      byte[] prefix = PREFIXES[random.nextInt(PREFIXES.length)];
      int n = random.nextInt(400);
      byte[] k = Arrays.copyOf(prefix, prefix.length + (n % 3 == 0 ? 1 : 2));
      k[prefix.length] = (byte) (n >>> 1);
      if (k.length > prefix.length + 1) {
        k[prefix.length + 1] = (byte) (n * 37);
      }
      return k;
    }

    byte[] value() {
      byte[] v = new byte[random.nextInt(0, 80)];
      random.nextBytes(v);
      return v;
    }

    byte[][] range() {
      byte[] a = random.nextInt(8) == 0 ? Bytes.EMPTY : key();
      if (random.nextInt(6) == 0) {
        return new byte[][] {a, null};
      }
      byte[] b = key();
      return Bytes.compare(a, b) <= 0 ? new byte[][] {a, b} : new byte[][] {b, a};
    }

    static NavigableMap<byte[], byte[]> slice(TreeMap<byte[], byte[]> m, byte[] start, byte[] end) {
      return end == null ? m.tailMap(start, true) : m.subMap(start, true, end, false);
    }

    static void assertSameValue(Optional<byte[]> actual, byte[] expected, byte[] key) {
      if (expected == null) {
        assertThat(actual).as("get(%s)", Bytes.toHex(key)).isEmpty();
      } else {
        assertThat(actual).as("get(%s)", Bytes.toHex(key)).isPresent();
        assertThat(actual.get()).as("get(%s)", Bytes.toHex(key)).isEqualTo(expected);
      }
    }

    static void checkFullScan(KvIterator it, Map<byte[], byte[]> expected) {
      List<String> actual = new ArrayList<>();
      try (it) {
        while (it.hasNext()) {
          KvEntry e = it.next();
          actual.add(Bytes.toHex(e.key()) + "=" + Bytes.toHex(e.value()));
        }
      }
      List<String> want = new ArrayList<>();
      expected.forEach((k, v) -> want.add(Bytes.toHex(k) + "=" + Bytes.toHex(v)));
      assertThat(actual).containsExactlyElementsOf(want);
    }
  }
}

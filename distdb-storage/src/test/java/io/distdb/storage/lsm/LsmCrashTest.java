package io.distdb.storage.lsm;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.common.Bytes;
import io.distdb.env.RandomSource;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import io.distdb.storage.StorageException;
import io.distdb.storage.WriteBatch;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * Phase 2 gate, part 2: crashes never lose synced data and never expose partial or out-of-order
 * writes.
 *
 * <p>Each seed runs many crash cycles on one simulated disk. A cycle opens the engine (sometimes
 * with a crash armed inside recovery itself), checks what it recovered, then writes random batches
 * (some synced) while stepping background flushes and compactions, until power fails: either at an
 * armed I/O operation (in the middle of a log append, a flush, a compaction, a manifest switch) or
 * at a random moment between calls. Unsynced data is then lost or torn at random.
 *
 * <p>After every recovery the engine must hold <em>exactly</em> the result of some prefix of the
 * batches written so far, ending on a batch boundary (atomicity, ordering), and that prefix must
 * include every batch that was acknowledged with {@code sync = true} (durability).
 *
 * <p>Scale with {@code -Ddistdb.storage.crashSeeds=N}.
 */
class LsmCrashTest {

  private static final Path DIR = Path.of("db");
  private static final int CYCLES = 40;

  @Test
  void recoveryRestoresASyncedPrefixAfterEveryCrash() {
    int seeds = Integer.getInteger("distdb.storage.crashSeeds", 60);
    long crashesInsideOperations = 0;
    for (long seed = 1; seed <= seeds; seed++) {
      crashesInsideOperations += new Run(seed).execute();
    }
    assertThat(crashesInsideOperations).as("armed crashes fired mid-operation").isPositive();
  }

  /** One batch as the test wrote it, with the sequence numbers the engine must have given it. */
  private record Batch(long first, long last, List<byte[][]> ops) {}

  private static final class Run {
    final long seed;
    final RandomSource random;
    final SimFileSystem fs;
    final List<Batch> history = new ArrayList<>();
    long durable; // highest sequence number that must survive any crash
    int cycle;
    long armedCrashesFired;

    Run(long seed) {
      this.seed = seed;
      this.random = SplittableRandomSource.seeded(seed);
      this.fs = new SimFileSystem(random.split());
    }

    long execute() {
      try {
        for (cycle = 0; cycle < CYCLES; cycle++) {
          runCycle();
        }
        return armedCrashesFired;
      } catch (AssertionError | RuntimeException e) {
        throw new AssertionError("seed " + seed + ", cycle " + cycle, e);
      }
    }

    void runCycle() {
      SimScheduler scheduler = new SimScheduler(new SimClock());
      boolean armedDuringOpen = random.nextInt(4) == 0;
      if (armedDuringOpen) {
        fs.crashAfter(random.nextInt(1, 30));
      }
      LsmEngine db;
      try {
        db = LsmEngine.open(fs, scheduler, DIR, LsmEngineTest.TINY);
      } catch (StorageException e) {
        if (!fs.isDead()) {
          throw e; // a real recovery failure, not our simulated power cut
        }
        armedCrashesFired++;
        fs.crash();
        return;
      }
      verifyRecovered(db);

      if (!armedDuringOpen || !fs.isDead()) {
        fs.crashAfter(random.nextInt(1, 600));
      }
      int ops = random.nextInt(50, 1500);
      for (int i = 0; i < ops && !fs.isDead(); i++) {
        int dice = random.nextInt(100);
        try {
          if (dice < 55) {
            writeBatch(db);
          } else if (dice < 95) {
            scheduler.runNext();
          } else {
            db.get(key()); // reads must not break anything either
          }
        } catch (StorageException e) {
          if (!fs.isDead()) {
            throw e;
          }
        }
      }
      if (fs.isDead()) {
        armedCrashesFired++;
      } else if (random.nextInt(10) == 0) {
        db.close(); // clean shutdown: everything acknowledged becomes durable
        durable = history.isEmpty() ? durable : Math.max(durable, history.getLast().last());
      }
      fs.crash(); // power loss (or power cycle after a clean shutdown)
    }

    void writeBatch(LsmEngine db) {
      WriteBatch batch = new WriteBatch();
      List<byte[][]> ops = new ArrayList<>();
      int n = random.nextInt(1, 6);
      for (int i = 0; i < n; i++) {
        byte[] k = key();
        if (random.nextInt(5) == 0) {
          batch.delete(k);
          ops.add(new byte[][] {k, null});
        } else {
          byte[] v = new byte[random.nextInt(0, 120)];
          random.nextBytes(v);
          batch.put(k, v);
          ops.add(new byte[][] {k, v});
        }
      }
      long first = db.lastSequence() + 1;
      Batch b = new Batch(first, first + n - 1, ops);
      history.add(b); // added first: a write that fails may or may not have reached the log
      boolean sync = random.nextInt(3) == 0;
      db.write(batch, sync);
      if (sync) {
        durable = b.last();
      }
    }

    void verifyRecovered(LsmEngine db) {
      long recovered = db.lastSequence();
      assertThat(recovered)
          .as("recovered sequence vs last synced write")
          .isGreaterThanOrEqualTo(durable);
      boolean onBoundary = recovered == 0;
      TreeMap<byte[], byte[]> expected = new TreeMap<>(Bytes.COMPARATOR);
      List<Batch> kept = new ArrayList<>();
      for (Batch b : history) {
        if (b.last() <= recovered) {
          kept.add(b);
          for (byte[][] op : b.ops()) {
            if (op[1] == null) {
              expected.remove(op[0]);
            } else {
              expected.put(op[0], op[1]);
            }
          }
          onBoundary |= b.last() == recovered;
        } else {
          assertThat(b.first()).as("batch straddles recovery point").isGreaterThan(recovered);
        }
      }
      assertThat(onBoundary).as("recovered to %d, not a batch boundary", recovered).isTrue();
      // Lost batches are forgotten; the engine will reuse their sequence numbers.
      history.clear();
      history.addAll(kept);
      durable = recovered; // recovery flushed everything it replayed to a synced table

      List<String> actual = new ArrayList<>();
      try (KvIterator it = db.scan(Bytes.EMPTY, null)) {
        while (it.hasNext()) {
          KvEntry e = it.next();
          actual.add(Bytes.toHex(e.key()) + "=" + Bytes.toHex(e.value()));
        }
      }
      List<String> want = new ArrayList<>();
      expected.forEach((k, v) -> want.add(Bytes.toHex(k) + "=" + Bytes.toHex(v)));
      assertThat(actual)
          .as("contents after recovering to %d", recovered)
          .containsExactlyElementsOf(want);
      for (int i = 0; i < 20; i++) {
        byte[] k = key();
        byte[] v = expected.get(k);
        if (v == null) {
          assertThat(db.get(k)).isEmpty();
        } else {
          assertThat(db.get(k)).hasValueSatisfying(got -> assertThat(got).isEqualTo(v));
        }
      }
    }

    byte[] key() {
      int n = random.nextInt(300);
      return new byte[] {(byte) (n % 3), (byte) (n >>> 2), (byte) n};
    }
  }
}

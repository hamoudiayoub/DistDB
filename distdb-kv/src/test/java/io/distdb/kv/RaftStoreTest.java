package io.distdb.kv;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.common.NodeId;
import io.distdb.env.RandomSource;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.raft.Entry;
import io.distdb.raft.HardState;
import io.distdb.raft.Membership;
import io.distdb.raft.PersistedState;
import io.distdb.raft.Ready;
import io.distdb.raft.Snapshot;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class RaftStoreTest {

  private static final Path DIR = Path.of("/node");
  private static final EngineOptions SMALL =
      new EngineOptions(8 * 1024, 512, 10, 16 * 1024, 2, 8, 64 * 1024, 4);
  private static final Membership THREE =
      Membership.ofVoters(new NodeId(1), new NodeId(2), new NodeId(3));

  @Test
  void anEmptyStoreLoadsAsANodeThatStoredNothing() {
    SimFileSystem fs = new SimFileSystem(SplittableRandomSource.seeded(1));
    try (LsmEngine engine = open(fs)) {
      assertThat(new RaftStore(engine, 7).load()).isEqualTo(PersistedState.empty());
    }
  }

  @Test
  void groupsDoNotSeeEachOther() {
    SimFileSystem fs = new SimFileSystem(SplittableRandomSource.seeded(1));
    try (LsmEngine engine = open(fs)) {
      RaftStore a = new RaftStore(engine, 1);
      RaftStore b = new RaftStore(engine, 2);
      a.save(ready(new HardState(3, new NodeId(1), 0), entries(1, 5, 3), null, false));
      assertThat(b.load()).isEqualTo(PersistedState.empty());
      assertThat(a.load().entries()).hasSize(5);
    }
  }

  /**
   * Random Readies of every shape (appends, conflicting suffixes, local and received snapshots,
   * hard-state changes), with power cuts in between. After every step, what loads must equal a
   * plain in-memory model of the Ready contract.
   */
  @Test
  void whatLoadsAlwaysMatchesTheModelAcrossCrashes() {
    for (long seed = 1; seed <= 30; seed++) {
      RandomSource r = SplittableRandomSource.seeded(seed);
      SimFileSystem fs = new SimFileSystem(r.split());
      Model model = new Model();
      LsmEngine engine = open(fs);
      RaftStore store = new RaftStore(engine, 42);
      for (int step = 0; step < 300; step++) {
        Ready ready = randomReady(r, model);
        store.save(ready);
        model.save(ready);
        if (r.nextInt(10) == 0) {
          fs.crash(); // every save was synced, so nothing may be lost
          engine = open(fs);
          store = new RaftStore(engine, 42);
        }
        assertThat(store.load()).as("seed %d step %d", seed, step).isEqualTo(model.load());
      }
      engine.close();
    }
  }

  private static Ready randomReady(RandomSource r, Model m) {
    HardState hs = null;
    if (r.nextInt(3) == 0) {
      long term = m.hardState.term() + r.nextInt(2);
      hs = new HardState(term, r.nextBoolean() ? null : new NodeId(r.nextInt(1, 4)), m.last());
    }
    long term = Math.max(1, m.hardState.term());
    switch (r.nextInt(10)) {
      case 0 -> {
        if (m.last() > m.snapshot.index()) { // compact locally
          long at = r.nextLong(m.snapshot.index() + 1, m.last() + 1);
          Snapshot s = new Snapshot(at, m.entries.get(at).term(), THREE, new byte[] {1, 2});
          return ready(hs, List.of(), s, false);
        }
        return ready(hs, List.of(), null, false);
      }
      case 1 -> { // a snapshot from the leader, past our log
        long at = m.last() + r.nextInt(1, 20);
        return ready(hs, List.of(), new Snapshot(at, term, THREE, new byte[] {9}), true);
      }
      default -> { // entries, sometimes replacing a suffix
        long first = r.nextLong(Math.max(m.snapshot.index() + 1, m.last() - 5), m.last() + 2);
        return ready(hs, entries(first, first + r.nextInt(8), term), null, false);
      }
    }
  }

  /** The Ready contract, applied to plain maps. */
  private static final class Model {
    HardState hardState = HardState.EMPTY;
    Snapshot snapshot = Snapshot.initial(Membership.EMPTY);
    final TreeMap<Long, Entry> entries = new TreeMap<>();

    long last() {
      return entries.isEmpty() ? snapshot.index() : entries.lastKey();
    }

    void save(Ready r) {
      if (r.hardState() != null) {
        hardState = r.hardState();
      }
      if (r.snapshot() != null) {
        snapshot = r.snapshot();
        if (r.snapshotReplacesLog()) {
          entries.clear();
        } else {
          entries.headMap(snapshot.index(), true).clear();
        }
      }
      if (!r.entries().isEmpty()) {
        entries.tailMap(r.entries().getFirst().index(), true).clear();
        r.entries().forEach(e -> entries.put(e.index(), e));
      }
    }

    PersistedState load() {
      return new PersistedState(hardState, snapshot, List.copyOf(entries.values()));
    }
  }

  private static Ready ready(HardState hs, List<Entry> entries, Snapshot s, boolean replaces) {
    return new Ready(hs, entries, s, replaces, List.of(), List.of(), List.of());
  }

  private static List<Entry> entries(long from, long to, long term) {
    List<Entry> out = new ArrayList<>();
    for (long i = from; i <= to; i++) {
      out.add(new Entry(i, term, Entry.Type.NORMAL, new byte[] {(byte) i, (byte) term}));
    }
    return out;
  }

  private static LsmEngine open(SimFileSystem fs) {
    return LsmEngine.open(fs, new SimScheduler(new SimClock()), DIR, SMALL);
  }
}

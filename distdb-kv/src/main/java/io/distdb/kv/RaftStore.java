package io.distdb.kv;

import io.distdb.common.Bytes;
import io.distdb.raft.Entry;
import io.distdb.raft.HardState;
import io.distdb.raft.Membership;
import io.distdb.raft.PersistedState;
import io.distdb.raft.RaftCodec;
import io.distdb.raft.Ready;
import io.distdb.raft.Snapshot;
import io.distdb.storage.KvEngine;
import io.distdb.storage.KvIterator;
import io.distdb.storage.WriteBatch;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Keeps one Raft group's durable state in the node's storage engine, next to everything else the
 * node stores:
 *
 * <pre>
 *   0x01 'H' group             → hard state (term, vote, commit)
 *   0x01 'L' group index(BE)   → one log entry
 *   0x01 'P' group             → the latest snapshot
 * </pre>
 *
 * <p>{@link #save(Ready)} writes everything a {@link Ready} asks for in one synced, atomic batch,
 * which is what the Ready contract requires before any of its messages may be sent: after a crash,
 * either all of it is on disk or none of it is, and Phase 2's engine guarantees that a synced batch
 * survives.
 */
public final class RaftStore {

  private final KvEngine engine;
  private final byte[] group;

  public RaftStore(KvEngine engine, long groupId) {
    this.engine = Objects.requireNonNull(engine);
    this.group = KeySpace.uint64(groupId);
  }

  /** Persists what {@code r} asks to persist, durably; does nothing if it asks for nothing. */
  public void save(Ready r) {
    if (r.hardState() == null && r.entries().isEmpty() && r.snapshot() == null) {
      return;
    }
    WriteBatch batch = new WriteBatch();
    if (r.snapshot() != null) {
      Snapshot s = r.snapshot();
      batch.put(snapshotKey(), RaftCodec.encodeSnapshot(s));
      // From the leader: it replaces the whole log. From local compaction: it replaces the
      // entries it covers.
      byte[] end = r.snapshotReplacesLog() ? logEnd() : logKey(s.index() + 1);
      deleteRange(batch, logKey(0), end);
    }
    if (!r.entries().isEmpty()) {
      // New entries replace any stored at or after the first one (a conflicting suffix).
      deleteRange(batch, logKey(r.entries().getFirst().index()), logEnd());
      for (Entry e : r.entries()) {
        batch.put(logKey(e.index()), RaftCodec.encodeEntry(e));
      }
    }
    if (r.hardState() != null) {
      batch.put(hardStateKey(), RaftCodec.encodeHardState(r.hardState()));
    }
    // Later operations in a batch win, so an entry deleted above and written again stays.
    engine.write(batch, true);
  }

  /** What was stored, for restarting the node. */
  public PersistedState load() {
    HardState hs =
        engine.get(hardStateKey()).map(RaftCodec::decodeHardState).orElse(HardState.EMPTY);
    Snapshot snapshot =
        engine
            .get(snapshotKey())
            .map(RaftCodec::decodeSnapshot)
            .orElse(Snapshot.initial(Membership.EMPTY));
    List<Entry> entries = new ArrayList<>();
    try (KvIterator it = engine.scan(logKey(snapshot.index() + 1), logEnd())) {
      while (it.hasNext()) {
        entries.add(RaftCodec.decodeEntry(it.next().value()));
      }
    }
    return new PersistedState(hs, snapshot, entries);
  }

  /** The stored entry at {@code index}, if any (for tests and debugging). */
  public Optional<Entry> entry(long index) {
    return engine.get(logKey(index)).map(RaftCodec::decodeEntry);
  }

  private void deleteRange(WriteBatch batch, byte[] start, byte[] end) {
    try (KvIterator it = engine.scan(start, end)) {
      while (it.hasNext()) {
        batch.delete(it.next().key());
      }
    }
  }

  private byte[] hardStateKey() {
    return KeySpace.local(KeySpace.RAFT_HARD_STATE, group);
  }

  private byte[] snapshotKey() {
    return KeySpace.local(KeySpace.RAFT_SNAPSHOT, group);
  }

  private byte[] logKey(long index) {
    return KeySpace.local(KeySpace.RAFT_LOG, group, KeySpace.uint64(index));
  }

  private byte[] logEnd() {
    return Bytes.prefixEnd(KeySpace.local(KeySpace.RAFT_LOG, group)).orElseThrow();
  }
}

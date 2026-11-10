package io.distdb.storage;

import java.util.Optional;

/**
 * A consistent, point-in-time view of the engine: reads through it never see writes made after the
 * snapshot was taken. Raft uses one to stream a range to a lagging follower while writes continue.
 * It keeps old versions alive until closed, so close it promptly.
 */
public interface EngineSnapshot extends AutoCloseable {

  /** The value of {@code key} as of the snapshot. */
  Optional<byte[]> get(byte[] key);

  /** Entries in {@code [startInclusive, endExclusive)} as of the snapshot; a null end is open. */
  KvIterator scan(byte[] startInclusive, byte[] endExclusive);

  @Override
  void close();
}

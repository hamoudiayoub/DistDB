package io.distdb.sim;

import io.distdb.raft.Entry;
import io.distdb.raft.Snapshot;
import java.nio.ByteBuffer;

/**
 * The simulation's state machine: a running hash of every entry applied, in order. Two replicas
 * that applied the same entries in the same order have the same hash at every index, and a
 * difference anywhere shows up at once, which makes "every replica applies the same commands in the
 * same order" cheap to check. Its snapshot is just the index and the hash.
 */
final class HashChain {

  private long applied;
  private long hash;

  private HashChain(long applied, long hash) {
    this.applied = applied;
    this.hash = hash;
  }

  static HashChain fromSnapshot(Snapshot s) {
    if (s.data().length == 0) {
      return new HashChain(s.index(), 0);
    }
    ByteBuffer b = ByteBuffer.wrap(s.data());
    long index = b.getLong();
    if (index != s.index()) {
      throw new AssertionError("snapshot data for " + index + " labelled " + s.index());
    }
    return new HashChain(index, b.getLong());
  }

  void apply(Entry e) {
    if (e.index() != applied + 1) {
      throw new AssertionError("applied " + e + " right after " + applied + ": a gap or a repeat");
    }
    long h = hash ^ 0xcbf29ce484222325L;
    h = mix(h, e.index());
    h = mix(h, e.term());
    h = mix(h, e.type().ordinal());
    for (byte x : e.data()) {
      h = (h ^ (x & 0xFF)) * 0x100000001b3L;
    }
    hash = h;
    applied = e.index();
  }

  long applied() {
    return applied;
  }

  long hash() {
    return hash;
  }

  byte[] snapshotData() {
    return ByteBuffer.allocate(16).putLong(applied).putLong(hash).array();
  }

  private static long mix(long h, long v) {
    for (int i = 0; i < 8; i++) {
      h = (h ^ ((v >>> (8 * i)) & 0xFF)) * 0x100000001b3L;
    }
    return h;
  }
}

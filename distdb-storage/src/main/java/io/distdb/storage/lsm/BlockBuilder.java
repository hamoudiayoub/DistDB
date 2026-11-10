package io.distdb.storage.lsm;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds one SSTable block: sorted entries with prefix-compressed keys.
 *
 * <p>Entry format: {@code shared (varint) | unshared (varint) | valueLength (varint) | key bytes
 * after the shared prefix | value}. Every {@code restartInterval} entries the key is stored in full
 * (shared = 0) and its offset recorded as a restart point; the block ends with the restart offsets
 * (4 bytes each) and their count (4 bytes). A reader binary-searches the restart points and scans
 * forward from there, so prefix compression costs at most {@code restartInterval} steps.
 */
final class BlockBuilder {

  private final int restartInterval;
  private final ByteSink buf = new ByteSink(4096);
  private final List<Integer> restarts = new ArrayList<>();
  private byte[] lastKey = new byte[0];
  private int sinceRestart;
  private int entries;

  BlockBuilder(int restartInterval) {
    this.restartInterval = restartInterval;
    restarts.add(0);
  }

  void add(byte[] key, byte[] value) {
    int shared = 0;
    if (sinceRestart < restartInterval) {
      int max = Math.min(lastKey.length, key.length);
      while (shared < max && lastKey[shared] == key[shared]) {
        shared++;
      }
    } else {
      restarts.add(buf.size());
      sinceRestart = 0;
    }
    buf.writeVarint(shared);
    buf.writeVarint(key.length - shared);
    buf.writeVarint(value.length);
    buf.writeBytes(key, shared, key.length - shared);
    buf.writeBytes(value);
    lastKey = key;
    sinceRestart++;
    entries++;
  }

  boolean isEmpty() {
    return entries == 0;
  }

  int estimatedSize() {
    return buf.size() + 4 * restarts.size() + 4;
  }

  byte[] finish() {
    for (int r : restarts) {
      buf.writeInt(r);
    }
    buf.writeInt(restarts.size());
    return buf.toByteArray();
  }

  void reset() {
    buf.reset();
    restarts.clear();
    restarts.add(0);
    lastKey = new byte[0];
    sinceRestart = 0;
    entries = 0;
  }
}

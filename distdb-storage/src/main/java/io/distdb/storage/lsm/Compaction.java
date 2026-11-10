package io.distdb.storage.lsm;

import java.util.ArrayList;
import java.util.List;

/**
 * One unit of compaction work: merge {@code inputs} at {@code level} with the overlapping {@code
 * nextLevelInputs} at {@code level + 1}, writing the result to {@code level + 1}.
 */
record Compaction(int level, List<FileMetadata> inputs, List<FileMetadata> nextLevelInputs) {

  int outputLevel() {
    return level + 1;
  }

  /** A single file with nothing to merge against can simply move down a level. */
  boolean isTrivialMove() {
    return inputs.size() == 1 && nextLevelInputs.isEmpty();
  }

  List<FileMetadata> allInputs() {
    List<FileMetadata> all = new ArrayList<>(inputs);
    all.addAll(nextLevelInputs);
    return all;
  }

  /**
   * Chooses the most urgent compaction, LevelDB style: each level gets a score (level 0: file count
   * over the trigger; deeper levels: bytes over their size budget) and the highest score at or
   * above 1 wins. Returns null if no level needs work.
   *
   * @param pointers per level, the largest user key of the last file compacted there, so files are
   *     picked round-robin through the key space
   */
  static Compaction pick(Version v, EngineOptions options, byte[][] pointers) {
    int bestLevel = -1;
    double bestScore = 1.0;
    for (int level = 0; level < Version.NUM_LEVELS - 1; level++) {
      double score =
          level == 0
              ? (double) v.numFiles(0) / options.l0CompactionTrigger()
              : (double) v.levelBytes(level) / maxBytesForLevel(options, level);
      if (score >= bestScore) {
        bestScore = score;
        bestLevel = level;
      }
    }
    if (bestLevel < 0) {
      return null;
    }
    List<FileMetadata> inputs;
    if (bestLevel == 0) {
      // All of level 0: its files overlap, and moving only some of them down could put an older
      // version of a key above a newer one.
      inputs = v.files(0);
    } else {
      inputs = List.of(roundRobin(v.files(bestLevel), pointers[bestLevel]));
    }
    byte[] lo = null;
    byte[] hi = null;
    for (FileMetadata f : inputs) {
      byte[] s = f.smallestUserKey();
      byte[] l = f.largestUserKey();
      if (lo == null || io.distdb.common.Bytes.compare(s, lo) < 0) {
        lo = s;
      }
      if (hi == null || io.distdb.common.Bytes.compare(l, hi) > 0) {
        hi = l;
      }
    }
    return new Compaction(bestLevel, inputs, v.overlapping(bestLevel + 1, lo, hi));
  }

  static boolean needed(Version v, EngineOptions options) {
    return pick(v, options, new byte[Version.NUM_LEVELS][]) != null;
  }

  static double maxBytesForLevel(EngineOptions options, int level) {
    double bytes = options.levelBaseBytes();
    for (int i = 1; i < level; i++) {
      bytes *= options.levelMultiplier();
    }
    return bytes;
  }

  private static FileMetadata roundRobin(List<FileMetadata> files, byte[] pointer) {
    if (pointer != null) {
      for (FileMetadata f : files) {
        if (InternalKey.compareUserKey(f.largest(), pointer) > 0) {
          return f;
        }
      }
    }
    return files.get(0);
  }
}

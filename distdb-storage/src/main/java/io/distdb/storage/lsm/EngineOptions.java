package io.distdb.storage.lsm;

/**
 * Tuning knobs of the LSM engine. Production uses {@link #defaults()}; tests shrink everything so
 * that a few thousand operations exercise many flushes and compactions.
 *
 * @param memtableBytes the memtable is frozen and flushed once it holds about this many bytes
 * @param blockBytes target size of an SSTable data block before it is cut
 * @param bloomBitsPerKey bloom filter size; 10 bits per key gives about 1% false positives
 * @param targetFileBytes compaction starts a new output SSTable after about this many bytes
 * @param l0CompactionTrigger level 0 is compacted once it has this many files
 * @param l0StopTrigger writes stall (and compact inline) once level 0 has this many files
 * @param levelBaseBytes maximum size of level 1; each deeper level may be {@code levelMultiplier}
 *     times larger
 * @param levelMultiplier size ratio between adjacent levels
 * @param groupCommit concurrent writers share one log write and one fsync (see {@link
 *     LsmEngine#write})
 */
public record EngineOptions(
    int memtableBytes,
    int blockBytes,
    int bloomBitsPerKey,
    long targetFileBytes,
    int l0CompactionTrigger,
    int l0StopTrigger,
    long levelBaseBytes,
    int levelMultiplier,
    boolean groupCommit) {

  /** Options with group commit on. */
  public EngineOptions(
      int memtableBytes,
      int blockBytes,
      int bloomBitsPerKey,
      long targetFileBytes,
      int l0CompactionTrigger,
      int l0StopTrigger,
      long levelBaseBytes,
      int levelMultiplier) {
    this(
        memtableBytes,
        blockBytes,
        bloomBitsPerKey,
        targetFileBytes,
        l0CompactionTrigger,
        l0StopTrigger,
        levelBaseBytes,
        levelMultiplier,
        true);
  }

  public EngineOptions {
    if (memtableBytes <= 0
        || blockBytes <= 0
        || bloomBitsPerKey <= 0
        || targetFileBytes <= 0
        || l0CompactionTrigger <= 0
        || levelBaseBytes <= 0
        || levelMultiplier < 2) {
      throw new IllegalArgumentException("invalid engine options");
    }
    if (l0StopTrigger < l0CompactionTrigger) {
      throw new IllegalArgumentException("l0StopTrigger must be >= l0CompactionTrigger");
    }
  }

  /** Defaults close to LevelDB's. */
  public static EngineOptions defaults() {
    return new EngineOptions(4 << 20, 4096, 10, 2 << 20, 4, 12, 10L << 20, 10);
  }

  /** A copy with group commit on or off (off: every synced write pays its own fsync). */
  public EngineOptions withGroupCommit(boolean on) {
    return new EngineOptions(
        memtableBytes,
        blockBytes,
        bloomBitsPerKey,
        targetFileBytes,
        l0CompactionTrigger,
        l0StopTrigger,
        levelBaseBytes,
        levelMultiplier,
        on);
  }
}

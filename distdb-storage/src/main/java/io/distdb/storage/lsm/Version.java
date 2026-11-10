package io.distdb.storage.lsm;

import io.distdb.storage.CorruptionException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An immutable snapshot of which SSTables are live and at which level.
 *
 * <p>Invariants:
 *
 * <ul>
 *   <li>Level 0 files come straight from memtable flushes and may overlap; they are kept newest
 *       first.
 *   <li>Files in levels 1 and deeper never overlap within a level and are kept sorted by key.
 *   <li>For any user key, newer versions live in shallower levels. Point lookups rely on this to
 *       stop at the first hit.
 * </ul>
 *
 * <p>Readers reference-count the version they use, so a compaction can replace files without
 * deleting them out from under an open iterator.
 */
final class Version {

  static final int NUM_LEVELS = 7;

  private static final Comparator<FileMetadata> NEWEST_FIRST =
      Comparator.comparingLong(FileMetadata::number).reversed();
  private static final Comparator<FileMetadata> BY_SMALLEST_KEY =
      (a, b) -> InternalKey.COMPARATOR.compare(a.smallest(), b.smallest());

  private final List<List<FileMetadata>> levels;
  private final AtomicInteger refs = new AtomicInteger();

  private Version(List<List<FileMetadata>> levels) {
    this.levels = levels;
  }

  static Version empty() {
    List<List<FileMetadata>> levels = new ArrayList<>();
    for (int i = 0; i < NUM_LEVELS; i++) {
      levels.add(List.of());
    }
    return new Version(List.copyOf(levels));
  }

  /** A new version with {@code edit}'s files added and removed; checks the level invariants. */
  Version apply(VersionEdit edit) {
    List<List<FileMetadata>> next = new ArrayList<>();
    for (List<FileMetadata> level : levels) {
      next.add(new ArrayList<>(level));
    }
    for (VersionEdit.DeletedFile d : edit.deleted()) {
      boolean removed = next.get(d.level()).removeIf(f -> f.number() == d.number());
      if (!removed) {
        throw new CorruptionException("edit deletes missing file " + d);
      }
    }
    for (VersionEdit.AddedFile a : edit.added()) {
      next.get(a.level()).add(a.file());
    }
    next.get(0).sort(NEWEST_FIRST);
    for (int level = 1; level < NUM_LEVELS; level++) {
      List<FileMetadata> files = next.get(level);
      files.sort(BY_SMALLEST_KEY);
      for (int i = 1; i < files.size(); i++) {
        FileMetadata prev = files.get(i - 1);
        if (InternalKey.compareUserKey(prev.largest(), files.get(i).smallestUserKey()) >= 0) {
          throw new CorruptionException(
              "overlapping files at level " + level + ": " + prev + " and " + files.get(i));
        }
      }
    }
    List<List<FileMetadata>> frozen = new ArrayList<>();
    for (List<FileMetadata> level : next) {
      frozen.add(List.copyOf(level));
    }
    return new Version(List.copyOf(frozen));
  }

  List<FileMetadata> files(int level) {
    return levels.get(level);
  }

  int numFiles(int level) {
    return levels.get(level).size();
  }

  long levelBytes(int level) {
    long sum = 0;
    for (FileMetadata f : levels.get(level)) {
      sum += f.size();
    }
    return sum;
  }

  Set<Long> fileNumbers() {
    Set<Long> out = new HashSet<>();
    for (List<FileMetadata> level : levels) {
      for (FileMetadata f : level) {
        out.add(f.number());
      }
    }
    return out;
  }

  /**
   * The newest entry for {@code userKey} at or below {@code sequence}, searching shallow to deep.
   */
  Hit get(byte[] userKey, long sequence, TableCache tables) {
    for (FileMetadata f : levels.get(0)) {
      if (f.containsUserKey(userKey)) {
        Hit h = tables.get(f.number()).get(userKey, sequence);
        if (h != null) {
          return h;
        }
      }
    }
    for (int level = 1; level < NUM_LEVELS; level++) {
      FileMetadata f = findFile(levels.get(level), userKey);
      if (f != null && f.containsUserKey(userKey)) {
        Hit h = tables.get(f.number()).get(userKey, sequence);
        if (h != null) {
          return h;
        }
      }
    }
    return null;
  }

  /** In a sorted, non-overlapping level: the first file whose largest key is >= userKey. */
  private static FileMetadata findFile(List<FileMetadata> files, byte[] userKey) {
    int lo = 0;
    int hi = files.size();
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (InternalKey.compareUserKey(files.get(mid).largest(), userKey) < 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo < files.size() ? files.get(lo) : null;
  }

  /** Adds an iterator for every file that may hold keys in {@code [start, end)}. */
  void addIterators(List<InternalIterator> out, TableCache tables, byte[] start, byte[] end) {
    for (List<FileMetadata> level : levels) {
      for (FileMetadata f : level) {
        if (f.overlapsHalfOpen(start, end)) {
          out.add(tables.get(f.number()).iterator());
        }
      }
    }
  }

  /** Files at {@code level} whose user-key range intersects {@code [lo, hi]}. */
  List<FileMetadata> overlapping(int level, byte[] lo, byte[] hi) {
    List<FileMetadata> out = new ArrayList<>();
    for (FileMetadata f : levels.get(level)) {
      if (f.overlapsInclusive(lo, hi)) {
        out.add(f);
      }
    }
    return out;
  }

  /**
   * Whether no level deeper than {@code level} can hold {@code userKey}. Only then may a compaction
   * into {@code level} drop a tombstone for it: there is nothing older left for it to hide.
   */
  boolean isBaseLevelForKey(byte[] userKey, int level) {
    for (int l = level + 1; l < NUM_LEVELS; l++) {
      for (FileMetadata f : levels.get(l)) {
        if (f.containsUserKey(userKey)) {
          return false;
        }
      }
    }
    return true;
  }

  void ref() {
    refs.incrementAndGet();
  }

  /** Drops a reference; returns true when it was the last one. */
  boolean unref() {
    int n = refs.decrementAndGet();
    if (n < 0) {
      throw new IllegalStateException("version released too many times");
    }
    return n == 0;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Version[");
    for (int i = 0; i < NUM_LEVELS; i++) {
      if (!levels.get(i).isEmpty()) {
        sb.append(" L").append(i).append('=').append(levels.get(i));
      }
    }
    return sb.append(" ]").toString();
  }
}

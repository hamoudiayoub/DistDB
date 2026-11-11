package io.distdb.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * A node's log: a snapshot covering indexes up to {@code snapshot.index()}, followed by entries.
 *
 * <pre>
 *   [ snapshot ......... ] [ entries ............................. ]
 *                       ^snapshotIndex     ^committed    ^stable    ^lastIndex
 * </pre>
 *
 * <ul>
 *   <li>{@code committed}: known to be stored on a majority; never truncated.
 *   <li>{@code stable}: entries up to here are on this node's disk. Entries after it are handed to
 *       the host in the next {@link Ready}.
 * </ul>
 */
final class RaftLog {

  private Snapshot snapshot;
  private final ArrayList<Entry> entries = new ArrayList<>();
  private long committed;
  private long stable;

  RaftLog(Snapshot snapshot, List<Entry> stored, long committed) {
    this.snapshot = snapshot;
    this.entries.addAll(stored);
    this.stable = lastIndex();
    this.committed = Math.max(snapshot.index(), Math.min(committed, lastIndex()));
  }

  Snapshot snapshot() {
    return snapshot;
  }

  long snapshotIndex() {
    return snapshot.index();
  }

  long firstIndex() {
    return snapshot.index() + 1;
  }

  long lastIndex() {
    return snapshot.index() + entries.size();
  }

  long lastTerm() {
    return term(lastIndex());
  }

  long committed() {
    return committed;
  }

  long stable() {
    return stable;
  }

  /** The term of entry {@code i}, or -1 if it is compacted away or beyond the end. */
  long term(long i) {
    if (i == snapshot.index()) {
      return snapshot.term();
    }
    if (i < snapshot.index() || i > lastIndex()) {
      return -1;
    }
    return entries.get(offset(i)).term();
  }

  boolean matchTerm(long i, long term) {
    long t = term(i);
    return t >= 0 && t == term;
  }

  Entry entry(long i) {
    return entries.get(offset(i));
  }

  /** Entries {@code from..to} (inclusive), at most {@code max} of them. */
  List<Entry> slice(long from, long to, int max) {
    if (from < firstIndex() || to > lastIndex()) {
      throw new IllegalArgumentException(
          "slice " + from + ".." + to + " outside " + firstIndex() + ".." + lastIndex());
    }
    if (from > to) {
      return List.of();
    }
    long end = Math.min(to, from + max - 1);
    return List.copyOf(entries.subList(offset(from), offset(end) + 1));
  }

  /**
   * Whether a log ending at {@code (lastIndex, lastTerm)} is at least as up to date as this one.
   */
  boolean isUpToDate(long lastIndex, long lastTerm) {
    return lastTerm > lastTerm() || (lastTerm == lastTerm() && lastIndex >= lastIndex());
  }

  /** Appends {@code e} at the end (the leader's path). */
  void append(Entry e) {
    if (e.index() != lastIndex() + 1) {
      throw new IllegalArgumentException("append " + e + " after " + lastIndex());
    }
    entries.add(e);
  }

  /**
   * The index of the first entry in {@code incoming} that this log lacks or holds with a different
   * term, or 0 if it already has all of them.
   */
  long findConflict(List<Entry> incoming) {
    for (Entry e : incoming) {
      if (!matchTerm(e.index(), e.term())) {
        return e.index();
      }
    }
    return 0;
  }

  /**
   * Replaces everything from {@code incoming.get(0).index()} on with {@code incoming}.
   *
   * @throws IllegalStateException if that would remove a committed entry: Raft guarantees it never
   *     happens, so it means a bug (or a disk that lost synced data)
   */
  void truncateAndAppend(List<Entry> incoming) {
    long first = incoming.getFirst().index();
    if (first <= committed) {
      throw new IllegalStateException(
          "would overwrite committed entry " + first + " (committed " + committed + ")");
    }
    if (first > lastIndex() + 1 || first <= snapshot.index()) {
      throw new IllegalArgumentException("cannot append at " + first + " after " + lastIndex());
    }
    if (first <= lastIndex()) {
      entries.subList(offset(first), entries.size()).clear();
      stable = Math.min(stable, first - 1);
    }
    entries.addAll(incoming);
  }

  void commitTo(long index) {
    if (index > committed) {
      if (index > lastIndex()) {
        throw new IllegalStateException("commit " + index + " beyond last index " + lastIndex());
      }
      committed = index;
    }
  }

  /** Entries the host has not yet been asked to store. */
  List<Entry> unstable() {
    if (stable >= lastIndex()) {
      return List.of();
    }
    return List.copyOf(entries.subList(offset(Math.max(stable + 1, firstIndex())), entries.size()));
  }

  /** The host stored entries up to {@code (index, term)}; ignored if the log changed since. */
  void stableTo(long index, long term) {
    if (index > stable && matchTerm(index, term)) {
      stable = index;
    }
  }

  /** Replaces entries up to {@code index} with a snapshot; {@code index} must be committed. */
  void compact(long index, byte[] data, Membership membership) {
    if (index <= snapshot.index() || index > committed) {
      throw new IllegalArgumentException(
          "cannot compact to "
              + index
              + " (snapshot "
              + snapshot.index()
              + ", committed "
              + committed
              + ")");
    }
    Snapshot s = new Snapshot(index, term(index), membership, data);
    entries.subList(0, offset(index) + 1).clear();
    snapshot = s;
    stable = Math.max(stable, index);
  }

  /** Discards the whole log in favour of a snapshot from the leader. */
  void restore(Snapshot s) {
    entries.clear();
    snapshot = s;
    committed = s.index();
    stable = s.index();
  }

  /** The first index of the run of {@code term} that contains index {@code at}. */
  long firstIndexOfTerm(long term, long at) {
    long i = at;
    while (i - 1 > snapshot.index() && term(i - 1) == term) {
      i--;
    }
    return i;
  }

  /** The last index whose term is {@code term}, or -1 if the log has none. */
  long lastIndexOfTerm(long term) {
    for (long i = lastIndex(); i > snapshot.index(); i--) {
      long t = term(i);
      if (t == term) {
        return i;
      }
      if (t < term) {
        return -1; // terms only grow along the log
      }
    }
    return snapshot.term() == term && snapshot.index() > 0 ? snapshot.index() : -1;
  }

  private int offset(long i) {
    return (int) (i - firstIndex());
  }
}

package io.distdb.raft;

import java.util.List;
import java.util.Objects;

/**
 * What a host stored for a node: everything needed to restart it after a crash.
 *
 * @param hardState the last stored term, vote and commit index
 * @param snapshot the last stored snapshot ({@link Snapshot#initial} if none)
 * @param entries the stored entries after the snapshot, contiguous and in order
 */
public record PersistedState(HardState hardState, Snapshot snapshot, List<Entry> entries) {

  public PersistedState {
    Objects.requireNonNull(hardState);
    Objects.requireNonNull(snapshot);
    entries = List.copyOf(entries);
    long expected = snapshot.index() + 1;
    for (Entry e : entries) {
      if (e.index() != expected++) {
        throw new IllegalArgumentException("stored entries are not contiguous at " + e);
      }
    }
  }

  /** A node that has stored nothing: it waits to be added to a group. */
  public static PersistedState empty() {
    return new PersistedState(HardState.EMPTY, Snapshot.initial(Membership.EMPTY), List.of());
  }
}

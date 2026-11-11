package io.distdb.raft;

import java.util.List;

/**
 * Everything the host must do for a {@link RaftNode}, in this order, before calling {@link
 * RaftNode#advance(Ready)}:
 *
 * <ol>
 *   <li><b>Persist</b>, durably (fsync) and atomically: {@link #hardState} if not null; {@link
 *       #snapshot} if not null; and {@link #entries}, first deleting any stored entries at or after
 *       the first one's index (they are being replaced). If {@link #snapshotReplacesLog} is true,
 *       delete every stored entry; otherwise delete stored entries up to the snapshot's index.
 *   <li><b>Send</b> {@link #messages}.
 *   <li><b>Apply</b>: if {@link #snapshotReplacesLog}, reset the state machine from {@link
 *       #snapshot}; then apply {@link #committedEntries} in order; then answer {@link #readStates}
 *       once the state machine has applied each one's index.
 * </ol>
 *
 * <p>Why persist before sending: a vote, or an acknowledgement of entries, is a promise. If a node
 * promised and then crashed before writing it down, after the restart it could vote again in the
 * same term, or deny holding entries a leader has already counted as committed.
 *
 * @param hardState term, vote and commit index, or null if unchanged
 * @param entries new entries to store, possibly replacing a stored suffix
 * @param snapshot a snapshot to store, or null
 * @param snapshotReplacesLog true if the snapshot came from the leader and replaces the whole log
 *     and the state machine; false if this node made it by compacting its own log
 * @param committedEntries committed entries to apply, in order
 * @param messages messages to send
 * @param readStates ReadIndex requests whose leadership check has passed
 */
public record Ready(
    HardState hardState,
    List<Entry> entries,
    Snapshot snapshot,
    boolean snapshotReplacesLog,
    List<Entry> committedEntries,
    List<Message> messages,
    List<ReadState> readStates) {

  public Ready {
    entries = List.copyOf(entries);
    committedEntries = List.copyOf(committedEntries);
    messages = List.copyOf(messages);
    readStates = List.copyOf(readStates);
  }

  /**
   * A linearizable read is safe once the state machine has applied {@link #index}: the leader
   * confirmed, after the read arrived, that it was still the leader, and {@code index} was its
   * commit index when the read arrived.
   *
   * @param index apply at least this far before reading
   * @param context what the host passed to {@link RaftNode#readIndex}, to match the answer up
   */
  public record ReadState(long index, byte[] context) {}
}

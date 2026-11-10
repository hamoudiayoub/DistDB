package io.distdb.storage.lsm;

import io.distdb.common.Bytes;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import java.util.NoSuchElementException;

/**
 * Turns a merged stream of internal entries into what a user sees at one sequence number: for each
 * user key, the newest version at or below the snapshot; tombstones hide the key; newer versions
 * are invisible.
 */
final class DbIterator implements KvIterator {

  private final InternalIterator merged;
  private final long snapshot;
  private final byte[] end;
  private final Runnable onClose;
  private KvEntry pending;
  private boolean closed;

  DbIterator(InternalIterator merged, long snapshot, byte[] start, byte[] end, Runnable onClose) {
    this.merged = merged;
    this.snapshot = snapshot;
    this.end = end;
    this.onClose = onClose;
    merged.seek(InternalKey.seekKey(start, snapshot));
    advance();
  }

  @Override
  public boolean hasNext() {
    return pending != null;
  }

  @Override
  public KvEntry next() {
    if (pending == null) {
      throw new NoSuchElementException();
    }
    KvEntry e = pending;
    advance();
    return e;
  }

  private void advance() {
    pending = null;
    while (merged.valid()) {
      byte[] ik = merged.key();
      if (end != null && InternalKey.compareUserKey(ik, end) >= 0) {
        return;
      }
      if (InternalKey.sequence(ik) > snapshot) {
        merged.next();
        continue;
      }
      // ik is the newest visible version of its user key.
      KvEntry candidate =
          InternalKey.kind(ik) == InternalKey.PUT
              ? new KvEntry(InternalKey.userKey(ik), merged.value().clone())
              : null;
      byte[] userKeyIk = ik;
      do {
        merged.next();
      } while (merged.valid() && InternalKey.sameUserKey(merged.key(), userKeyIk));
      if (candidate != null) {
        pending = candidate;
        return;
      }
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      merged.close();
      onClose.run();
    }
  }

  @Override
  public String toString() {
    return "DbIterator(snapshot="
        + snapshot
        + ", end="
        + (end == null ? "-" : Bytes.toHex(end))
        + ")";
  }
}

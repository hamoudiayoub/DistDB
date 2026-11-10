package io.distdb.storage.mvcc;

import io.distdb.common.Bytes;
import io.distdb.common.encoding.KeyReader;
import io.distdb.common.encoding.KeyWriter;
import io.distdb.common.encoding.Order;
import io.distdb.storage.KvEngine;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import java.util.Optional;

/**
 * The multi-version key format: {@code userKey@timestamp}, with all versions of a key stored
 * together, newest first.
 *
 * <p>Encoding: the user key as an escaped, terminated byte string, then the timestamp as a
 * descending 8-byte integer (both from {@link KeyWriter}). Because the byte-string encoding is
 * prefix-free, every version of {@code k} starts with {@link #prefix(byte[]) prefix(k)} and no
 * version of any other key does. Because the timestamp sorts descending, seeking to {@code k@t}
 * lands on the newest version at or before {@code t}: exactly what a snapshot read at {@code t}
 * needs.
 *
 * <p>Phase 7's transaction layer builds its data and write columns on this format.
 */
public final class MvccKey {

  /** One decoded version. */
  public record Version(byte[] userKey, long timestamp, byte[] value) {}

  private MvccKey() {}

  /** {@code userKey@timestamp}. Timestamps are non-negative. */
  public static byte[] encode(byte[] userKey, long timestamp) {
    if (timestamp < 0) {
      throw new IllegalArgumentException("timestamp must be non-negative: " + timestamp);
    }
    return new KeyWriter(userKey.length + 12)
        .writeBytes(userKey)
        .writeLong(timestamp, Order.DESC)
        .toByteArray();
  }

  /** The bytes every version of {@code userKey} starts with. */
  public static byte[] prefix(byte[] userKey) {
    return new KeyWriter(userKey.length + 3).writeBytes(userKey).toByteArray();
  }

  public static byte[] userKeyOf(byte[] mvccKey) {
    return new KeyReader(mvccKey).readBytes();
  }

  public static long timestampOf(byte[] mvccKey) {
    KeyReader r = new KeyReader(mvccKey);
    r.readBytes();
    return r.readLong();
  }

  /** The newest version of {@code userKey} with timestamp at most {@code timestamp}, if any. */
  public static Optional<Version> readAt(KvEngine engine, byte[] userKey, long timestamp) {
    byte[] start = encode(userKey, timestamp);
    byte[] end = Bytes.prefixEnd(prefix(userKey)).orElse(null);
    try (KvIterator it = engine.scan(start, end)) {
      if (!it.hasNext()) {
        return Optional.empty();
      }
      KvEntry e = it.next();
      return Optional.of(new Version(userKey.clone(), timestampOf(e.key()), e.value()));
    }
  }
}

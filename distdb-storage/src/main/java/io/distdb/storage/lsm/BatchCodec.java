package io.distdb.storage.lsm;

import io.distdb.storage.CorruptionException;
import io.distdb.storage.WriteBatch;

/**
 * Encodes a {@link WriteBatch} as one write-ahead log record.
 *
 * <p>Format: {@code firstSequence (8) | count (4)}, then per operation {@code kind (1) | key
 * (length-prefixed) | value (length-prefixed, puts only)}. Operation {@code i} gets sequence number
 * {@code firstSequence + i}.
 */
final class BatchCodec {

  private BatchCodec() {}

  static byte[] encode(WriteBatch batch, long firstSequence) {
    ByteSink out = new ByteSink((int) Math.min(Integer.MAX_VALUE, batch.approximateBytes() + 16));
    out.writeLong(firstSequence);
    out.writeInt(batch.count());
    batch.forEach(
        new WriteBatch.Visitor() {
          @Override
          public void put(byte[] key, byte[] value) {
            out.writeByte(InternalKey.PUT);
            out.writeLengthPrefixed(key);
            out.writeLengthPrefixed(value);
          }

          @Override
          public void delete(byte[] key) {
            out.writeByte(InternalKey.DELETE);
            out.writeLengthPrefixed(key);
          }
        });
    return out.toByteArray();
  }

  /** Inserts {@code batch} into {@code memtable} starting at {@code firstSequence}. */
  static void apply(WriteBatch batch, long firstSequence, Memtable memtable) {
    long[] seq = {firstSequence};
    batch.forEach(
        new WriteBatch.Visitor() {
          @Override
          public void put(byte[] key, byte[] value) {
            memtable.add(seq[0]++, InternalKey.PUT, key, value);
          }

          @Override
          public void delete(byte[] key) {
            memtable.add(seq[0]++, InternalKey.DELETE, key, null);
          }
        });
  }

  /**
   * Replays a log record into {@code memtable}.
   *
   * @return the last sequence number the record used
   */
  static long replay(byte[] record, Memtable memtable) {
    ByteSource in = new ByteSource(record);
    long seq = in.readLong();
    int count = in.readInt();
    if (count <= 0) {
      throw new CorruptionException("batch with " + count + " operations");
    }
    for (int i = 0; i < count; i++) {
      int kind = in.readByte();
      byte[] key = in.readLengthPrefixed();
      switch (kind) {
        case InternalKey.PUT ->
            memtable.add(seq + i, InternalKey.PUT, key, in.readLengthPrefixed());
        case InternalKey.DELETE -> memtable.add(seq + i, InternalKey.DELETE, key, null);
        default -> throw new CorruptionException("unknown operation kind " + kind);
      }
    }
    if (in.hasRemaining()) {
      throw new CorruptionException("trailing bytes after batch");
    }
    return seq + count - 1;
  }
}

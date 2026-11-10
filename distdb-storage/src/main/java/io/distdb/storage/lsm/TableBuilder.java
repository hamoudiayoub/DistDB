package io.distdb.storage.lsm;

import io.distdb.env.FileHandle;
import io.distdb.storage.lsm.TableFormat.BlockHandle;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Writes one SSTable (see {@link TableFormat}). Entries must be added in strictly increasing
 * internal-key order. The caller syncs and closes the file after {@link #finish()}.
 */
final class TableBuilder {

  private final FileHandle file;
  private final EngineOptions options;
  private final BlockBuilder data = new BlockBuilder(TableFormat.DATA_RESTART_INTERVAL);
  private final BlockBuilder index = new BlockBuilder(TableFormat.INDEX_RESTART_INTERVAL);
  private long[] keyHashes = new long[256];
  private long offset;
  private long entries;
  private byte[] smallest;
  private byte[] largest;
  private boolean finished;

  TableBuilder(FileHandle file, EngineOptions options) {
    this.file = file;
    this.options = options;
  }

  void add(byte[] internalKey, byte[] value) throws IOException {
    if (finished) {
      throw new IllegalStateException("table already finished");
    }
    if (largest != null && InternalKey.COMPARATOR.compare(largest, internalKey) >= 0) {
      throw new IllegalArgumentException("keys must be added in increasing order");
    }
    if (smallest == null) {
      smallest = internalKey;
    }
    largest = internalKey;
    data.add(internalKey, value);
    if (entries == keyHashes.length) {
      keyHashes = Arrays.copyOf(keyHashes, keyHashes.length * 2);
    }
    keyHashes[(int) entries] =
        BloomFilter.hash(internalKey, 0, internalKey.length - InternalKey.TRAILER_BYTES);
    entries++;
    if (data.estimatedSize() >= options.blockBytes()) {
      flushDataBlock();
    }
  }

  /** Bytes written so far plus the pending block; used to decide when to cut a new file. */
  long estimatedSize() {
    return offset + data.estimatedSize();
  }

  long entries() {
    return entries;
  }

  byte[] smallest() {
    return smallest;
  }

  byte[] largest() {
    return largest;
  }

  /**
   * Writes the remaining data, the filter, the index and the footer.
   *
   * @return the final file size
   */
  long finish() throws IOException {
    flushDataBlock();
    BlockHandle filter =
        writeBlock(BloomFilter.build(keyHashes, (int) entries, options.bloomBitsPerKey()));
    BlockHandle idx = writeBlock(index.finish());
    ByteSink footer = new ByteSink(TableFormat.FOOTER_BYTES);
    footer.writeLong(idx.offset());
    footer.writeInt(idx.size());
    footer.writeLong(filter.offset());
    footer.writeInt(filter.size());
    footer.writeLong(TableFormat.MAGIC);
    byte[] f = footer.toByteArray();
    footer.writeInt(Checksums.crc32c(f, 0, f.length));
    byte[] full = footer.toByteArray();
    file.write(ByteBuffer.wrap(full), offset);
    offset += full.length;
    finished = true;
    return offset;
  }

  private void flushDataBlock() throws IOException {
    if (data.isEmpty()) {
      return;
    }
    BlockHandle handle = writeBlock(data.finish());
    index.add(largest, handle.encode());
    data.reset();
  }

  private BlockHandle writeBlock(byte[] contents) throws IOException {
    byte[] withCrc = Arrays.copyOf(contents, contents.length + 4);
    int crc = Checksums.crc32c(contents, 0, contents.length);
    withCrc[contents.length] = (byte) (crc >>> 24);
    withCrc[contents.length + 1] = (byte) (crc >>> 16);
    withCrc[contents.length + 2] = (byte) (crc >>> 8);
    withCrc[contents.length + 3] = (byte) crc;
    file.write(ByteBuffer.wrap(withCrc), offset);
    BlockHandle handle = new BlockHandle(offset, contents.length);
    offset += withCrc.length;
    return handle;
  }
}

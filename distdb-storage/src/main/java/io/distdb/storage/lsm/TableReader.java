package io.distdb.storage.lsm;

import io.distdb.env.FileHandle;
import io.distdb.storage.CorruptionException;
import io.distdb.storage.StorageException;
import io.distdb.storage.lsm.TableFormat.BlockHandle;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/**
 * Reads one SSTable. The index and bloom filter are loaded once and kept in memory; data blocks are
 * read (and checksum-verified) on demand. Thread-safe: files are immutable and reads are
 * positional.
 */
final class TableReader implements Closeable {

  private final FileHandle file;
  private final long number;
  private final Block index;
  private final byte[] filter;

  private TableReader(FileHandle file, long number, Block index, byte[] filter) {
    this.file = file;
    this.number = number;
    this.index = index;
    this.filter = filter;
  }

  static TableReader open(FileHandle file, long number) throws IOException {
    long size = file.size();
    if (size < TableFormat.FOOTER_BYTES) {
      throw new CorruptionException("table " + number + " too short: " + size + " bytes");
    }
    ByteBuffer footer = ByteBuffer.allocate(TableFormat.FOOTER_BYTES);
    file.readFully(footer, size - TableFormat.FOOTER_BYTES);
    byte[] f = footer.array();
    int crc = ByteSource.readInt(f, 32);
    if (Checksums.crc32c(f, 0, 32) != crc) {
      throw new CorruptionException("table " + number + ": footer checksum mismatch");
    }
    ByteSource in = new ByteSource(f);
    BlockHandle indexHandle = new BlockHandle(in.readLong(), in.readInt());
    BlockHandle filterHandle = new BlockHandle(in.readLong(), in.readInt());
    if (in.readLong() != TableFormat.MAGIC) {
      throw new CorruptionException("table " + number + ": bad magic number");
    }
    Block index = new Block(readBlock(file, number, indexHandle));
    byte[] filter = readBlock(file, number, filterHandle);
    return new TableReader(file, number, index, filter);
  }

  long number() {
    return number;
  }

  /** The newest entry for {@code userKey} visible at {@code sequence}, or null if none. */
  Hit get(byte[] userKey, long sequence) {
    if (!BloomFilter.mayContain(filter, BloomFilter.hash(userKey, 0, userKey.length))) {
      return null;
    }
    InternalIterator it = iterator();
    it.seek(InternalKey.seekKey(userKey, sequence));
    if (!it.valid() || InternalKey.compareUserKey(it.key(), userKey) != 0) {
      return null;
    }
    return InternalKey.kind(it.key()) == InternalKey.PUT ? new Hit(it.value()) : Hit.DELETED;
  }

  /**
   * A two-level iterator: the in-memory index picks the block, the block iterator does the rest.
   */
  InternalIterator iterator() {
    return new InternalIterator() {
      private final InternalIterator indexIt = index.iterator(InternalKey.COMPARATOR);
      private InternalIterator dataIt;

      @Override
      public boolean valid() {
        return dataIt != null && dataIt.valid();
      }

      @Override
      public void seekToFirst() {
        indexIt.seekToFirst();
        loadBlock();
        if (dataIt != null) {
          dataIt.seekToFirst();
        }
        skipEmptyBlocks();
      }

      @Override
      public void seek(byte[] target) {
        indexIt.seek(target);
        loadBlock();
        if (dataIt != null) {
          dataIt.seek(target);
        }
        skipEmptyBlocks();
      }

      @Override
      public void next() {
        dataIt.next();
        skipEmptyBlocks();
      }

      @Override
      public byte[] key() {
        return dataIt.key();
      }

      @Override
      public byte[] value() {
        return dataIt.value();
      }

      private void loadBlock() {
        if (!indexIt.valid()) {
          dataIt = null;
          return;
        }
        BlockHandle h = BlockHandle.decode(indexIt.value());
        dataIt = new Block(readBlockUnchecked(h)).iterator(InternalKey.COMPARATOR);
      }

      private void skipEmptyBlocks() {
        while (dataIt != null && !dataIt.valid()) {
          indexIt.next();
          loadBlock();
          if (dataIt != null) {
            dataIt.seekToFirst();
          }
        }
      }
    };
  }

  private byte[] readBlockUnchecked(BlockHandle h) {
    try {
      return readBlock(file, number, h);
    } catch (IOException e) {
      throw new StorageException("reading table " + number, new UncheckedIOException(e));
    }
  }

  private static byte[] readBlock(FileHandle file, long number, BlockHandle h) throws IOException {
    if (h.size() < 0 || h.offset() < 0) {
      throw new CorruptionException("table " + number + ": bad block handle " + h);
    }
    ByteBuffer buf = ByteBuffer.allocate(h.size() + 4);
    file.readFully(buf, h.offset());
    byte[] b = buf.array();
    int expected = ByteSource.readInt(b, h.size());
    if (Checksums.crc32c(b, 0, h.size()) != expected) {
      throw new CorruptionException(
          "table " + number + ": block checksum mismatch at " + h.offset());
    }
    byte[] contents = new byte[h.size()];
    System.arraycopy(b, 0, contents, 0, h.size());
    return contents;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }
}

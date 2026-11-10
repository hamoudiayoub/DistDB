package io.distdb.storage.lsm;

/**
 * SSTable layout constants.
 *
 * <pre>
 * [data block][crc] ... [data block][crc] [filter][crc] [index block][crc] [footer]
 *
 * footer (36 bytes): indexOffset (8) | indexSize (4) | filterOffset (8) | filterSize (4)
 *                    | magic (8) | crc32c of the preceding 32 bytes (4)
 * </pre>
 *
 * Every block is followed by the CRC32C of its contents. The index block maps the last internal key
 * of each data block to that block's handle ({@code offset (8) | size (4)}), so a seek
 * binary-searches the index for the first block whose last key is at least the target.
 */
final class TableFormat {

  static final int FOOTER_BYTES = 36;
  static final long MAGIC = 0x6469737464625353L; // "distdbSS"
  static final int DATA_RESTART_INTERVAL = 16;
  static final int INDEX_RESTART_INTERVAL = 1;

  private TableFormat() {}

  /** Where a block lives in the file; {@code size} excludes the 4-byte checksum. */
  record BlockHandle(long offset, int size) {
    byte[] encode() {
      ByteSink s = new ByteSink(12);
      s.writeLong(offset);
      s.writeInt(size);
      return s.toByteArray();
    }

    static BlockHandle decode(byte[] b) {
      ByteSource in = new ByteSource(b);
      return new BlockHandle(in.readLong(), in.readInt());
    }
  }
}

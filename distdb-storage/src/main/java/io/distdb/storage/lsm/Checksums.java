package io.distdb.storage.lsm;

import java.util.zip.CRC32C;

/** CRC32C (Castagnoli), hardware-accelerated on modern CPUs; used for every on-disk record. */
final class Checksums {

  private Checksums() {}

  static int crc32c(byte[] b, int off, int len) {
    CRC32C crc = new CRC32C();
    crc.update(b, off, len);
    return (int) crc.getValue();
  }
}

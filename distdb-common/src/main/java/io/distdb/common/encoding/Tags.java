package io.distdb.common.encoding;

/**
 * The first byte of every encoded component. Ascending tags are below {@code 0x80}; a descending
 * component inverts its tag along with its payload, so its tag is above {@code 0x80}. That makes
 * every component self-describing: a reader can tell type and direction from the tag alone.
 *
 * <p>Gaps are left for future types (decimal, timestamp, ...) so they can slot in between without
 * changing existing tags. Tags are part of the on-disk format: never renumber one.
 */
final class Tags {
  static final int NULL = 0x05;
  static final int FALSE = 0x10;
  static final int TRUE = 0x11;
  static final int LONG = 0x20;
  static final int DOUBLE = 0x30;
  static final int BYTES = 0x40;
  static final int STRING = 0x41;

  /** Escapes a {@code 0x00} content byte inside bytes and strings: {@code 00 FF}. */
  static final int ESCAPED_ZERO = 0xFF;

  /** Ends bytes and strings: {@code 00 01}. Sorts below both an escaped zero and any content. */
  static final int TERMINATOR = 0x01;

  private Tags() {}
}

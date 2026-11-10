package io.distdb.common.encoding;

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Builds an order-preserving key from typed components: for two keys built from the same sequence
 * of types and orders, comparing the bytes as unsigned (see {@link io.distdb.common.Bytes}) gives
 * the same answer as comparing the values component by component. A key that is a prefix of another
 * sorts first.
 *
 * <p>Each component is a tag byte (see {@link Tags}) followed by its payload:
 *
 * <ul>
 *   <li>{@code long}: 8 bytes big-endian with the sign bit flipped.
 *   <li>{@code double}: 8 bytes big-endian; positives have the sign bit flipped, negatives have
 *       every bit inverted. This matches {@link Double#compare}: {@code -0.0 < 0.0} and {@code NaN}
 *       is largest. All NaNs are stored as the canonical NaN.
 *   <li>bytes and strings (UTF-8): each {@code 00} byte becomes {@code 00 FF}, and the value ends
 *       with {@code 00 01}. Strings therefore sort by Unicode code point, like PostgreSQL's {@code
 *       C} collation, not by {@link String#compareTo} (which compares UTF-16 units).
 *   <li>{@code boolean} and {@code NULL}: the tag alone.
 * </ul>
 *
 * <p>A descending component is written ascending and then has every one of its bytes inverted.
 * Every component encoding is prefix-free (no encoding is a proper prefix of another), which is
 * what makes inversion reverse the order exactly.
 *
 * <p>Not thread-safe.
 */
public final class KeyWriter {

  private byte[] buf;
  private int size;

  /** An empty key. */
  public KeyWriter() {
    this(32);
  }

  /** An empty key with room for {@code initialCapacity} bytes before it grows. */
  public KeyWriter(int initialCapacity) {
    buf = new byte[Math.max(1, initialCapacity)];
  }

  /** Appends an ascending {@code NULL}. */
  public KeyWriter writeNull() {
    return writeNull(Order.ASC);
  }

  /** Appends a {@code NULL}. */
  public KeyWriter writeNull(Order order) {
    int start = size;
    put(Tags.NULL);
    return finish(start, order);
  }

  /** Appends an ascending boolean ({@code false < true}). */
  public KeyWriter writeBoolean(boolean value) {
    return writeBoolean(value, Order.ASC);
  }

  /** Appends a boolean ({@code false < true} when ascending). */
  public KeyWriter writeBoolean(boolean value, Order order) {
    int start = size;
    put(value ? Tags.TRUE : Tags.FALSE);
    return finish(start, order);
  }

  /** Appends an ascending signed {@code long}. */
  public KeyWriter writeLong(long value) {
    return writeLong(value, Order.ASC);
  }

  /** Appends a signed {@code long}. */
  public KeyWriter writeLong(long value, Order order) {
    int start = size;
    put(Tags.LONG);
    putLong(value ^ Long.MIN_VALUE);
    return finish(start, order);
  }

  /** Appends an ascending {@code double}. */
  public KeyWriter writeDouble(double value) {
    return writeDouble(value, Order.ASC);
  }

  /** Appends a {@code double}, ordered as {@link Double#compare}. */
  public KeyWriter writeDouble(double value, Order order) {
    long bits = Double.doubleToLongBits(value); // canonicalizes NaN
    bits = bits < 0 ? ~bits : bits ^ Long.MIN_VALUE;
    int start = size;
    put(Tags.DOUBLE);
    putLong(bits);
    return finish(start, order);
  }

  /** Appends an ascending byte string. */
  public KeyWriter writeBytes(byte[] value) {
    return writeBytes(value, Order.ASC);
  }

  /** Appends a byte string, ordered as unsigned bytes with shorter prefixes first. */
  public KeyWriter writeBytes(byte[] value, Order order) {
    int start = size;
    put(Tags.BYTES);
    putEscaped(value);
    return finish(start, order);
  }

  /** Appends an ascending string. */
  public KeyWriter writeString(String value) {
    return writeString(value, Order.ASC);
  }

  /**
   * Appends a string, ordered by Unicode code point.
   *
   * @throws IllegalArgumentException if {@code value} is not valid Unicode (an unpaired surrogate)
   */
  public KeyWriter writeString(String value, Order order) {
    int start = size;
    put(Tags.STRING);
    putEscaped(strictUtf8(value));
    return finish(start, order);
  }

  /** The number of bytes written so far. */
  public int size() {
    return size;
  }

  /** A copy of the key built so far. */
  public byte[] toByteArray() {
    return Arrays.copyOf(buf, size);
  }

  private static byte[] strictUtf8(String value) {
    try {
      var bytes =
          StandardCharsets.UTF_8
              .newEncoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .encode(CharBuffer.wrap(value));
      byte[] out = new byte[bytes.remaining()];
      bytes.get(out);
      return out;
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException("string is not valid Unicode", e);
    }
  }

  private void putEscaped(byte[] value) {
    ensure(value.length + 2);
    for (byte b : value) {
      if (b == 0) {
        ensure(2);
        buf[size++] = 0;
        buf[size++] = (byte) Tags.ESCAPED_ZERO;
      } else {
        ensure(1);
        buf[size++] = b;
      }
    }
    ensure(2);
    buf[size++] = 0;
    buf[size++] = (byte) Tags.TERMINATOR;
  }

  private void putLong(long v) {
    ensure(8);
    for (int shift = 56; shift >= 0; shift -= 8) {
      buf[size++] = (byte) (v >>> shift);
    }
  }

  private void put(int b) {
    ensure(1);
    buf[size++] = (byte) b;
  }

  private KeyWriter finish(int start, Order order) {
    if (order == Order.DESC) {
      for (int i = start; i < size; i++) {
        buf[i] = (byte) ~buf[i];
      }
    }
    return this;
  }

  private void ensure(int extra) {
    if (size + extra > buf.length) {
      buf = Arrays.copyOf(buf, Math.max(buf.length * 2, size + extra));
    }
  }
}

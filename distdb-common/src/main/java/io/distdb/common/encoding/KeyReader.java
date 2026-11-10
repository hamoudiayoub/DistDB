package io.distdb.common.encoding;

import io.distdb.common.Bytes;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Decodes keys written by {@link KeyWriter}, one component at a time. Components carry their type
 * and direction in their tag, so the reader needs no schema; each {@code read} method checks that
 * the next component has the type it expects.
 *
 * <p>Every method throws {@link IllegalArgumentException} on a truncated or corrupt key. Not
 * thread-safe.
 */
public final class KeyReader {

  private final byte[] buf;
  private final int end;
  private int pos;

  /** Reads all of {@code key}. */
  public KeyReader(byte[] key) {
    this(key, 0, key.length);
  }

  /** Reads {@code key[offset, offset + length)}. */
  public KeyReader(byte[] key, int offset, int length) {
    Objects.checkFromIndexSize(offset, length, key.length);
    this.buf = key;
    this.pos = offset;
    this.end = offset + length;
  }

  /** Whether any components are left. */
  public boolean hasRemaining() {
    return pos < end;
  }

  /** The number of bytes not yet read. */
  public int remaining() {
    return end - pos;
  }

  /** The type of the next component, without consuming it. */
  public ValueType peekType() {
    return switch (ascendingTag(peekTag())) {
      case Tags.NULL -> ValueType.NULL;
      case Tags.FALSE, Tags.TRUE -> ValueType.BOOLEAN;
      case Tags.LONG -> ValueType.LONG;
      case Tags.DOUBLE -> ValueType.DOUBLE;
      case Tags.BYTES -> ValueType.BYTES;
      case Tags.STRING -> ValueType.STRING;
      default -> throw corrupt("unknown tag 0x%02x".formatted(peekTag()));
    };
  }

  /** The direction of the next component, without consuming it. */
  public Order peekOrder() {
    return isDescending(peekTag()) ? Order.DESC : Order.ASC;
  }

  /** Consumes a {@code NULL}. */
  public void readNull() {
    expect(Tags.NULL);
  }

  /** Consumes a boolean. */
  public boolean readBoolean() {
    int tag = ascendingTag(peekTag());
    if (tag != Tags.FALSE && tag != Tags.TRUE) {
      throw corrupt("expected BOOLEAN but found " + peekType());
    }
    pos++;
    return tag == Tags.TRUE;
  }

  /** Consumes a {@code long}. */
  public long readLong() {
    int mask = mask(expect(Tags.LONG));
    return readRawLong(mask) ^ Long.MIN_VALUE;
  }

  /** Consumes a {@code double}. */
  public double readDouble() {
    int mask = mask(expect(Tags.DOUBLE));
    long bits = readRawLong(mask);
    bits = bits < 0 ? bits ^ Long.MIN_VALUE : ~bits;
    return Double.longBitsToDouble(bits);
  }

  /** Consumes a byte string. */
  public byte[] readBytes() {
    int mask = mask(expect(Tags.BYTES));
    return readEscaped(mask);
  }

  /** Consumes a string. */
  public String readString() {
    int mask = mask(expect(Tags.STRING));
    byte[] utf8 = readEscaped(mask);
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(utf8))
          .toString();
    } catch (CharacterCodingException e) {
      throw corrupt("string is not valid UTF-8");
    }
  }

  /** Consumes the next component, whatever its type. */
  public void skip() {
    switch (peekType()) {
      case NULL -> readNull();
      case BOOLEAN -> readBoolean();
      case LONG -> readLong();
      case DOUBLE -> readDouble();
      case BYTES -> readBytes();
      case STRING -> readString();
    }
  }

  /**
   * A readable rendering such as {@code /7/"accounts"/x'00ff'/NULL(desc)}, for logs and debug
   * pages. Never throws: an undecodable tail is shown as hex after {@code /?}.
   */
  public static String toDebugString(byte[] key) {
    StringBuilder sb = new StringBuilder();
    KeyReader r = new KeyReader(key);
    while (r.hasRemaining()) {
      int start = r.pos;
      int rendered = sb.length();
      try {
        Order order = r.peekOrder();
        sb.append('/');
        switch (r.peekType()) {
          case NULL -> {
            r.readNull();
            sb.append("NULL");
          }
          case BOOLEAN -> sb.append(r.readBoolean());
          case LONG -> sb.append(r.readLong());
          case DOUBLE -> sb.append(r.readDouble());
          case BYTES -> sb.append("x'").append(Bytes.toHex(r.readBytes())).append('\'');
          case STRING -> sb.append('"').append(r.readString()).append('"');
        }
        if (order == Order.DESC) {
          sb.append("(desc)");
        }
      } catch (IllegalArgumentException e) {
        sb.setLength(rendered);
        sb.append("/?").append(Bytes.toHex(Arrays.copyOfRange(key, start, key.length)));
        break;
      }
    }
    return sb.toString();
  }

  private byte[] readEscaped(int mask) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    while (true) {
      int b = next() ^ mask;
      if (b != 0) {
        out.write(b);
        continue;
      }
      int marker = next() ^ mask;
      if (marker == Tags.TERMINATOR) {
        return out.toByteArray();
      }
      if (marker != Tags.ESCAPED_ZERO) {
        throw corrupt("bad escape byte 0x%02x".formatted(marker));
      }
      out.write(0);
    }
  }

  private long readRawLong(int mask) {
    if (end - pos < 8) {
      throw corrupt("truncated: 8 bytes needed, " + (end - pos) + " left");
    }
    long v = 0;
    for (int i = 0; i < 8; i++) {
      v = (v << 8) | ((buf[pos++] ^ mask) & 0xFF);
    }
    return v;
  }

  /** Consumes the tag of the expected type and returns it as stored (inverted if descending). */
  private int expect(int ascendingTag) {
    int raw = peekTag();
    if (ascendingTag(raw) != ascendingTag) {
      throw corrupt("unexpected tag 0x%02x".formatted(raw));
    }
    pos++;
    return raw;
  }

  private int next() {
    if (pos >= end) {
      throw corrupt("truncated: unterminated bytes or string");
    }
    return buf[pos++] & 0xFF;
  }

  private int peekTag() {
    if (pos >= end) {
      throw corrupt("no components left");
    }
    return buf[pos] & 0xFF;
  }

  private static boolean isDescending(int rawTag) {
    return rawTag >= 0x80;
  }

  private static int ascendingTag(int rawTag) {
    return isDescending(rawTag) ? ~rawTag & 0xFF : rawTag;
  }

  private static int mask(int rawTag) {
    return isDescending(rawTag) ? 0xFF : 0x00;
  }

  private IllegalArgumentException corrupt(String why) {
    return new IllegalArgumentException("corrupt key at offset " + pos + ": " + why);
  }
}

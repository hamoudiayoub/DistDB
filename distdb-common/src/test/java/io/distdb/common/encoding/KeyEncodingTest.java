package io.distdb.common.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.common.Bytes;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

/**
 * Example-based tests. The golden-byte tests pin the on-disk format: if one fails, an existing
 * database would no longer read, so change the format deliberately or not at all.
 */
class KeyEncodingTest {

  private static String hex(KeyWriter w) {
    return Bytes.toHex(w.toByteArray());
  }

  @Test
  void goldenBytes() {
    assertThat(hex(new KeyWriter().writeNull())).isEqualTo("05");
    assertThat(hex(new KeyWriter().writeNull(Order.DESC))).isEqualTo("fa");
    assertThat(hex(new KeyWriter().writeBoolean(false))).isEqualTo("10");
    assertThat(hex(new KeyWriter().writeBoolean(true))).isEqualTo("11");
    assertThat(hex(new KeyWriter().writeLong(0))).isEqualTo("208000000000000000");
    assertThat(hex(new KeyWriter().writeLong(-1))).isEqualTo("207fffffffffffffff");
    assertThat(hex(new KeyWriter().writeLong(1, Order.DESC))).isEqualTo("df7ffffffffffffffe");
    assertThat(hex(new KeyWriter().writeDouble(1.0))).isEqualTo("30bff0000000000000");
    assertThat(hex(new KeyWriter().writeDouble(-1.0))).isEqualTo("30400fffffffffffff");
    assertThat(hex(new KeyWriter().writeBytes(new byte[] {0, 1}))).isEqualTo("4000ff010001");
    assertThat(hex(new KeyWriter().writeString("a\0"))).isEqualTo("416100ff0001");
    assertThat(hex(new KeyWriter().writeString("", Order.DESC))).isEqualTo("befffe");
  }

  @Test
  void compositeKeyReadsBackInOrder() {
    byte[] key =
        new KeyWriter()
            .writeLong(7)
            .writeString("accounts")
            .writeDouble(-2.5, Order.DESC)
            .writeNull(Order.DESC)
            .writeBoolean(true)
            .writeBytes(new byte[] {0, (byte) 0xFF})
            .toByteArray();

    KeyReader r = new KeyReader(key);
    assertThat(r.readLong()).isEqualTo(7);
    assertThat(r.readString()).isEqualTo("accounts");
    assertThat(r.peekOrder()).isEqualTo(Order.DESC);
    assertThat(r.readDouble()).isEqualTo(-2.5);
    assertThat(r.peekType()).isEqualTo(ValueType.NULL);
    r.readNull();
    assertThat(r.readBoolean()).isTrue();
    assertThat(r.readBytes()).containsExactly(0, 0xFF);
    assertThat(r.hasRemaining()).isFalse();
  }

  @Test
  void skipConsumesAnyComponent() {
    byte[] key = new KeyWriter().writeString("x", Order.DESC).writeLong(3).toByteArray();
    KeyReader r = new KeyReader(key);
    r.skip();
    assertThat(r.readLong()).isEqualTo(3);
  }

  @Test
  void readerHonoursOffsetAndLength() {
    byte[] inner = new KeyWriter().writeLong(42).toByteArray();
    byte[] framed = new byte[inner.length + 4];
    System.arraycopy(inner, 0, framed, 2, inner.length);
    KeyReader r = new KeyReader(framed, 2, inner.length);
    assertThat(r.readLong()).isEqualTo(42);
    assertThat(r.hasRemaining()).isFalse();
  }

  @Test
  void debugStringIsReadable() {
    byte[] key =
        new KeyWriter()
            .writeLong(7)
            .writeString("accounts")
            .writeBytes(new byte[] {0, (byte) 0xFF})
            .writeNull(Order.DESC)
            .toByteArray();
    assertThat(KeyReader.toDebugString(key)).isEqualTo("/7/\"accounts\"/x'00ff'/NULL(desc)");
  }

  @Test
  void debugStringShowsUndecodableTailAsHex() {
    byte[] key = HexFormat.of().parseHex("2080000000000000010203");
    assertThat(KeyReader.toDebugString(key)).isEqualTo("/1/?0203");
  }

  @Test
  void rejectsUnpairedSurrogate() {
    assertThatThrownBy(() -> new KeyWriter().writeString("bad\uD800"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unicode");
  }

  @Test
  void rejectsWrongType() {
    byte[] key = new KeyWriter().writeString("x").toByteArray();
    assertThatThrownBy(() -> new KeyReader(key).readLong())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unexpected tag");
  }

  @Test
  void rejectsTruncatedLong() {
    byte[] key = HexFormat.of().parseHex("208000");
    assertThatThrownBy(() -> new KeyReader(key).readLong())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("truncated");
  }

  @Test
  void rejectsUnterminatedString() {
    byte[] key = HexFormat.of().parseHex("416162");
    assertThatThrownBy(() -> new KeyReader(key).readString())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unterminated");
  }

  @Test
  void rejectsBadEscape() {
    byte[] key = HexFormat.of().parseHex("40000501");
    assertThatThrownBy(() -> new KeyReader(key).readBytes())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bad escape");
  }

  @Test
  void rejectsInvalidUtf8() {
    byte[] key = HexFormat.of().parseHex("41c30001");
    assertThatThrownBy(() -> new KeyReader(key).readString())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("UTF-8");
  }

  @Test
  void rejectsUnknownTag() {
    assertThatThrownBy(() -> new KeyReader(new byte[] {0x77}).peekType())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unknown tag");
  }

  @Test
  void rejectsReadingPastTheEnd() {
    assertThatThrownBy(() -> new KeyReader(Bytes.EMPTY).readNull())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no components left");
  }
}

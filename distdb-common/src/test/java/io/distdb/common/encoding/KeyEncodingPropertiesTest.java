package io.distdb.common.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.common.Bytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.statistics.Statistics;

/**
 * The Phase 1 gate: for every type and both directions, the byte order of encoded keys equals the
 * order of the values they encode, and decoding returns the original value.
 */
class KeyEncodingPropertiesTest {

  // ---- longs ----

  @Property(tries = 2000)
  void longOrderMatchesValueOrder(
      @ForAll("longs") long a, @ForAll("longs") long b, @ForAll Order order) {
    assertSameOrder(encode(a, order), encode(b, order), Long.compare(a, b), order);
  }

  @Property
  void longRoundTrips(@ForAll("longs") long v, @ForAll Order order) {
    assertThat(new KeyReader(encode(v, order)).readLong()).isEqualTo(v);
  }

  @Provide
  Arbitrary<Long> longs() {
    return Arbitraries.oneOf(
        Arbitraries.longs(),
        Arbitraries.longs().between(-3, 3),
        Arbitraries.of(Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L, Long.MAX_VALUE));
  }

  // ---- doubles ----

  @Property(tries = 2000)
  void doubleOrderMatchesDoubleCompare(
      @ForAll("doubles") double a, @ForAll("doubles") double b, @ForAll Order order) {
    assertSameOrder(encode(a, order), encode(b, order), Double.compare(a, b), order);
  }

  @Property
  void doubleRoundTrips(@ForAll("doubles") double v, @ForAll Order order) {
    double decoded = new KeyReader(encode(v, order)).readDouble();
    assertThat(Double.compare(decoded, v)).isZero();
  }

  @Provide
  Arbitrary<Double> doubles() {
    return Arbitraries.oneOf(
        // Every bit pattern, including subnormals and NaNs with payloads.
        Arbitraries.longs().map(Double::longBitsToDouble),
        Arbitraries.doubles(),
        Arbitraries.of(
            Double.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            -1.0,
            -Double.MIN_VALUE,
            -0.0,
            0.0,
            Double.MIN_VALUE,
            1.0,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY,
            Double.NaN));
  }

  // ---- booleans ----

  @Property
  void booleanOrderMatchesValueOrder(@ForAll boolean a, @ForAll boolean b, @ForAll Order order) {
    byte[] ea = new KeyWriter().writeBoolean(a, order).toByteArray();
    byte[] eb = new KeyWriter().writeBoolean(b, order).toByteArray();
    assertSameOrder(ea, eb, Boolean.compare(a, b), order);
    assertThat(new KeyReader(ea).readBoolean()).isEqualTo(a);
  }

  // ---- byte strings ----

  @Property(tries = 2000)
  void bytesOrderMatchesUnsignedOrder(
      @ForAll("byteArrays") byte[] a, @ForAll("byteArrays") byte[] b, @ForAll Order order) {
    int expected = Arrays.compareUnsigned(a, b);
    assertSameOrder(encode(a, order), encode(b, order), expected, order);
    Statistics.label("comparison").collect(Integer.signum(expected));
  }

  @Property
  void bytesRoundTrip(@ForAll("byteArrays") byte[] v, @ForAll Order order) {
    assertThat(new KeyReader(encode(v, order)).readBytes()).isEqualTo(v);
  }

  @Property
  void bytesEncodingIsPrefixFree(
      @ForAll("byteArrays") byte[] a, @ForAll("byteArrays") byte[] b, @ForAll Order order) {
    byte[] ea = encode(a, order);
    byte[] eb = encode(b, order);
    if (!Arrays.equals(a, b)) {
      assertThat(Bytes.startsWith(eb, ea)).isFalse();
    }
  }

  @Provide
  Arbitrary<byte[]> byteArrays() {
    // Short arrays over a few interesting bytes, so that equal prefixes, embedded zeros and bytes
    // next to the escape and terminator values come up constantly.
    Arbitrary<Byte> interesting =
        Arbitraries.of((byte) 0x00, (byte) 0x01, (byte) 0x02, (byte) 0xFE, (byte) 0xFF);
    Arbitrary<Byte> any = Arbitraries.bytes();
    return Arbitraries.frequencyOf(
            net.jqwik.api.Tuple.of(3, interesting), net.jqwik.api.Tuple.of(1, any))
        .array(byte[].class)
        .ofMaxSize(8);
  }

  // ---- strings ----

  @Property(tries = 2000)
  void stringOrderMatchesCodePointOrder(
      @ForAll("strings") String a, @ForAll("strings") String b, @ForAll Order order) {
    int expected = compareCodePoints(a, b);
    assertSameOrder(encode(a, order), encode(b, order), expected, order);
    Statistics.label("comparison").collect(Integer.signum(expected));
  }

  @Property
  void stringRoundTrips(@ForAll("strings") String v, @ForAll Order order) {
    assertThat(new KeyReader(encode(v, order)).readString()).isEqualTo(v);
  }

  @Provide
  Arbitrary<String> strings() {
    Arbitrary<Integer> interesting =
        Arbitraries.of(
            0x00, 0x01, (int) 'a', (int) 'b', 0x7F, 0x80, 0xFFFF, 0xE000, 0x10000, 0x10FFFF);
    Arbitrary<Integer> anyCodePoint =
        Arbitraries.integers()
            .between(0, Character.MAX_CODE_POINT)
            .filter(cp -> cp < Character.MIN_SURROGATE || cp > Character.MAX_SURROGATE);
    return Arbitraries.frequencyOf(
            net.jqwik.api.Tuple.of(3, interesting), net.jqwik.api.Tuple.of(1, anyCodePoint))
        .list()
        .ofMaxSize(6)
        .map(cps -> {
          StringBuilder sb = new StringBuilder();
          cps.forEach(sb::appendCodePoint);
          return sb.toString();
        });
  }

  // ---- composite keys ----

  /** One column of a key schema. */
  record Column(ValueType type, Order order) {}

  /** Two keys with the same schema; either may be a prefix (fewer components). */
  record KeyPair(List<Column> schema, List<Object> a, List<Object> b) {}

  @Property(tries = 3000)
  void compositeKeyOrderMatchesTupleOrder(@ForAll("keyPairs") KeyPair p) {
    int expected = compareTuples(p.schema(), p.a(), p.b());
    int actual = Bytes.compare(encodeTuple(p.schema(), p.a()), encodeTuple(p.schema(), p.b()));
    assertThat(Integer.signum(actual))
        .as("schema %s, %s vs %s", p.schema(), p.a(), p.b())
        .isEqualTo(Integer.signum(expected));
    Statistics.label("comparison").collect(Integer.signum(expected));
    Statistics.label("comparison").coverage(c -> c.check(0).percentage(pct -> pct > 2.0));
  }

  @Property(tries = 1000)
  void compositeKeyRoundTrips(@ForAll("keyPairs") KeyPair p) {
    byte[] key = encodeTuple(p.schema(), p.a());
    KeyReader r = new KeyReader(key);
    for (int i = 0; i < p.a().size(); i++) {
      Object expected = p.a().get(i);
      assertThat(r.peekOrder()).isEqualTo(p.schema().get(i).order());
      if (expected == null) {
        assertThat(r.peekType()).isEqualTo(ValueType.NULL);
        r.readNull();
        continue;
      }
      assertThat(r.peekType()).isEqualTo(p.schema().get(i).type());
      Object actual =
          switch (p.schema().get(i).type()) {
            case BOOLEAN -> r.readBoolean();
            case LONG -> r.readLong();
            case DOUBLE -> r.readDouble();
            case BYTES -> r.readBytes();
            case STRING -> r.readString();
            case NULL -> throw new AssertionError();
          };
      assertThat(compareValues(p.schema().get(i).type(), actual, expected)).isZero();
    }
    assertThat(r.hasRemaining()).isFalse();
  }

  @Provide
  Arbitrary<KeyPair> keyPairs() {
    Arbitrary<Column> columns =
        Combinators.combine(
                Arbitraries.of(
                    ValueType.BOOLEAN,
                    ValueType.LONG,
                    ValueType.DOUBLE,
                    ValueType.BYTES,
                    ValueType.STRING),
                Arbitraries.of(Order.class))
            .as(Column::new);
    return columns
        .list()
        .ofMinSize(1)
        .ofMaxSize(4)
        .flatMap(schema ->
            Combinators.combine(tuple(schema), tuple(schema))
                .as((a, b) -> new KeyPair(schema, a, b)));
  }

  /** A tuple for {@code schema}, truncated to a random length, over small value domains. */
  private static Arbitrary<List<Object>> tuple(List<Column> schema) {
    List<Arbitrary<Object>> perColumn =
        schema.stream().map(c -> smallDomain(c.type()).injectNull(0.15)).toList();
    return Combinators.combine(perColumn)
        .as(values -> values)
        .flatMap(values ->
            Arbitraries.integers()
                .between(0, values.size())
                .map(n -> (List<Object>) new ArrayList<>(values.subList(0, n))));
  }

  private static Arbitrary<Object> smallDomain(ValueType type) {
    return switch (type) {
      case BOOLEAN -> Arbitraries.<Object>of(false, true);
      case LONG -> Arbitraries.<Object>of(Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE);
      case DOUBLE ->
          Arbitraries.<Object>of(Double.NEGATIVE_INFINITY, -1.5, -0.0, 0.0, 2.5, Double.NaN);
      case BYTES ->
          Arbitraries.<Object>of(
              new byte[0], new byte[] {0}, new byte[] {0, 0}, new byte[] {1}, new byte[] {-1});
      case STRING -> Arbitraries.<Object>of("", "\0", "a", "a\0", "ab", "￿", "😀");
      case NULL -> throw new IllegalArgumentException();
    };
  }

  // ---- model ----

  private static int compareTuples(List<Column> schema, List<Object> a, List<Object> b) {
    int n = Math.min(a.size(), b.size());
    for (int i = 0; i < n; i++) {
      Column col = schema.get(i);
      int c = compareNullable(col.type(), a.get(i), b.get(i));
      if (c != 0) {
        return col.order() == Order.ASC ? c : -c;
      }
    }
    return Integer.compare(a.size(), b.size());
  }

  /** Ascending comparison with NULL first. */
  private static int compareNullable(ValueType type, Object a, Object b) {
    if (a == null || b == null) {
      return a == null ? (b == null ? 0 : -1) : 1;
    }
    return compareValues(type, a, b);
  }

  private static int compareValues(ValueType type, Object a, Object b) {
    return switch (type) {
      case BOOLEAN -> Boolean.compare((Boolean) a, (Boolean) b);
      case LONG -> Long.compare((Long) a, (Long) b);
      case DOUBLE -> Double.compare((Double) a, (Double) b);
      case BYTES -> Arrays.compareUnsigned((byte[]) a, (byte[]) b);
      case STRING -> compareCodePoints((String) a, (String) b);
      case NULL -> 0;
    };
  }

  private static int compareCodePoints(String a, String b) {
    return Arrays.compare(a.codePoints().toArray(), b.codePoints().toArray());
  }

  private static byte[] encodeTuple(List<Column> schema, List<Object> values) {
    KeyWriter w = new KeyWriter();
    for (int i = 0; i < values.size(); i++) {
      Column col = schema.get(i);
      Object v = values.get(i);
      if (v == null) {
        w.writeNull(col.order());
        continue;
      }
      switch (col.type()) {
        case BOOLEAN -> w.writeBoolean((Boolean) v, col.order());
        case LONG -> w.writeLong((Long) v, col.order());
        case DOUBLE -> w.writeDouble((Double) v, col.order());
        case BYTES -> w.writeBytes((byte[]) v, col.order());
        case STRING -> w.writeString((String) v, col.order());
        case NULL -> throw new AssertionError();
      }
    }
    return w.toByteArray();
  }

  // ---- helpers ----

  private static void assertSameOrder(byte[] ea, byte[] eb, int valueOrder, Order order) {
    int expected = order == Order.ASC ? Integer.signum(valueOrder) : -Integer.signum(valueOrder);
    assertThat(Integer.signum(Bytes.compare(ea, eb))).isEqualTo(expected);
  }

  private static byte[] encode(long v, Order order) {
    return new KeyWriter().writeLong(v, order).toByteArray();
  }

  private static byte[] encode(double v, Order order) {
    return new KeyWriter().writeDouble(v, order).toByteArray();
  }

  private static byte[] encode(byte[] v, Order order) {
    return new KeyWriter().writeBytes(v, order).toByteArray();
  }

  private static byte[] encode(String v, Order order) {
    return new KeyWriter().writeString(v, order).toByteArray();
  }
}

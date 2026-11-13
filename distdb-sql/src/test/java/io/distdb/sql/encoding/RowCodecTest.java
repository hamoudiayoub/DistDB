package io.distdb.sql.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.common.Bytes;
import io.distdb.common.encoding.KeyReader;
import io.distdb.common.encoding.KeyWriter;
import io.distdb.sql.types.Type;
import io.distdb.sql.types.Values;
import java.math.BigDecimal;
import java.math.BigInteger;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/** Key order is SQL order, for every type: what makes range scans and index lookups correct. */
class RowCodecTest {

  private static byte[] key(Type t, Object v) {
    KeyWriter w = new KeyWriter();
    RowCodec.writeKey(w, t, v);
    return w.toByteArray();
  }

  private static void sameOrder(Type t, Object a, Object b) {
    int sql = Integer.signum(Values.compare(a, b));
    int bytes = Integer.signum(Bytes.compare(key(t, a), key(t, b)));
    assertThat(bytes).as("%s vs %s", a, b).isEqualTo(sql);
    assertThat(RowCodec.readKey(new KeyReader(key(t, a)), t))
        .satisfies(back -> assertThat(Values.compare(back, a)).isZero());
  }

  @Provide
  Arbitrary<BigDecimal> decimals() {
    Arbitrary<BigInteger> unscaled =
        Arbitraries.bigIntegers()
            .between(new BigInteger("-1" + "0".repeat(30)), new BigInteger("1" + "0".repeat(30)));
    return Combinators.combine(unscaled, Arbitraries.integers().between(0, 25)).as(BigDecimal::new);
  }

  @Property(tries = 3000)
  void numericKeysSortByValue(@ForAll("decimals") BigDecimal a, @ForAll("decimals") BigDecimal b) {
    sameOrder(Type.NUMERIC, a, b);
  }

  @Property(tries = 2000)
  void integerKeysSortByValue(@ForAll long a, @ForAll long b) {
    sameOrder(Type.INT, a, b);
  }

  @Property(tries = 2000)
  void floatKeysSortByValue(@ForAll double a, @ForAll double b) {
    sameOrder(Type.FLOAT, a, b);
  }

  @Property(tries = 2000)
  void textKeysSortByCodePoint(@ForAll String a, @ForAll String b) {
    sameOrder(Type.TEXT, a, b);
  }

  @Property(tries = 200)
  void negativeZeroIsZero(@ForAll double ignored) {
    assertThat(key(Type.FLOAT, -0.0)).isEqualTo(key(Type.FLOAT, 0.0));
    assertThat(key(Type.NUMERIC, new BigDecimal("1.0")))
        .isEqualTo(key(Type.NUMERIC, new BigDecimal("1.00")));
  }
}

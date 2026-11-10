package io.distdb.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;

class BytesTest {

  @Property(tries = 3000)
  void prefixEndBoundsExactlyTheKeysWithThatPrefix(
      @ForAll("keys") byte[] prefix, @ForAll("keys") byte[] key) {
    Optional<byte[]> end = Bytes.prefixEnd(prefix);
    if (Bytes.startsWith(key, prefix)) {
      end.ifPresent(e -> assertThat(Bytes.compare(key, e)).isNegative());
    } else if (Bytes.compare(key, prefix) > 0) {
      assertThat(end).isPresent();
      assertThat(Bytes.compare(key, end.get())).isGreaterThanOrEqualTo(0);
    }
  }

  @Property
  void prefixEndIsEmptyOnlyForAllOnesPrefix(@ForAll("keys") byte[] prefix) {
    boolean allOnes = true;
    for (byte b : prefix) {
      allOnes &= b == (byte) 0xFF;
    }
    assertThat(Bytes.prefixEnd(prefix).isEmpty()).isEqualTo(allOnes);
  }

  @Provide
  Arbitrary<byte[]> keys() {
    return Arbitraries.of((byte) 0x00, (byte) 0x01, (byte) 0xFE, (byte) 0xFF)
        .array(byte[].class)
        .ofMaxSize(4);
  }

  @Test
  void prefixEndExamples() {
    assertThat(Bytes.prefixEnd(new byte[] {1, 2}))
        .hasValueSatisfying(e -> assertThat(e).containsExactly(1, 3));
    assertThat(Bytes.prefixEnd(new byte[] {1, (byte) 0xFF}))
        .hasValueSatisfying(e -> assertThat(e).containsExactly(2));
    assertThat(Bytes.prefixEnd(new byte[] {(byte) 0xFF})).isEmpty();
    assertThat(Bytes.prefixEnd(Bytes.EMPTY)).isEmpty();
  }

  @Test
  void compareIsUnsignedAndShorterPrefixFirst() {
    assertThat(Bytes.compare(new byte[] {(byte) 0x80}, new byte[] {0x7F})).isPositive();
    assertThat(Bytes.compare(new byte[] {1}, new byte[] {1, 0})).isNegative();
    assertThat(Bytes.compare(Bytes.EMPTY, new byte[] {0})).isNegative();
  }

  @Test
  void hex() {
    assertThat(Bytes.toHex(new byte[] {0, (byte) 0xAB})).isEqualTo("00ab");
  }
}

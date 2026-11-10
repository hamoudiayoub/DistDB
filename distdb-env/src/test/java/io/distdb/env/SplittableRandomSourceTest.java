package io.distdb.env;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class SplittableRandomSourceTest {

  private static long[] draw(RandomSource r, int n) {
    return LongStream.range(0, n).map(i -> r.nextLong()).toArray();
  }

  @Test
  void sameSeedGivesSameSequence() {
    assertThat(draw(SplittableRandomSource.seeded(42), 100))
        .isEqualTo(draw(SplittableRandomSource.seeded(42), 100));
  }

  @Test
  void differentSeedsDiverge() {
    assertThat(draw(SplittableRandomSource.seeded(1), 10))
        .isNotEqualTo(draw(SplittableRandomSource.seeded(2), 10));
  }

  @Test
  void splitStreamsAreDeterministicAndIndependent() {
    RandomSource a = SplittableRandomSource.seeded(7);
    RandomSource b = SplittableRandomSource.seeded(7);
    RandomSource childA = a.split();
    RandomSource childB = b.split();
    assertThat(draw(childA, 50)).isEqualTo(draw(childB, 50));
    // The parent keeps its own stream after splitting.
    assertThat(draw(a, 50)).isEqualTo(draw(b, 50)).isNotEqualTo(draw(childA, 50));
  }

  @Test
  void boundedDrawsStayInRange() {
    RandomSource r = SplittableRandomSource.seeded(3);
    for (int i = 0; i < 10_000; i++) {
      assertThat(r.nextInt(150, 300)).isBetween(150, 299);
      assertThat(r.nextInt(5)).isBetween(0, 4);
      assertThat(r.nextLong(-10, 10)).isBetween(-10L, 9L);
      assertThat(r.nextDouble()).isGreaterThanOrEqualTo(0.0).isLessThan(1.0);
    }
  }

  @Test
  void nextBytesFillsTheArray() {
    byte[] a = new byte[32];
    byte[] b = new byte[32];
    SplittableRandomSource.seeded(9).nextBytes(a);
    SplittableRandomSource.seeded(9).nextBytes(b);
    assertThat(a).isEqualTo(b).isNotEqualTo(new byte[32]);
  }
}

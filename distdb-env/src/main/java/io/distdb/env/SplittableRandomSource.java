package io.distdb.env;

import java.util.SplittableRandom;

/**
 * A {@link RandomSource} backed by {@link SplittableRandom}. Production seeds it from entropy; the
 * simulator seeds it with the run's seed so that every choice can be replayed. Not thread-safe.
 */
public final class SplittableRandomSource implements RandomSource {

  private final SplittableRandom random;

  private SplittableRandomSource(SplittableRandom random) {
    this.random = random;
  }

  /** A source whose whole sequence, and that of every stream split from it, is fixed by seed. */
  public static SplittableRandomSource seeded(long seed) {
    return new SplittableRandomSource(new SplittableRandom(seed));
  }

  /** A source seeded from the JVM's entropy; use in production only. */
  public static SplittableRandomSource fromEntropy() {
    return new SplittableRandomSource(new SplittableRandom());
  }

  @Override
  public long nextLong() {
    return random.nextLong();
  }

  @Override
  public long nextLong(long origin, long bound) {
    return random.nextLong(origin, bound);
  }

  @Override
  public int nextInt(int bound) {
    return random.nextInt(bound);
  }

  @Override
  public int nextInt(int origin, int bound) {
    return random.nextInt(origin, bound);
  }

  @Override
  public double nextDouble() {
    return random.nextDouble();
  }

  @Override
  public boolean nextBoolean() {
    return random.nextBoolean();
  }

  @Override
  public void nextBytes(byte[] bytes) {
    random.nextBytes(bytes);
  }

  @Override
  public RandomSource split() {
    return new SplittableRandomSource(random.split());
  }
}

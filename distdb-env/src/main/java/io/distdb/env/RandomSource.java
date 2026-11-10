package io.distdb.env;

/**
 * The only source of randomness for core logic (election timeouts, jitter, sampling). Core code
 * never constructs {@code java.util.Random} or uses {@code ThreadLocalRandom}.
 *
 * <p>A {@code RandomSource} is confined to one thread. Give each component its own stream with
 * {@link #split()}: splitting is deterministic, so a whole simulated cluster still derives every
 * random choice from one seed, and adding a component does not shift the streams of the others.
 */
public interface RandomSource {

  /** A uniformly distributed {@code long}. */
  long nextLong();

  /** A uniformly distributed {@code long} in {@code [origin, bound)}. */
  long nextLong(long origin, long bound);

  /** A uniformly distributed {@code int} in {@code [0, bound)}. */
  int nextInt(int bound);

  /** A uniformly distributed {@code int} in {@code [origin, bound)}. */
  int nextInt(int origin, int bound);

  /** A uniformly distributed {@code double} in {@code [0, 1)}. */
  double nextDouble();

  /** A fair coin flip. */
  boolean nextBoolean();

  /** Fills {@code bytes} with random bytes. */
  void nextBytes(byte[] bytes);

  /**
   * A new, independent stream. Its sequence is fully determined by this source's state, and calling
   * this method advances this source.
   */
  RandomSource split();
}

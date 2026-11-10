package io.distdb.client;

import java.time.Duration;

/**
 * Timings for a {@link KvClient}.
 *
 * @param defaultTimeout the overall deadline of a call that does not give one
 * @param maxTimeout the longest deadline a call may have. Servers keep a client's session (and so
 *     can recognize its retries) for an hour; capping deadlines far below that is what makes
 *     retrying safe.
 * @param attemptTimeout the longest a single attempt may wait before the client retries
 * @param initialBackoff the backoff ceiling before the first retry; it doubles each retry
 * @param maxBackoff the highest the backoff ceiling goes
 */
public record ClientOptions(
    Duration defaultTimeout,
    Duration maxTimeout,
    Duration attemptTimeout,
    Duration initialBackoff,
    Duration maxBackoff) {

  public ClientOptions {
    if (!isPositive(defaultTimeout)
        || defaultTimeout.compareTo(maxTimeout) > 0
        || !isPositive(attemptTimeout)
        || !isPositive(initialBackoff)
        || maxBackoff.compareTo(initialBackoff) < 0) {
      throw new IllegalArgumentException("invalid ClientOptions: " + this);
    }
  }

  /** 5 s calls (at most 5 min), 2 s attempts, backoff from 20 ms up to 1 s. */
  public static ClientOptions defaults() {
    return new ClientOptions(
        Duration.ofSeconds(5),
        Duration.ofMinutes(5),
        Duration.ofSeconds(2),
        Duration.ofMillis(20),
        Duration.ofSeconds(1));
  }

  /** A copy with a different {@link #defaultTimeout}. */
  public ClientOptions withDefaultTimeout(Duration timeout) {
    return new ClientOptions(timeout, maxTimeout, attemptTimeout, initialBackoff, maxBackoff);
  }

  private static boolean isPositive(Duration d) {
    return !d.isNegative() && !d.isZero();
  }
}

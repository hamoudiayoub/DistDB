package io.distdb.env;

/**
 * The only source of time for core logic. Core code never calls {@code System.nanoTime()}, {@code
 * System.currentTimeMillis()} or {@code Instant.now()} directly; it asks a {@code Clock}, which the
 * simulator replaces with virtual time that it advances itself.
 */
public interface Clock {

  /**
   * Monotonic time in nanoseconds from an arbitrary origin. Use it to measure elapsed time
   * (timeouts, leases, latencies). It never goes backwards within one process, and values from
   * different processes cannot be compared.
   */
  long nanoTime();

  /**
   * Wall-clock time in milliseconds since the Unix epoch. It can jump forwards or backwards (NTP,
   * operator changes), so never use it to measure durations. The timestamp oracle uses it as the
   * physical part of a timestamp and must tolerate it going backwards.
   */
  long wallTimeMillis();
}

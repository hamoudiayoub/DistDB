package io.distdb.kv;

import java.time.Duration;

/**
 * Limits and timings for a {@link KvService}.
 *
 * @param maxKeyBytes the longest key a client may write
 * @param maxValueBytes the longest value a client may write
 * @param defaultScanLimit entries per scan page when the request does not say
 * @param maxScanLimit the most entries one scan page may return
 * @param scanPageBytes a scan page stops once it holds about this many key and value bytes
 * @param maxInflightRequests beyond this many concurrent requests, new ones get {@code ServerBusy}
 * @param sessionTtl how long a silent client's session is kept; must be far longer than any client
 *     deadline, since a write retried after its session is forgotten can be applied twice
 * @param sessionSweepInterval how often expired sessions are looked for
 */
public record KvServiceOptions(
    int maxKeyBytes,
    int maxValueBytes,
    int defaultScanLimit,
    int maxScanLimit,
    int scanPageBytes,
    int maxInflightRequests,
    Duration sessionTtl,
    Duration sessionSweepInterval) {

  public KvServiceOptions {
    if (maxKeyBytes < 1
        || maxValueBytes < 0
        || defaultScanLimit < 1
        || maxScanLimit < defaultScanLimit
        || scanPageBytes < 1
        || maxInflightRequests < 1
        || sessionTtl.isNegative()
        || sessionSweepInterval.isNegative()
        || sessionSweepInterval.isZero()) {
      throw new IllegalArgumentException("invalid KvServiceOptions: " + this);
    }
  }

  /** 16 KiB keys, 1 MiB values, 1000-entry scan pages, 1024 requests in flight, 1 h sessions. */
  public static KvServiceOptions defaults() {
    return new KvServiceOptions(
        16 * 1024,
        1024 * 1024,
        1000,
        10_000,
        1024 * 1024,
        1024,
        Duration.ofHours(1),
        Duration.ofMinutes(1));
  }

  /** A copy with a different {@link #maxInflightRequests}. */
  public KvServiceOptions withMaxInflightRequests(int n) {
    return new KvServiceOptions(
        maxKeyBytes,
        maxValueBytes,
        defaultScanLimit,
        maxScanLimit,
        scanPageBytes,
        n,
        sessionTtl,
        sessionSweepInterval);
  }
}

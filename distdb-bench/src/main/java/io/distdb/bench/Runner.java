package io.distdb.bench;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;

/**
 * Drives a workload and measures it.
 *
 * <ul>
 *   <li><b>Closed loop</b> ({@link #closed}): {@code n} clients, each sends its next request when
 *       the last one answers. It finds the maximum throughput, but its latencies are optimistic: a
 *       stall delays the requests that were not sent during it, and they are never counted
 *       (coordinated omission).
 *   <li><b>Open loop</b> ({@link #open}): requests start on a fixed schedule, whatever happens, and
 *       each one's latency counts from when it was <i>supposed</i> to start. A stall is charged to
 *       every request it delayed, as a real user would feel it.
 * </ul>
 *
 * Both record per-100-ms throughput, for timelines (recovery after a crash).
 */
final class Runner {

  private Runner() {}

  /** An operation; it completes (or fails) when the system answered. */
  @FunctionalInterface
  interface Op {
    CompletableFuture<?> run(long seq);
  }

  /** A synchronous operation, for blocking clients (JDBC). */
  @FunctionalInterface
  interface SyncOp {
    void run(long seq) throws Exception;
  }

  /** What a run measured, after its warm-up. */
  record Result(
      long ops, long errors, double seconds, Histogram latency, long[] perTick, Duration tick) {
    double throughput() {
      return ops / seconds;
    }

    double ms(double q) {
      return latency.quantile(q) / 1e6;
    }
  }

  static final Duration TICK = Duration.ofMillis(100);

  /** {@code clients} closed-loop clients of {@code op}, each with its own thread. */
  static Result closed(int clients, Duration warmup, Duration measure, SyncOp op)
      throws InterruptedException {
    Histogram h = new Histogram();
    AtomicLong ok = new AtomicLong();
    AtomicLong errors = new AtomicLong();
    AtomicLong seq = new AtomicLong();
    long start = System.nanoTime();
    long measureFrom = start + warmup.toNanos();
    long end = measureFrom + measure.toNanos();
    int ticks = (int) (measure.toNanos() / TICK.toNanos()) + 1;
    AtomicLongArray perTick = new AtomicLongArray(ticks);
    List<Thread> threads = new ArrayList<>();
    for (int c = 0; c < clients; c++) {
      threads.add(
          Thread.ofVirtual()
              .start(() -> {
                while (true) {
                  long t0 = System.nanoTime();
                  if (t0 >= end) {
                    return;
                  }
                  boolean success;
                  try {
                    op.run(seq.getAndIncrement());
                    success = true;
                  } catch (Exception e) {
                    success = false;
                  }
                  long t1 = System.nanoTime();
                  if (t0 >= measureFrom && t1 <= end) {
                    if (success) {
                      h.record(t1 - t0);
                      ok.incrementAndGet();
                      perTick.incrementAndGet((int) ((t1 - measureFrom) / TICK.toNanos()));
                    } else {
                      errors.incrementAndGet();
                    }
                  }
                }
              }));
    }
    for (Thread t : threads) {
      t.join();
    }
    return new Result(ok.get(), errors.get(), measure.toNanos() / 1e9, h, toArray(perTick), TICK);
  }

  /** Closed-loop async clients: {@code clients} requests always outstanding. */
  static Result closedAsync(int clients, Duration warmup, Duration measure, Op op)
      throws InterruptedException {
    return closed(clients, warmup, measure, s -> op.run(s).join());
  }

  /**
   * An open loop at {@code rate} requests a second: request i is due at start + i / rate, and its
   * latency counts from then. At most {@code maxOutstanding} may be in flight; a request that finds
   * that many waiting counts as an error (the system is overloaded).
   */
  static Result open(double rate, Duration warmup, Duration measure, int maxOutstanding, Op op)
      throws InterruptedException {
    Histogram h = new Histogram();
    AtomicLong ok = new AtomicLong();
    AtomicLong errors = new AtomicLong();
    AtomicLong outstanding = new AtomicLong();
    long start = System.nanoTime();
    long measureFrom = start + warmup.toNanos();
    long end = measureFrom + measure.toNanos();
    int ticks = (int) (measure.toNanos() / TICK.toNanos()) + 1;
    AtomicLongArray perTick = new AtomicLongArray(ticks);
    double interval = 1e9 / rate;
    AtomicBoolean done = new AtomicBoolean();
    for (long i = 0; ; i++) {
      long due = start + (long) (i * interval);
      if (due >= end) {
        break;
      }
      long now;
      while ((now = System.nanoTime()) < due) {
        LockSupport.parkNanos(Math.min(due - now, 200_000));
      }
      boolean measured = due >= measureFrom;
      if (outstanding.get() >= maxOutstanding) {
        if (measured) {
          errors.incrementAndGet();
        }
        continue;
      }
      outstanding.incrementAndGet();
      CompletableFuture<?> f;
      try {
        f = op.run(i);
      } catch (RuntimeException e) {
        f = CompletableFuture.failedFuture(e);
      }
      f.whenComplete((r, t) -> {
        outstanding.decrementAndGet();
        long finished = System.nanoTime();
        if (!measured || done.get()) {
          return;
        }
        if (t == null) {
          h.record(finished - due);
          ok.incrementAndGet();
          int tick = (int) ((finished - measureFrom) / TICK.toNanos());
          if (tick < ticks) {
            perTick.incrementAndGet(tick);
          }
        } else {
          errors.incrementAndGet();
        }
      });
    }
    // Let the last requests finish (up to a few seconds), then stop counting.
    long waitUntil = System.nanoTime() + 5_000_000_000L;
    while (outstanding.get() > 0 && System.nanoTime() < waitUntil) {
      Thread.sleep(5);
    }
    done.set(true);
    errors.addAndGet(outstanding.get());
    return new Result(ok.get(), errors.get(), measure.toNanos() / 1e9, h, toArray(perTick), TICK);
  }

  private static long[] toArray(AtomicLongArray a) {
    long[] out = new long[a.length()];
    for (int i = 0; i < out.length; i++) {
      out[i] = a.get(i);
    }
    return out;
  }
}

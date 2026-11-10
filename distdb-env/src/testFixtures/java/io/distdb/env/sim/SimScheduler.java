package io.distdb.env.sim;

import io.distdb.env.Scheduler;
import java.time.Duration;
import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * A single-threaded {@link Scheduler} driven by the test. Nothing runs until the test calls {@link
 * #runNext()}, {@link #runFor} or {@link #runUntilIdle()}; tasks run in order of due time, ties
 * broken by submission order, and running a delayed task jumps the {@link SimClock} straight to its
 * due time. A test can therefore interleave background work with foreground calls exactly as it
 * likes, and every interleaving is reproducible.
 *
 * <p>Not thread-safe: everything must happen on the test's thread. Exceptions thrown by tasks
 * propagate to the caller of the {@code run} method, so failures are loud.
 */
public final class SimScheduler implements Scheduler {

  private final SimClock clock;
  private final PriorityQueue<Task> queue =
      new PriorityQueue<>(
          Comparator.comparingLong((Task t) -> t.due).thenComparingLong((Task t) -> t.order));
  private long nextOrder;

  public SimScheduler(SimClock clock) {
    this.clock = Objects.requireNonNull(clock);
  }

  /** The clock this scheduler advances. */
  public SimClock clock() {
    return clock;
  }

  @Override
  public void execute(Runnable task) {
    enqueue(clock.nanoTime(), task);
  }

  @Override
  public Cancellable schedule(Duration delay, Runnable task) {
    Task t = enqueue(clock.nanoTime() + Math.max(0, delay.toNanos()), task);
    return () -> {
      if (t.cancelled || t.started) {
        return false;
      }
      t.cancelled = true;
      return true;
    };
  }

  private Task enqueue(long due, Runnable runnable) {
    Task t = new Task(due, nextOrder++, Objects.requireNonNull(runnable));
    queue.add(t);
    return t;
  }

  /**
   * Runs the next task, advancing the clock to its due time if needed.
   *
   * @return {@code false} if no task was pending
   */
  public boolean runNext() {
    Task t;
    while ((t = queue.poll()) != null) {
      if (t.cancelled) {
        continue;
      }
      clock.advanceTo(t.due);
      t.started = true;
      t.runnable.run();
      return true;
    }
    return false;
  }

  /** Runs only tasks already due, without advancing the clock. */
  public int runReady() {
    int ran = 0;
    while (!queue.isEmpty() && queue.peek().due <= clock.nanoTime()) {
      if (runNext()) {
        ran++;
      }
    }
    return ran;
  }

  /** Runs every task due within {@code d} from now, then sets the clock to now + {@code d}. */
  public void runFor(Duration d) {
    long until = clock.nanoTime() + d.toNanos();
    while (!queue.isEmpty() && queue.peek().due <= until) {
      runNext();
    }
    clock.advanceTo(until);
  }

  /**
   * Runs tasks until none are pending.
   *
   * @throws IllegalStateException after {@code maxTasks} tasks, which usually means a task keeps
   *     rescheduling itself
   */
  public int runUntilIdle(int maxTasks) {
    int ran = 0;
    while (runNext()) {
      if (++ran >= maxTasks) {
        throw new IllegalStateException("still busy after " + maxTasks + " tasks");
      }
    }
    return ran;
  }

  /** Runs tasks until none are pending, giving up after a million. */
  public int runUntilIdle() {
    return runUntilIdle(1_000_000);
  }

  /** The number of tasks waiting to run, including cancelled ones not yet discarded. */
  public int pendingTasks() {
    return queue.size();
  }

  private static final class Task {
    final long due;
    final long order;
    final Runnable runnable;
    boolean cancelled;
    boolean started;

    Task(long due, long order, Runnable runnable) {
      this.due = due;
      this.order = order;
      this.runnable = runnable;
    }
  }
}

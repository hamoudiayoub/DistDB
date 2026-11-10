package io.distdb.env;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * The production {@link Scheduler}: every task runs on its own virtual thread, and one platform
 * timer thread hands delayed tasks over when they are due. Tasks may run concurrently.
 */
public final class ExecutorScheduler implements Scheduler, AutoCloseable {

  private static final System.Logger LOG = System.getLogger(ExecutorScheduler.class.getName());

  private final ExecutorService workers;
  private final ScheduledExecutorService timer;
  private final Consumer<Throwable> onUncaught;

  /** A scheduler that logs tasks' uncaught exceptions. */
  public ExecutorScheduler() {
    this(t -> LOG.log(System.Logger.Level.ERROR, "uncaught exception in scheduled task", t));
  }

  /** A scheduler that passes tasks' uncaught exceptions to {@code onUncaught}. */
  public ExecutorScheduler(Consumer<Throwable> onUncaught) {
    this.onUncaught = Objects.requireNonNull(onUncaught);
    this.workers = Executors.newVirtualThreadPerTaskExecutor();
    ScheduledThreadPoolExecutor stpe =
        new ScheduledThreadPoolExecutor(
            1,
            r -> {
              Thread t = new Thread(r, "distdb-timer");
              t.setDaemon(true);
              return t;
            });
    stpe.setRemoveOnCancelPolicy(true);
    stpe.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    this.timer = stpe;
  }

  @Override
  public void execute(Runnable task) {
    Objects.requireNonNull(task);
    workers.execute(() -> runGuarded(task));
  }

  @Override
  public Cancellable schedule(Duration delay, Runnable task) {
    Objects.requireNonNull(task);
    // Set once, by whichever of cancel() and the timer gets there first.
    AtomicBoolean claimed = new AtomicBoolean();
    ScheduledFuture<?> future =
        timer.schedule(
            () -> {
              if (claimed.compareAndSet(false, true)) {
                execute(task);
              }
            },
            Math.max(0, delay.toNanos()),
            TimeUnit.NANOSECONDS);
    return () -> {
      if (claimed.compareAndSet(false, true)) {
        future.cancel(false);
        return true;
      }
      return false;
    };
  }

  private void runGuarded(Runnable task) {
    try {
      task.run();
    } catch (Throwable t) {
      onUncaught.accept(t);
    }
  }

  /** Drops pending timers and waits for running tasks to finish. */
  @Override
  public void close() {
    timer.shutdownNow();
    workers.close();
  }
}

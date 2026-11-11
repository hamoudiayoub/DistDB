package io.distdb.sim;

import io.distdb.env.Scheduler;
import java.time.Duration;
import java.util.Objects;

/**
 * One life of a simulated process on the shared scheduler. When the process dies, the timers it
 * left behind (election ticks, background flushes, sweepers) must die with it, as its threads
 * would; a restarted process gets a new {@code Life}.
 */
public final class Life implements Scheduler {

  private final Scheduler shared;
  private boolean alive = true;
  private long stretch = 1;

  public Life(Scheduler shared) {
    this.shared = Objects.requireNonNull(shared);
  }

  /** The process died: drop every task it scheduled, now and later. */
  public void end() {
    alive = false;
  }

  public boolean alive() {
    return alive;
  }

  /**
   * Makes this process's timers run {@code factor} times slower (1 is normal): a stalled process,
   * as a long GC pause or an overloaded machine makes one. Its sense of elapsed time falls behind
   * everyone else's.
   */
  public void stall(long factor) {
    if (factor < 1) {
      throw new IllegalArgumentException("factor must be at least 1: " + factor);
    }
    stretch = factor;
  }

  @Override
  public void execute(Runnable task) {
    shared.execute(() -> runIfAlive(task));
  }

  @Override
  public Cancellable schedule(Duration delay, Runnable task) {
    return shared.schedule(delay.multipliedBy(stretch), () -> runIfAlive(task));
  }

  private void runIfAlive(Runnable task) {
    if (alive) {
      task.run();
    }
  }
}

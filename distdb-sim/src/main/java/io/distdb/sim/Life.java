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

  @Override
  public void execute(Runnable task) {
    shared.execute(() -> runIfAlive(task));
  }

  @Override
  public Cancellable schedule(Duration delay, Runnable task) {
    return shared.schedule(delay, () -> runIfAlive(task));
  }

  private void runIfAlive(Runnable task) {
    if (alive) {
      task.run();
    }
  }
}

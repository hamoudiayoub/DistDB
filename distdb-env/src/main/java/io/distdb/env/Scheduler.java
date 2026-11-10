package io.distdb.env;

import java.time.Duration;

/**
 * The only way core logic runs work later or elsewhere. Core code never calls {@code Thread.sleep},
 * starts threads or creates executors; it hands tasks to a {@code Scheduler}, which the simulator
 * replaces with a single-threaded event loop driven by virtual time.
 *
 * <p>The production implementation may run tasks concurrently and in any order. Code that needs
 * tasks serialized (for example everything touching one Raft group) must say so explicitly by
 * funnelling them through its own queue, so that the same code is correct both in production and
 * under the simulator's one-at-a-time execution.
 */
public interface Scheduler {

  /** Runs {@code task} as soon as possible. */
  void execute(Runnable task);

  /**
   * Runs {@code task} once, no earlier than {@code delay} from now as measured by this scheduler's
   * clock.
   */
  Cancellable schedule(Duration delay, Runnable task);

  /** A handle to a scheduled task. */
  @FunctionalInterface
  interface Cancellable {
    /**
     * Prevents the task from running if it has not started yet.
     *
     * @return {@code true} if this call stopped the task from running
     */
    boolean cancel();
  }
}

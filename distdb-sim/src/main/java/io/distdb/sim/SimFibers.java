package io.distdb.sim;

import io.distdb.sql.kv.Awaiter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;

/**
 * Blocking code (the SQL layer) inside the deterministic simulation, many at once. Each fiber is a
 * thread that runs only while the simulation's thread waits for it, so exactly one thread runs at
 * any moment and every run replays from its seed.
 *
 * <p>A fiber that waits for a future ({@link #awaiter()}) gives control back; when the future
 * completes, a simulation task hands control to the fiber again, and waits until it waits again or
 * ends. Fibers' statements thus interleave like concurrent clients', at every wait, with no
 * nesting: unlike waiting by running the world from inside a task, a fiber whose answer has come
 * never waits for another fiber's.
 */
final class SimFibers {

  private final java.util.function.BiConsumer<Duration, Runnable> scheduler;
  private final Runnable pump;
  private final List<Fiber> fibers = new ArrayList<>();
  private final ThreadLocal<Fiber> current = new ThreadLocal<>();
  private Throwable failure;

  /**
   * @param pump runs the world a little, for waits outside any fiber
   */
  SimFibers(java.util.function.BiConsumer<Duration, Runnable> scheduler, Runnable pump) {
    this.scheduler = scheduler;
    this.pump = pump;
  }

  private final class Fiber {
    final Semaphore run = new Semaphore(0);
    final Semaphore back = new Semaphore(0);
    final Thread thread;
    volatile boolean done;

    Fiber(String name, Runnable body) {
      thread =
          Thread.ofPlatform()
              .name(name)
              .daemon()
              .unstarted(() -> {
                current.set(this);
                run.acquireUninterruptibly();
                try {
                  body.run();
                } catch (Throwable t) {
                  if (failure == null) {
                    failure = t;
                  }
                } finally {
                  done = true;
                  back.release();
                }
              });
    }

    /** On the simulation's thread: lets the fiber run until it waits or ends. */
    void resume() {
      run.release();
      back.acquireUninterruptibly();
      if (failure != null) {
        Throwable t = failure;
        failure = null;
        if (t instanceof RuntimeException r) {
          throw r;
        }
        if (t instanceof Error e) {
          throw e;
        }
        throw new IllegalStateException(t);
      }
    }

    /** On the fiber's thread: gives control back until resumed. */
    void yieldUntilResumed() {
      back.release();
      run.acquireUninterruptibly();
    }
  }

  /** Starts {@code body} as a fiber after {@code delay}. */
  void spawn(String name, Duration delay, Runnable body) {
    Fiber f = new Fiber(name, body);
    fibers.add(f);
    f.thread.start();
    scheduler.accept(delay, f::resume);
  }

  /** The names of the fibers still running. */
  List<String> running() {
    List<String> out = new ArrayList<>();
    for (Fiber f : fibers) {
      if (!f.done) {
        out.add(f.thread.getName());
      }
    }
    return out;
  }

  /** Whether every fiber has ended. */
  boolean allDone() {
    for (Fiber f : fibers) {
      if (!f.done) {
        return false;
      }
    }
    return true;
  }

  /** In a fiber, waits by giving control back for {@code d}; elsewhere, runs the world. */
  void sleep(Duration d) {
    CompletableFuture<Void> f = new CompletableFuture<>();
    scheduler.accept(d, () -> f.complete(null));
    awaiter().await(f);
  }

  /** Waits for futures: a fiber yields, other code runs the world until they complete. */
  Awaiter awaiter() {
    return new Awaiter() {
      @Override
      public <T> T await(CompletableFuture<T> f) {
        Fiber me = current.get();
        if (me == null) {
          for (int i = 0; i < 600_000 && !f.isDone(); i++) {
            pump.run();
          }
        } else if (!f.isDone()) {
          f.whenComplete((v, t) -> scheduler.accept(Duration.ZERO, me::resume));
          me.yieldUntilResumed();
        }
        if (!f.isDone()) {
          throw new AssertionError("a future never completed");
        }
        try {
          return f.join();
        } catch (CompletionException e) {
          throw e.getCause() instanceof RuntimeException r ? r : e;
        }
      }
    };
  }
}

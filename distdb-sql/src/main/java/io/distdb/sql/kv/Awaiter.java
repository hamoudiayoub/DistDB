package io.distdb.sql.kv;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * How the synchronous SQL code waits for the asynchronous transaction client. In a server each
 * connection has its own (virtual) thread, and waiting is {@link #blocking()}. In the simulator,
 * waiting runs the simulation until the future completes, so SQL executes inside the deterministic
 * world, on its one thread.
 */
@FunctionalInterface
public interface Awaiter {

  /** Waits for {@code future}; rethrows its failure (unwrapped from a CompletionException). */
  <T> T await(CompletableFuture<T> future);

  static Awaiter blocking() {
    return new Awaiter() {
      @Override
      public <T> T await(CompletableFuture<T> future) {
        try {
          return future.join();
        } catch (CompletionException e) {
          if (e.getCause() instanceof RuntimeException r) {
            throw r;
          }
          throw e;
        }
      }
    };
  }
}

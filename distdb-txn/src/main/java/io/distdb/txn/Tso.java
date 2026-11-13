package io.distdb.txn;

import io.distdb.env.Clock;
import io.distdb.env.Scheduler;
import io.distdb.rpc.PdClient;
import io.distdb.rpc.proto.PdRequest;
import io.distdb.rpc.proto.TsoRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Timestamps from the placement driver's oracle, batched: while one request is in flight, every
 * caller that arrives waits for the next one, which asks for as many timestamps as there are
 * callers. Thread-safe.
 */
final class Tso {

  private final PdClient pd;
  private final Clock clock;
  private final Scheduler scheduler;
  private final Duration timeout;
  private final List<Waiter> waiting = new ArrayList<>(); // guarded by this
  private boolean inFlight; // guarded by this

  private record Waiter(CompletableFuture<Long> future, long deadline) {}

  Tso(PdClient pd, Clock clock, Scheduler scheduler, Duration timeout) {
    this.pd = Objects.requireNonNull(pd);
    this.clock = Objects.requireNonNull(clock);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.timeout = Objects.requireNonNull(timeout);
  }

  /** The next timestamp: above every timestamp issued before this call. */
  CompletableFuture<Long> next() {
    CompletableFuture<Long> f = new CompletableFuture<>();
    synchronized (this) {
      waiting.add(new Waiter(f, clock.nanoTime() + timeout.toNanos()));
      if (inFlight) {
        return f;
      }
      inFlight = true;
    }
    fire();
    return f;
  }

  private void fire() {
    List<Waiter> batch;
    synchronized (this) {
      long now = clock.nanoTime();
      waiting.removeIf(w -> {
        if (now >= w.deadline()) {
          w.future()
              .completeExceptionally(
                  new TxnException(TxnException.Code.FAILED, "no timestamp in time", null));
          return true;
        }
        return false;
      });
      if (waiting.isEmpty()) {
        inFlight = false;
        return;
      }
      batch = new ArrayList<>(waiting);
      waiting.clear();
    }
    PdRequest request =
        PdRequest.newBuilder().setTso(TsoRequest.newBuilder().setCount(batch.size())).build();
    pd.call(request, Duration.ofMillis(500))
        .whenComplete((response, failure) -> {
          if (response != null
              && response.hasTso()
              && response.getTso().getCount() >= batch.size()) {
            long first = response.getTso().getFirst();
            for (int i = 0; i < batch.size(); i++) {
              batch.get(i).future().complete(first + i);
            }
            next(batch, Duration.ZERO);
          } else {
            next(batch, Duration.ofMillis(20)); // no leader, or busy: ask again shortly
          }
        });
  }

  /** Puts {@code unserved} back in front, and fires again after {@code delay}. */
  private void next(List<Waiter> unserved, Duration delay) {
    synchronized (this) {
      List<Waiter> again = new ArrayList<>();
      for (Waiter w : unserved) {
        if (!w.future().isDone()) {
          again.add(w);
        }
      }
      waiting.addAll(0, again);
    }
    if (delay.isZero()) {
      fire();
    } else {
      scheduler.schedule(delay, this::fire);
    }
  }
}

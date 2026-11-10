package io.distdb.env;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ExecutorSchedulerTest {

  @Test
  void executeRunsTheTask() throws InterruptedException {
    try (ExecutorScheduler scheduler = new ExecutorScheduler()) {
      CountDownLatch ran = new CountDownLatch(1);
      scheduler.execute(ran::countDown);
      assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void scheduleWaitsAtLeastTheDelay() throws InterruptedException {
    try (ExecutorScheduler scheduler = new ExecutorScheduler()) {
      CountDownLatch ran = new CountDownLatch(1);
      long start = System.nanoTime();
      AtomicReference<Long> ranAt = new AtomicReference<>();
      scheduler.schedule(
          Duration.ofMillis(50),
          () -> {
            ranAt.set(System.nanoTime());
            ran.countDown();
          });
      assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(ranAt.get() - start).isGreaterThanOrEqualTo(Duration.ofMillis(50).toNanos());
    }
  }

  @Test
  void cancelBeforeDueStopsTheTask() throws InterruptedException {
    try (ExecutorScheduler scheduler = new ExecutorScheduler()) {
      AtomicBoolean ran = new AtomicBoolean();
      Scheduler.Cancellable handle =
          scheduler.schedule(Duration.ofMillis(200), () -> ran.set(true));
      assertThat(handle.cancel()).isTrue();
      assertThat(handle.cancel()).isFalse();
      // A later task on the same timer thread proves the cancelled one's due time has passed.
      CountDownLatch later = new CountDownLatch(1);
      scheduler.schedule(Duration.ofMillis(300), later::countDown);
      assertThat(later.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(ran).isFalse();
    }
  }

  @Test
  void cancelAfterRunReportsFalse() throws InterruptedException {
    try (ExecutorScheduler scheduler = new ExecutorScheduler()) {
      CountDownLatch ran = new CountDownLatch(1);
      Scheduler.Cancellable handle = scheduler.schedule(Duration.ZERO, ran::countDown);
      assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(handle.cancel()).isFalse();
    }
  }

  @Test
  void uncaughtExceptionsReachTheHandler() throws InterruptedException {
    CountDownLatch reported = new CountDownLatch(1);
    AtomicReference<Throwable> seen = new AtomicReference<>();
    try (ExecutorScheduler scheduler =
        new ExecutorScheduler(t -> {
          seen.set(t);
          reported.countDown();
        })) {
      scheduler.execute(() -> {
        throw new IllegalStateException("boom");
      });
      assertThat(reported.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(seen.get()).isInstanceOf(IllegalStateException.class).hasMessage("boom");
    }
  }
}

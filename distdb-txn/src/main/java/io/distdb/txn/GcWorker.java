package io.distdb.txn;

import com.google.protobuf.ByteString;
import io.distdb.env.Scheduler;
import io.distdb.rpc.PdClient;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.LockInfo;
import io.distdb.rpc.proto.PdRequest;
import io.distdb.rpc.proto.ScanLocksRequest;
import io.distdb.rpc.proto.SetGcSafePointRequest;
import io.distdb.rpc.proto.SetTxnFloorRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

/**
 * Moves the cluster's GC safe point forward, so old versions can be dropped, without ever making a
 * transaction's outcome unknowable. Each round, with {@code G = now − lifetime}:
 *
 * <ol>
 *   <li><b>Floor.</b> Walk every range in key order and tell it to refuse prewrites below {@code
 *       G}. After the walk, no new lock below {@code G} can appear anywhere (a range split since
 *       inherits the floor).
 *   <li><b>Resolve.</b> Walk every range again for locks below {@code G}, and resolve them. Their
 *       transactions outlived the lifetime: those that did not commit are rolled back.
 *   <li><b>Safe point.</b> Give {@code G} to the placement driver, which passes it on to the
 *       stores, whose ranges drop the versions no read at {@code G} or later can see.
 * </ol>
 *
 * Without the first two steps, GC could delete a primary's commit record while a secondary's lock
 * still points at it; a reader would then take a committed transaction for a dead one and roll half
 * of it back.
 */
public final class GcWorker {

  private final TxnClient txn;
  private final PdClient pd;
  private final Scheduler scheduler;
  private final Duration lifetime;
  private final Duration interval;
  private final BooleanSupplier active;
  private volatile boolean stopped;
  private volatile long lastSafePoint;
  private long rounds;

  /**
   * @param lifetime how long a version stays readable after a newer one replaced it; transactions
   *     older than this fail with {@code TOO_OLD}
   * @param interval how often to run a round
   * @param active whether this process should run rounds now (on the PD: whether it leads)
   */
  public GcWorker(
      TxnClient txn,
      PdClient pd,
      Scheduler scheduler,
      Duration lifetime,
      Duration interval,
      BooleanSupplier active) {
    this.txn = Objects.requireNonNull(txn);
    this.pd = Objects.requireNonNull(pd);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.lifetime = Objects.requireNonNull(lifetime);
    this.interval = Objects.requireNonNull(interval);
    this.active = Objects.requireNonNull(active);
  }

  /** Runs a round every interval until {@link #stop()}. */
  public void start() {
    scheduler.schedule(interval, this::tick);
  }

  public void stop() {
    stopped = true;
  }

  /** The last safe point this worker set, or 0. */
  public long lastSafePoint() {
    return lastSafePoint;
  }

  /** How many rounds finished. */
  public synchronized long rounds() {
    return rounds;
  }

  private void tick() {
    if (stopped) {
      return;
    }
    CompletableFuture<?> round =
        active.getAsBoolean() ? runOnce() : CompletableFuture.completedFuture(null);
    round.whenComplete((v, t) -> {
      if (!stopped) {
        scheduler.schedule(interval, this::tick);
      }
    });
  }

  /** One round; completes with the new safe point (or fails, changing nothing that matters). */
  public CompletableFuture<Long> runOnce() {
    return txn.timestamp()
        .thenCompose(now -> {
          long g = now - (lifetime.toMillis() << 18);
          if (g <= lastSafePoint) {
            return CompletableFuture.completedFuture(lastSafePoint);
          }
          return floor(new byte[0], g)
              .thenCompose(v -> resolve(new byte[0], g))
              .thenCompose(v ->
                  pd.call(
                      PdRequest.newBuilder()
                          .setSetGcSafePoint(SetGcSafePointRequest.newBuilder().setSafePoint(g))
                          .build(),
                      Duration.ofSeconds(2)))
              .thenApply(r -> {
                long safePoint = r.getSetGcSafePoint().getSafePoint();
                synchronized (this) {
                  lastSafePoint = Math.max(lastSafePoint, safePoint);
                  rounds++;
                }
                return safePoint;
              });
        });
  }

  /** Step 1, from {@code key} to the end of the key space, one range at a time. */
  private CompletableFuture<Void> floor(byte[] key, long g) {
    KvRequest request =
        KvRequest.newBuilder()
            .setSetTxnFloor(
                SetTxnFloorRequest.newBuilder().setKey(ByteString.copyFrom(key)).setFloor(g))
            .build();
    return txn.send(request)
        .thenCompose(r -> {
          if (!r.hasSetTxnFloor()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("floor not set: " + r.getError().getMessage()));
          }
          if (!r.getSetTxnFloor().hasRangeEnd()) {
            return CompletableFuture.completedFuture(null);
          }
          return floor(r.getSetTxnFloor().getRangeEnd().toByteArray(), g);
        });
  }

  /** Step 2, from {@code key} to the end of the key space. */
  private CompletableFuture<Void> resolve(byte[] key, long g) {
    KvRequest request =
        KvRequest.newBuilder()
            .setScanLocks(
                ScanLocksRequest.newBuilder()
                    .setStartKey(ByteString.copyFrom(key))
                    .setMaxTs(g)
                    .setLimit(256))
            .build();
    return txn.send(request)
        .thenCompose(r -> {
          if (!r.hasScanLocks()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("locks not scanned: " + r.getError().getMessage()));
          }
          List<CompletableFuture<Boolean>> resolving = new ArrayList<>();
          for (LockInfo lock : r.getScanLocks().getLocksList()) {
            resolving.add(resolveFully(lock, 0));
          }
          return CompletableFuture.allOf(resolving.toArray(CompletableFuture[]::new))
              .thenCompose(v -> next(r, g));
        });
  }

  private CompletableFuture<Void> next(KvResponse r, long g) {
    if (!r.getScanLocks().hasResumeKey()) {
      return CompletableFuture.completedFuture(null);
    }
    return resolve(r.getScanLocks().getResumeKey().toByteArray(), g);
  }

  /**
   * Resolves {@code lock} until it is gone. Its primary's range has a floor above it, so the
   * primary answers committed or rolled back, never "still running"; a retry covers a primary in a
   * range that had not applied its floor yet.
   */
  private CompletableFuture<Boolean> resolveFully(LockInfo lock, int attempt) {
    return txn.resolve(lock)
        .thenCompose(gone -> {
          if (gone || attempt >= 20) {
            return gone
                ? CompletableFuture.completedFuture(true)
                : CompletableFuture.<Boolean>failedFuture(
                    new IllegalStateException("a lock below the safe point would not go"));
          }
          return txn.later(Duration.ofMillis(50)).thenCompose(v -> resolveFully(lock, attempt + 1));
        });
  }
}

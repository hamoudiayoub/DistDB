package io.distdb.txn;

import com.google.protobuf.ByteString;
import io.distdb.client.KvClient;
import io.distdb.env.Clock;
import io.distdb.env.RandomSource;
import io.distdb.env.Scheduler;
import io.distdb.rpc.PdClient;
import io.distdb.rpc.proto.CheckTxnStatusRequest;
import io.distdb.rpc.proto.CheckTxnStatusResponse;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.LockInfo;
import io.distdb.rpc.proto.TxnCommitRequest;
import io.distdb.rpc.proto.TxnRollbackRequest;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * Transactions with snapshot isolation over a sharded cluster, after Google's Percolator.
 *
 * <p>{@link #begin()} takes a start timestamp from the placement driver's oracle. A {@link Txn}
 * reads as of that timestamp, buffers its writes, and on {@link Txn#commit()} prewrites every key
 * (locks naming the primary key), takes a commit timestamp, commits the primary (the commit point),
 * then the rest. There is no coordinator: a reader that meets a lock asks the primary's range how
 * the transaction ended, and finishes the job ({@link #resolve(LockInfo)}).
 *
 * <p>Thread-safe; non-blocking: every call returns a future.
 */
public final class TxnClient {

  /** Where a test makes a client stop, as if it had died there. */
  public enum CrashPoint {
    NONE,
    /** Every key prewritten (locked), nothing committed. */
    AFTER_PREWRITE,
    /** The primary committed (the transaction is committed), the secondaries still locked. */
    AFTER_PRIMARY
  }

  /** Decides, per transaction, whether its client dies during commit; for tests. */
  @FunctionalInterface
  public interface Crash {
    CrashPoint at(Txn txn);
  }

  /**
   * How transactions behave.
   *
   * @param lockTtl how long a lock protects an unfinished transaction from readers
   * @param timeout the deadline for each request (a read, a prewrite, a commit)
   * @param crash where to stop on purpose, for tests ({@code t -> NONE} otherwise)
   */
  public record Options(Duration lockTtl, Duration timeout, Crash crash) {
    public Options {
      Objects.requireNonNull(lockTtl);
      Objects.requireNonNull(timeout);
      Objects.requireNonNull(crash);
    }

    /** Locks live 3 s, requests wait up to 10 s, nobody crashes. */
    public static Options defaults() {
      return new Options(Duration.ofSeconds(3), Duration.ofSeconds(10), t -> CrashPoint.NONE);
    }
  }

  private final KvClient kv;
  private final Tso tso;
  private final Clock clock;
  private final Scheduler scheduler;
  private final RandomSource random; // guarded by itself
  private final Options options;

  /**
   * @param kv a client routed through the placement driver ({@link KvClient#KvClient(PdClient,
   *     Function, Clock, Scheduler, RandomSource, io.distdb.client.ClientOptions)})
   * @param pd the placement driver, for timestamps
   */
  public TxnClient(
      KvClient kv,
      PdClient pd,
      Clock clock,
      Scheduler scheduler,
      RandomSource random,
      Options options) {
    this.kv = Objects.requireNonNull(kv);
    this.clock = Objects.requireNonNull(clock);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.random = Objects.requireNonNull(random);
    this.options = Objects.requireNonNull(options);
    this.tso = new Tso(pd, clock, scheduler, options.timeout());
  }

  /** Starts a transaction: it will see every transaction that committed before this call. */
  public CompletableFuture<Txn> begin() {
    return tso.next().thenApply(ts -> new Txn(this, ts));
  }

  /** A fresh timestamp from the oracle. */
  public CompletableFuture<Long> timestamp() {
    return tso.next();
  }

  /**
   * Runs {@code body} in a transaction and commits it, starting again (after a backoff) when it
   * fails in a way a retry may fix: a conflict, an abort, or a too-old start. At most {@code
   * attempts} times. The body must not have effects outside the transaction.
   */
  public <T> CompletableFuture<T> run(Function<Txn, CompletableFuture<T>> body, int attempts) {
    return begin()
        .thenCompose(
            txn -> body.apply(txn).thenCompose(value -> txn.commit().thenApply(ts -> value)))
        .handle((value, failure) -> {
          if (failure == null) {
            return CompletableFuture.completedFuture(value);
          }
          Throwable cause = unwrap(failure);
          if (cause instanceof TxnException e && e.retryable() && attempts > 1) {
            return later(backoff()).thenCompose(v -> run(body, attempts - 1));
          }
          return CompletableFuture.<T>failedFuture(cause);
        })
        .thenCompose(f -> f);
  }

  /**
   * Resolves a lock that stands in a reader's way: asks the transaction's primary how it ended
   * (rolling it back there if its TTL ran out), then commits or rolls back this lock to match.
   * Completes when that is done, or at once if the owner is still alive (the caller backs off and
   * reads again).
   *
   * @return whether the lock is gone
   */
  CompletableFuture<Boolean> resolve(LockInfo lock) {
    return timestamp()
        .thenCompose(now ->
            send(
                KvRequest.newBuilder()
                    .setCheckTxnStatus(
                        CheckTxnStatusRequest.newBuilder()
                            .setPrimary(lock.getPrimary())
                            .setStartTs(lock.getStartTs())
                            .setCurrentTs(now))
                    .build()))
        .thenCompose(response -> {
          if (!response.hasCheckTxnStatus()) {
            return CompletableFuture.completedFuture(false);
          }
          CheckTxnStatusResponse status = response.getCheckTxnStatus();
          boolean isPrimary = lock.getKey().equals(lock.getPrimary());
          return switch (status.getStatus()) {
            case LOCKED -> CompletableFuture.completedFuture(false);
            case COMMITTED ->
                isPrimary
                    ? CompletableFuture.completedFuture(true)
                    : send(commitRequest(lock.getKey(), lock.getStartTs(), status.getCommitTs()))
                        .thenApply(r -> true); // roll forward
            case ROLLED_BACK ->
                isPrimary
                    ? CompletableFuture.completedFuture(true)
                    : send(rollbackRequest(lock.getKey(), lock.getStartTs())).thenApply(r -> true);
            case UNRECOGNIZED -> CompletableFuture.completedFuture(false);
          };
        });
  }

  // -----------------------------------------------------------------------------------------------
  // For Txn

  CompletableFuture<KvResponse> send(KvRequest request) {
    return kv.send(request, options.timeout());
  }

  Options options() {
    return options;
  }

  CompletableFuture<Void> later(Duration delay) {
    CompletableFuture<Void> f = new CompletableFuture<>();
    scheduler.schedule(delay, () -> f.complete(null));
    return f;
  }

  Duration backoff() {
    synchronized (random) {
      return Duration.ofMillis(random.nextInt(10, 60));
    }
  }

  Clock clock() {
    return clock;
  }

  static KvRequest commitRequest(ByteString key, long startTs, long commitTs) {
    return KvRequest.newBuilder()
        .setTxnCommit(
            TxnCommitRequest.newBuilder().setKey(key).setStartTs(startTs).setCommitTs(commitTs))
        .build();
  }

  static KvRequest rollbackRequest(ByteString key, long startTs) {
    return KvRequest.newBuilder()
        .setTxnRollback(TxnRollbackRequest.newBuilder().setKey(key).setStartTs(startTs))
        .build();
  }

  static Throwable unwrap(Throwable t) {
    return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
  }
}

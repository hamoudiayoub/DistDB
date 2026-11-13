package io.distdb.txn;

import com.google.protobuf.ByteString;
import io.distdb.common.Bytes;
import io.distdb.rpc.proto.Error;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.Mutation;
import io.distdb.rpc.proto.PrewriteRequest;
import io.distdb.rpc.proto.TxnEntry;
import io.distdb.rpc.proto.TxnGetRequest;
import io.distdb.rpc.proto.TxnScanRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * One transaction: reads as of its start timestamp, writes buffered until {@link #commit()}.
 * Snapshot isolation: every read sees the same consistent snapshot (plus this transaction's own
 * writes), and the commit fails if another transaction committed a write to a key this one writes
 * after this one started (first committer wins). Not thread-safe; use it from one task at a time.
 */
public final class Txn {

  /** One key and value read by a scan. */
  public record Entry(byte[] key, byte[] value) {}

  private final TxnClient client;
  private final long startTs;
  private final TreeMap<byte[], Mutation> writes = new TreeMap<>(Arrays::compareUnsigned);
  private boolean finished;

  Txn(TxnClient client, long startTs) {
    this.client = client;
    this.startTs = startTs;
  }

  public long startTs() {
    return startTs;
  }

  /** The value of {@code key} in this transaction's snapshot (its own writes included). */
  public CompletableFuture<Optional<byte[]>> get(byte[] key) {
    Mutation buffered = writes.get(key);
    if (buffered != null) {
      return CompletableFuture.completedFuture(
          buffered.getOp() == Mutation.Op.PUT
              ? Optional.of(buffered.getValue().toByteArray())
              : Optional.empty());
    }
    KvRequest request =
        KvRequest.newBuilder()
            .setTxnGet(
                TxnGetRequest.newBuilder().setKey(ByteString.copyFrom(key)).setReadTs(startTs))
            .build();
    return read(request, deadline())
        .thenApply(r ->
            r.getTxnGet().getFound()
                ? Optional.of(r.getTxnGet().getValue().toByteArray())
                : Optional.<byte[]>empty());
  }

  /**
   * Up to {@code limit} entries of {@code [start, end)} in this transaction's snapshot, in key
   * order, with its own writes applied; a null {@code end} means no bound.
   */
  public CompletableFuture<List<Entry>> scan(byte[] start, byte[] end, int limit) {
    TreeMap<byte[], byte[]> out = new TreeMap<>(Arrays::compareUnsigned);
    // Our own deletes can hide at most one stored entry each, so this many stored entries are
    // enough for `limit` results; the scan stops there.
    int own = 0;
    for (byte[] k : writes.keySet()) {
      if (Arrays.compareUnsigned(k, start) >= 0
          && (end == null || Arrays.compareUnsigned(k, end) < 0)) {
        own++;
      }
    }
    int need =
        limit <= 0 ? Integer.MAX_VALUE : (int) Math.min(Integer.MAX_VALUE, (long) limit + own);
    return scanFrom(start, end, out, need, deadline())
        .thenApply(complete -> {
          // Overlay our own writes, within the part of the span the stored entries covered: when
          // the scan stopped early, own writes past its last key would skip stored keys before
          // them.
          byte[] covered = complete || out.isEmpty() ? end : Bytes.successor(out.lastKey());
          for (Map.Entry<byte[], Mutation> w : writes.entrySet()) {
            byte[] k = w.getKey();
            boolean inside =
                Arrays.compareUnsigned(k, start) >= 0
                    && (covered == null || Arrays.compareUnsigned(k, covered) < 0);
            if (inside) {
              if (w.getValue().getOp() == Mutation.Op.PUT) {
                out.put(k, w.getValue().getValue().toByteArray());
              } else {
                out.remove(k);
              }
            }
          }
          List<Entry> entries = new ArrayList<>();
          for (Map.Entry<byte[], byte[]> e : out.entrySet()) {
            if (entries.size() == limit) {
              break;
            }
            entries.add(new Entry(e.getKey(), e.getValue()));
          }
          return entries;
        });
  }

  /**
   * Reads stored entries of {@code [start, end)} into {@code out} until there are {@code need} of
   * them; completes with whether it read the whole span.
   */
  private CompletableFuture<Boolean> scanFrom(
      byte[] start, byte[] end, TreeMap<byte[], byte[]> out, int need, long deadline) {
    TxnScanRequest.Builder scan =
        TxnScanRequest.newBuilder().setStartKey(ByteString.copyFrom(start)).setReadTs(startTs);
    if (end != null) {
      scan.setEndKey(ByteString.copyFrom(end));
    }
    if (need != Integer.MAX_VALUE) {
      scan.setLimit(need - out.size());
    }
    return read(KvRequest.newBuilder().setTxnScan(scan).build(), deadline)
        .thenCompose(r -> {
          for (TxnEntry e : r.getTxnScan().getEntriesList()) {
            out.put(e.getKey().toByteArray(), e.getValue().toByteArray());
          }
          if (!r.getTxnScan().hasResumeKey()) {
            return CompletableFuture.completedFuture(true);
          }
          if (out.size() >= need) {
            return CompletableFuture.completedFuture(false);
          }
          return scanFrom(r.getTxnScan().getResumeKey().toByteArray(), end, out, need, deadline);
        });
  }

  /** Sets {@code key} to {@code value} when this transaction commits. */
  public void put(byte[] key, byte[] value) {
    checkOpen();
    writes.put(
        key.clone(),
        Mutation.newBuilder()
            .setOp(Mutation.Op.PUT)
            .setKey(ByteString.copyFrom(key))
            .setValue(ByteString.copyFrom(value))
            .build());
  }

  /** Removes {@code key} when this transaction commits. */
  public void delete(byte[] key) {
    checkOpen();
    writes.put(
        key.clone(),
        Mutation.newBuilder().setOp(Mutation.Op.DELETE).setKey(ByteString.copyFrom(key)).build());
  }

  /**
   * Commits: prewrites every key, takes a commit timestamp, commits the primary (the smallest key:
   * that single write is the commit point), then the others in the background. Completes with the
   * commit timestamp (the start timestamp for a read-only transaction), or fails with a {@link
   * TxnException}: {@code CONFLICT}, {@code ABORTED} and {@code TOO_OLD} changed nothing; {@code
   * UNKNOWN} may have committed.
   */
  public CompletableFuture<Long> commit() {
    checkOpen();
    finished = true;
    if (writes.isEmpty()) {
      return CompletableFuture.completedFuture(startTs);
    }
    TxnClient.CrashPoint crash = client.options().crash().at(this);
    ByteString primary = writes.firstEntry().getValue().getKey();
    long deadline = deadline();
    List<CompletableFuture<Void>> prewrites = new ArrayList<>();
    for (Mutation m : writes.values()) {
      prewrites.add(prewrite(m, primary, deadline));
    }
    return CompletableFuture.allOf(prewrites.toArray(CompletableFuture[]::new))
        .handle((v, failure) -> {
          if (failure != null) {
            rollBackAll(); // best effort: readers would clean up after the TTL anyway
            return CompletableFuture.<Long>failedFuture(TxnClient.unwrap(failure));
          }
          if (crash == TxnClient.CrashPoint.AFTER_PREWRITE) {
            return CompletableFuture.<Long>failedFuture(abandoned("after prewriting"));
          }
          return client
              .timestamp()
              .thenCompose(commitTs -> commitPrimary(primary, commitTs, crash));
        })
        .thenCompose(f -> f);
  }

  /** Abandons the transaction: removes its locks, if any were taken. */
  public CompletableFuture<Void> rollback() {
    finished = true;
    rollBackAll();
    return CompletableFuture.completedFuture(null);
  }

  // -----------------------------------------------------------------------------------------------

  private CompletableFuture<Void> prewrite(Mutation m, ByteString primary, long deadline) {
    KvRequest request =
        KvRequest.newBuilder()
            .setPrewrite(
                PrewriteRequest.newBuilder()
                    .setMutation(m)
                    .setPrimary(primary)
                    .setStartTs(startTs)
                    .setTtlMs(client.options().lockTtl().toMillis()))
            .build();
    return client
        .send(request)
        .handle((response, failure) -> {
          if (failure != null) {
            return CompletableFuture.<Void>failedFuture(
                new TxnException(
                    TxnException.Code.FAILED, "prewrite failed", TxnClient.unwrap(failure)));
          }
          if (response.hasPrewrite()) {
            return CompletableFuture.<Void>completedFuture(null);
          }
          Error error = response.getError();
          return switch (error.getKindCase()) {
            case KEY_LOCKED ->
                // Someone else's lock. If its owner is dead, clean it up and try again; if it is
                // alive, give up rather than wait (waiting on each other's locks would deadlock).
                client
                    .resolve(error.getKeyLocked().getLock())
                    .thenCompose(gone -> {
                      if (gone && client.clock().nanoTime() < deadline) {
                        return prewrite(m, primary, deadline);
                      }
                      return CompletableFuture.<Void>failedFuture(
                          new TxnException(
                              TxnException.Code.CONFLICT,
                              "key locked: " + error.getMessage(),
                              null));
                    });
            case WRITE_CONFLICT ->
                CompletableFuture.<Void>failedFuture(
                    new TxnException(TxnException.Code.CONFLICT, error.getMessage(), null));
            case TXN_ABORTED ->
                CompletableFuture.<Void>failedFuture(
                    new TxnException(TxnException.Code.ABORTED, error.getMessage(), null));
            case TXN_TOO_OLD ->
                CompletableFuture.<Void>failedFuture(
                    new TxnException(TxnException.Code.TOO_OLD, error.getMessage(), null));
            default ->
                CompletableFuture.<Void>failedFuture(
                    new TxnException(
                        TxnException.Code.FAILED, "prewrite: " + error.getMessage(), null));
          };
        })
        .thenCompose(f -> f);
  }

  private CompletableFuture<Long> commitPrimary(
      ByteString primary, long commitTs, TxnClient.CrashPoint crash) {
    return client
        .send(TxnClient.commitRequest(primary, startTs, commitTs))
        .handle((response, failure) -> {
          if (failure != null) {
            // The commit point may or may not have been reached. Readers will settle it either
            // way (roll our locks forward or back), so all of it happens or none of it does.
            throw new TxnException(
                TxnException.Code.UNKNOWN,
                "the primary's commit got no answer; the transaction may have committed",
                TxnClient.unwrap(failure));
          }
          if (response.hasTxnCommit()) {
            return commitTs;
          }
          if (response.getError().hasTxnAborted()) {
            // A reader found us dead and rolled us back before our commit point.
            rollBackAll();
            throw new TxnException(
                TxnException.Code.ABORTED, response.getError().getMessage(), null);
          }
          throw new TxnException(
              TxnException.Code.UNKNOWN, "commit: " + response.getError().getMessage(), null);
        })
        .thenApply(ts -> {
          if (crash == TxnClient.CrashPoint.AFTER_PRIMARY) {
            throw abandoned("after committing the primary");
          }
          // Committed. The secondaries are a formality: a reader that meets one of their locks
          // finds the primary committed and rolls it forward.
          for (Mutation m : writes.values()) {
            if (!m.getKey().equals(primary)) {
              client.send(TxnClient.commitRequest(m.getKey(), startTs, ts));
            }
          }
          return ts;
        });
  }

  private void rollBackAll() {
    for (Mutation m : writes.values()) {
      client.send(TxnClient.rollbackRequest(m.getKey(), startTs));
    }
  }

  /**
   * Sends a read, and resolves any lock in its way: a lock of a transaction that started at or
   * before ours might commit below our timestamp, so we can't read past it until it is settled.
   */
  private CompletableFuture<KvResponse> read(KvRequest request, long deadline) {
    return client
        .send(request)
        .handle((response, failure) -> {
          if (failure != null) {
            return CompletableFuture.<KvResponse>failedFuture(
                new TxnException(
                    TxnException.Code.FAILED, "read failed", TxnClient.unwrap(failure)));
          }
          if (!response.hasError()) {
            return CompletableFuture.completedFuture(response);
          }
          Error error = response.getError();
          if (error.hasKeyLocked() && client.clock().nanoTime() < deadline) {
            return client
                .resolve(error.getKeyLocked().getLock())
                .thenCompose(gone ->
                    gone
                        ? read(request, deadline)
                        : client.later(client.backoff()).thenCompose(x -> read(request, deadline)));
          }
          TxnException.Code code =
              error.hasTxnTooOld() ? TxnException.Code.TOO_OLD : TxnException.Code.FAILED;
          return CompletableFuture.<KvResponse>failedFuture(
              new TxnException(
                  code, "read: " + error.getKindCase() + " " + error.getMessage(), null));
        })
        .thenCompose(f -> f);
  }

  private long deadline() {
    return client.clock().nanoTime() + client.options().timeout().toNanos();
  }

  private void checkOpen() {
    if (finished) {
      throw new IllegalStateException("the transaction has finished");
    }
  }

  private static TxnException abandoned(String where) {
    return new TxnException(TxnException.Code.ABANDONED, "the client stopped " + where, null);
  }
}

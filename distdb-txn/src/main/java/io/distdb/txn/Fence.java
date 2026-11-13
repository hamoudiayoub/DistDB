package io.distdb.txn;

import com.google.protobuf.ByteString;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.LockInfo;
import io.distdb.rpc.proto.ScanLocksRequest;
import io.distdb.rpc.proto.SetTxnFloorRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Ends every transaction that started below a timestamp: {@link TxnClient#fence(long)}.
 *
 * <ol>
 *   <li><b>Floor.</b> Walk every range in key order and tell it to refuse prewrites below {@code
 *       ts}. After the walk, no new lock below {@code ts} can appear anywhere (a range split since
 *       inherits the floor).
 *   <li><b>Resolve.</b> Walk every range again for locks below {@code ts}, and resolve them. Their
 *       primaries are in ranges with the floor too, so each answers committed or rolled back
 *       (rolling back the ones still running), never "still running".
 * </ol>
 *
 * The GC worker fences at {@code now − lifetime} before it moves the safe point; a schema change
 * fences at the commit timestamp of the transaction that published the new schema.
 */
final class Fence {

  private final TxnClient txn;
  private final long ts;

  private Fence(TxnClient txn, long ts) {
    this.txn = txn;
    this.ts = ts;
  }

  static CompletableFuture<Void> run(TxnClient txn, long ts) {
    Fence f = new Fence(txn, ts);
    return f.floor(new byte[0]).thenCompose(v -> f.resolve(new byte[0]));
  }

  /** Step 1, from {@code key} to the end of the key space, one range at a time. */
  private CompletableFuture<Void> floor(byte[] key) {
    KvRequest request =
        KvRequest.newBuilder()
            .setSetTxnFloor(
                SetTxnFloorRequest.newBuilder().setKey(ByteString.copyFrom(key)).setFloor(ts))
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
          return floor(r.getSetTxnFloor().getRangeEnd().toByteArray());
        });
  }

  /** Step 2, from {@code key} to the end of the key space. */
  private CompletableFuture<Void> resolve(byte[] key) {
    KvRequest request =
        KvRequest.newBuilder()
            .setScanLocks(
                ScanLocksRequest.newBuilder()
                    .setStartKey(ByteString.copyFrom(key))
                    .setMaxTs(ts)
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
              .thenCompose(v -> next(r));
        });
  }

  private CompletableFuture<Void> next(KvResponse r) {
    if (!r.getScanLocks().hasResumeKey()) {
      return CompletableFuture.completedFuture(null);
    }
    return resolve(r.getScanLocks().getResumeKey().toByteArray());
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
                    new IllegalStateException("a lock below the fence would not go"));
          }
          return txn.later(Duration.ofMillis(50)).thenCompose(v -> resolveFully(lock, attempt + 1));
        });
  }
}

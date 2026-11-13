package io.distdb.sim;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.distdb.client.KvClient;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.Mutation;
import io.distdb.rpc.proto.PrewriteRequest;
import io.distdb.rpc.proto.TxnCommitRequest;
import io.distdb.txn.Txn;
import io.distdb.txn.TxnClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * {@link TxnClient#fence}'s contract, which schema changes rely on: afterwards no transaction that
 * started below the fence can commit, not even one that had already locked its keys (prewritten)
 * and only had its commit left to do.
 */
class FenceTest {

  private static ByteString key(String k) {
    return ByteString.copyFrom(k, StandardCharsets.UTF_8);
  }

  @Test
  void aTransactionThatPrewroteBeforeTheFenceCannotCommitAfterIt() {
    ShardedKvSimulation.Cluster cluster = ShardedKvSimulation.Cluster.start(7);
    TxnClient txn = cluster.txnClient(t -> TxnClient.CrashPoint.NONE);
    KvClient kv = cluster.kvClient();
    // An old transaction locks its key, with a long TTL: it is alive, just slow.
    long start = cluster.await(txn.timestamp());
    KvResponse prewrite =
        cluster.await(
            kv.send(
                KvRequest.newBuilder()
                    .setPrewrite(
                        PrewriteRequest.newBuilder()
                            .setMutation(
                                Mutation.newBuilder()
                                    .setOp(Mutation.Op.PUT)
                                    .setKey(key("fenced"))
                                    .setValue(key("old schema")))
                            .setPrimary(key("fenced"))
                            .setStartTs(start)
                            .setTtlMs(60_000))
                    .build(),
                Duration.ofSeconds(5)));
    assertThat(prewrite.hasError()).as(prewrite.toString()).isFalse();
    // The fence, at a later timestamp.
    long fence = cluster.await(txn.timestamp());
    cluster.await(txn.fence(fence));
    // The old transaction's commit point must be refused.
    long commitTs = cluster.await(txn.timestamp());
    KvResponse commit =
        cluster.await(
            kv.send(
                KvRequest.newBuilder()
                    .setTxnCommit(
                        TxnCommitRequest.newBuilder()
                            .setKey(key("fenced"))
                            .setStartTs(start)
                            .setCommitTs(commitTs))
                    .build(),
                Duration.ofSeconds(5)));
    assertThat(commit.hasError()).as("the commit after the fence: %s", commit).isTrue();
    assertThat(commit.getError().hasTxnAborted()).as(commit.toString()).isTrue();
    // Nothing of it is visible, and a transaction started after the fence commits normally.
    Txn reader = cluster.await(txn.begin());
    Optional<byte[]> seen = cluster.await(reader.get("fenced".getBytes(StandardCharsets.UTF_8)));
    assertThat(seen).isEmpty();
    Txn next = cluster.await(txn.begin());
    next.put("fenced".getBytes(StandardCharsets.UTF_8), "new".getBytes(StandardCharsets.UTF_8));
    cluster.await(next.commit());
  }
}

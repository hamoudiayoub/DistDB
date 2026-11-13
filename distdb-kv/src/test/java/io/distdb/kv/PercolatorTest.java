package io.distdb.kv;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.rpc.proto.CheckTxnStatusRequest;
import io.distdb.rpc.proto.CheckTxnStatusResponse;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.Mutation;
import io.distdb.rpc.proto.PrewriteRequest;
import io.distdb.rpc.proto.RangeDescriptor;
import io.distdb.rpc.proto.TxnCommitRequest;
import io.distdb.rpc.proto.TxnGetRequest;
import io.distdb.rpc.proto.TxnRollbackRequest;
import io.distdb.storage.KvEngine;
import io.distdb.storage.WriteBatch;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** Percolator's rules for one range, command by command, on a real engine. */
class PercolatorTest {

  private static final EngineOptions SMALL =
      new EngineOptions(16 * 1024, 1024, 10, 32 * 1024, 4, 12, 128 * 1024, 10);
  private static final RangeDescriptor ALL =
      RangeDescriptor.newBuilder().setRangeId(1).setStartKey(ByteString.EMPTY).build();

  private final KvEngine engine =
      LsmEngine.open(
          new SimFileSystem(SplittableRandomSource.seeded(1)),
          new SimScheduler(new SimClock()),
          Path.of("/n"),
          SMALL);
  private Percolator.Limits limits = Percolator.Limits.NONE;

  @Test
  void aCommittedWriteIsVisibleFromItsCommitTimestampOn() {
    write("k", "v1", 10, 20);
    assertThat(get("k", 15)).isNull();
    assertThat(get("k", 20)).isEqualTo("v1");
    assertThat(get("k", 99)).isEqualTo("v1");
  }

  @Test
  void aReaderAtOrAfterALocksStartMustResolveIt() {
    assertThat(prewrite("k", "v", "k", 10).hasPrewrite()).isTrue();
    assertThat(read("k", 9).hasTxnGet()).as("started before: cannot commit below it").isTrue();
    assertThat(read("k", 10).getError().hasKeyLocked()).isTrue();
    assertThat(read("k", 11).getError().getKeyLocked().getLock().getPrimary().toStringUtf8())
        .isEqualTo("k");
  }

  @Test
  void firstCommitterWins() {
    write("k", "a", 10, 20);
    assertThat(prewrite("k", "b", "k", 15).getError().getWriteConflict().getConflictCommitTs())
        .as("started at 15, but a write committed at 20")
        .isEqualTo(20);
    assertThat(prewrite("k", "c", "k", 25).hasPrewrite()).isTrue();
    assertThat(prewrite("k", "d", "k", 30).getError().hasKeyLocked())
        .as("a second transaction meets the lock")
        .isTrue();
  }

  @Test
  void everyCommandIsIdempotent() {
    assertThat(prewrite("k", "v", "k", 10).hasPrewrite()).isTrue();
    assertThat(prewrite("k", "v", "k", 10).hasPrewrite()).isTrue();
    assertThat(commit("k", 10, 20).hasTxnCommit()).isTrue();
    assertThat(commit("k", 10, 20).hasTxnCommit()).as("already committed").isTrue();
    assertThat(prewrite("k", "v", "k", 10).hasPrewrite())
        .as("a late prewrite of a committed transaction changes nothing")
        .isTrue();
    assertThat(Percolator.lock(engine, utf8("k"))).isEmpty();
    assertThat(rollback("k", 10).getError().getTxnCommitted().getCommitTs()).isEqualTo(20);
  }

  @Test
  void aRollbackRecordStopsALatePrewrite() {
    assertThat(rollback("k", 10).hasTxnRollback()).as("before its prewrite arrived").isTrue();
    assertThat(prewrite("k", "v", "k", 10).getError().hasTxnAborted()).isTrue();
    assertThat(commit("k", 10, 20).getError().hasTxnAborted()).isTrue();
    assertThat(get("k", 99)).isNull();
  }

  @Test
  void theStatusOfATransactionIsThatOfItsPrimary() {
    prewrite("p", "1", "p", 10 << 18);
    prewrite("s", "2", "p", 10 << 18);
    assertThat(status("p", 10 << 18, 11 << 18).getStatus())
        .as("within its TTL")
        .isEqualTo(CheckTxnStatusResponse.Status.LOCKED);
    commit("p", 10 << 18, 12 << 18);
    CheckTxnStatusResponse committed = status("p", 10 << 18, 13 << 18);
    assertThat(committed.getStatus()).isEqualTo(CheckTxnStatusResponse.Status.COMMITTED);
    assertThat(committed.getCommitTs()).isEqualTo(12 << 18);
    // A reader rolls the secondary forward, at the primary's commit timestamp.
    commit("s", 10 << 18, committed.getCommitTs());
    assertThat(get("s", 13 << 18)).isEqualTo("2");
  }

  @Test
  void anExpiredLockIsRolledBackByWhoeverAsks() {
    prewrite("p", "1", "p", 10L << 18); // TTL 1000 ms
    assertThat(status("p", 10L << 18, 1011L << 18).getStatus())
        .isEqualTo(CheckTxnStatusResponse.Status.ROLLED_BACK);
    assertThat(Percolator.lock(engine, utf8("p"))).isEmpty();
    assertThat(commit("p", 10L << 18, 1012L << 18).getError().hasTxnAborted())
        .as("its owner can no longer commit")
        .isTrue();
    assertThat(status("q", 30, 40).getStatus())
        .as("never prewritten: rolled back, so a late prewrite is refused")
        .isEqualTo(CheckTxnStatusResponse.Status.ROLLED_BACK);
    assertThat(prewrite("q", "x", "q", 30).getError().hasTxnAborted()).isTrue();
  }

  @Test
  void theFloorRefusesOldPrewritesAndRollsBackOldLocks() {
    prewrite("p", "1", "p", 10L << 18);
    limits = new Percolator.Limits(20L << 18, 0);
    assertThat(prewrite("q", "1", "q", 15L << 18).getError().hasTxnTooOld()).isTrue();
    assertThat(status("p", 10L << 18, 11L << 18).getStatus())
        .as("alive by its TTL, but below the floor")
        .isEqualTo(CheckTxnStatusResponse.Status.ROLLED_BACK);
  }

  @Test
  void gcKeepsWhatAReadAtTheSafePointOrLaterSees() {
    write("k", "v1", 10, 11);
    write("k", "v2", 20, 21);
    write("k", "v3", 30, 31);
    write("gone", "x", 12, 13);
    deleteKey("gone", 22, 23);
    gc(25);
    assertThat(get("k", 25)).isEqualTo("v2");
    assertThat(get("k", 40)).isEqualTo("v3");
    assertThat(engine.get(Percolator.dataKey(utf8("k"), 10))).as("v1 is gone").isEmpty();
    assertThat(engine.get(Percolator.dataKey(utf8("gone"), 12))).isEmpty();
    assertThat(engine.get(Percolator.writeKey(utf8("gone"), 23))).isEmpty();
    limits = new Percolator.Limits(25, 25);
    assertThat(read("k", 24).getError().hasTxnTooOld()).isTrue();
  }

  // ---------------------------------------------------------------------------------------------

  private KvResponse prewrite(String key, String value, String primary, long start) {
    return apply(b ->
        Percolator.prewrite(
            b,
            engine,
            PrewriteRequest.newBuilder()
                .setMutation(
                    Mutation.newBuilder()
                        .setOp(Mutation.Op.PUT)
                        .setKey(ByteString.copyFromUtf8(key))
                        .setValue(ByteString.copyFromUtf8(value)))
                .setPrimary(ByteString.copyFromUtf8(primary))
                .setStartTs(start)
                .setTtlMs(1000)
                .build(),
            limits));
  }

  private void deleteKey(String key, long start, long commit) {
    apply(b ->
        Percolator.prewrite(
            b,
            engine,
            PrewriteRequest.newBuilder()
                .setMutation(
                    Mutation.newBuilder()
                        .setOp(Mutation.Op.DELETE)
                        .setKey(ByteString.copyFromUtf8(key)))
                .setPrimary(ByteString.copyFromUtf8(key))
                .setStartTs(start)
                .setTtlMs(1000)
                .build(),
            limits));
    commit(key, start, commit);
  }

  private KvResponse commit(String key, long start, long commit) {
    return apply(b ->
        Percolator.commit(
            b,
            engine,
            TxnCommitRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setStartTs(start)
                .setCommitTs(commit)
                .build()));
  }

  private KvResponse rollback(String key, long start) {
    return apply(b ->
        Percolator.rollback(
            b,
            engine,
            TxnRollbackRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setStartTs(start)
                .build()));
  }

  private CheckTxnStatusResponse status(String primary, long start, long now) {
    return apply(b ->
            Percolator.checkTxnStatus(
                b,
                engine,
                CheckTxnStatusRequest.newBuilder()
                    .setPrimary(ByteString.copyFromUtf8(primary))
                    .setStartTs(start)
                    .setCurrentTs(now)
                    .build(),
                limits))
        .getCheckTxnStatus();
  }

  private void write(String key, String value, long start, long commit) {
    assertThat(prewrite(key, value, key, start).hasPrewrite()).isTrue();
    assertThat(commit(key, start, commit).hasTxnCommit()).isTrue();
  }

  private void gc(long safePoint) {
    WriteBatch b = new WriteBatch();
    Percolator.gc(b, engine, ALL, safePoint);
    engine.write(b, true);
  }

  private KvResponse read(String key, long ts) {
    return Percolator.get(
        engine,
        TxnGetRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)).setReadTs(ts).build(),
        limits);
  }

  private String get(String key, long ts) {
    KvResponse r = read(key, ts);
    assertThat(r.hasTxnGet()).as("%s", r).isTrue();
    return r.getTxnGet().getFound() ? r.getTxnGet().getValue().toStringUtf8() : null;
  }

  private interface Command {
    KvResponse run(WriteBatch batch);
  }

  private KvResponse apply(Command c) {
    WriteBatch b = new WriteBatch();
    KvResponse r = c.run(b);
    engine.write(b, true);
    return r;
  }

  private static byte[] utf8(String s) {
    return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }
}

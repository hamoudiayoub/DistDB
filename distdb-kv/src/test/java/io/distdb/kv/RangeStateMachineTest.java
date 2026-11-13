package io.distdb.kv;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.distdb.common.NodeId;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.raft.Entry;
import io.distdb.raft.HardState;
import io.distdb.raft.Membership;
import io.distdb.raft.PersistedState;
import io.distdb.raft.RaftCodec;
import io.distdb.rpc.proto.Command;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.PutRequest;
import io.distdb.rpc.proto.RangeDescriptor;
import io.distdb.rpc.proto.RequestId;
import io.distdb.rpc.proto.SplitCommand;
import io.distdb.rpc.proto.WriteCommand;
import io.distdb.storage.KvEngine;
import io.distdb.storage.WriteBatch;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The rules of one range's state machine, applied entry by entry, without Raft around it. */
class RangeStateMachineTest {

  private static final NodeId N1 = new NodeId(1);
  private static final byte[] ALICE = new byte[16];
  private static final EngineOptions SMALL =
      new EngineOptions(16 * 1024, 1024, 10, 32 * 1024, 4, 12, 128 * 1024, 10);

  private final KvEngine engine = engine(1);
  private long index = RangeStateMachine.INITIAL_INDEX;

  @Test
  void aWriteForAKeyOutsideTheSpanChangesNothing() {
    RangeStateMachine m = range(1, "", "m", N1);
    RangeStateMachine.Applied a = m.apply(entry(put(1, "x", "v")));
    assertThat(a.response().getError().hasEpochMismatch()).isTrue();
    assertThat(a.effect()).isFalse();
    assertThat(engine.get(KeySpace.user(utf8("x")))).isEmpty();
    assertThat(m.apply(entry(put(2, "a", "v"))).effect()).isTrue();
  }

  @Test
  void aSplitNarrowsTheRangeAndCreatesTheRightOneInTheSameBatch() {
    RangeStateMachine m = range(1, "", null, N1);
    m.apply(entry(put(1, "b", "1")));
    m.apply(entry(put(2, "q", "2")));
    RangeStateMachine.Applied a = m.apply(entry(split("m", 7, 1)));

    assertThat(m.descriptor().getEndKey().toStringUtf8()).isEqualTo("m");
    assertThat(m.descriptor().getVersion()).isEqualTo(2);
    RangeDescriptor right = a.created();
    assertThat(right.getRangeId()).isEqualTo(7);
    assertThat(right.getStartKey().toStringUtf8()).isEqualTo("m");
    assertThat(right.hasEndKey()).isFalse();
    assertThat(right.getVersion()).isEqualTo(2);
    assertThat(right.getVotersList()).containsExactly(1);

    RangeStateMachine r = new RangeStateMachine(engine, N1, 7, KvServiceOptions.defaults());
    assertThat(r.descriptor()).isEqualTo(right);
    assertThat(r.appliedIndex()).isEqualTo(RangeStateMachine.INITIAL_INDEX);
    PersistedState raft = new RaftStore(engine, 7).load();
    assertThat(raft.snapshot().index()).isEqualTo(RangeStateMachine.INITIAL_INDEX);
    assertThat(raft.hardState().commit()).isEqualTo(RangeStateMachine.INITIAL_INDEX);
    assertThat(RangeStateMachine.descriptorOf(raft.snapshot())).isEqualTo(right);
    // The data did not move: both ranges share the engine, each sees its own span.
    assertThat(engine.get(KeySpace.user(utf8("q")))).isPresent();
  }

  @Test
  void theNewRangeGetsACopyOfTheSessionsSoARetryThereIsRecognized() {
    RangeStateMachine m = range(1, "", null, N1);
    KvRequest write = put(1, "q", "v");
    assertThat(m.apply(entry(write)).effect()).isTrue();
    m.apply(entry(split("m", 7, 1)));
    RangeStateMachine right = new RangeStateMachine(engine, N1, 7, KvServiceOptions.defaults());
    index = RangeStateMachine.INITIAL_INDEX;
    RangeStateMachine.Applied retry = right.apply(entry(write));
    assertThat(retry.effect()).as("answered from the copied record").isFalse();
    assertThat(retry.response().hasPut()).isTrue();
  }

  @Test
  void aStaleSplitIsIgnored() {
    RangeStateMachine m = range(1, "", null, N1);
    assertThat(m.apply(entry(split("m", 7, 1))).created()).isNotNull();
    assertThat(m.apply(entry(split("f", 8, 1))).created()).as("version moved on").isNull();
    assertThat(m.apply(entry(split("z", 9, 2))).created()).as("key outside").isNull();
    assertThat(m.descriptor().getEndKey().toStringUtf8()).isEqualTo("m");
  }

  @Test
  void aNodeAlreadyRemovedFromTheNewRangeDoesNotCreateIt() {
    WriteBatch tomb = new WriteBatch();
    tomb.put(RangeStateMachine.tombstoneKey(7), KeySpace.uint64(40));
    engine.write(tomb, true);
    RangeStateMachine m = range(1, "", null, N1);
    m.apply(entry(put(1, "q", "v")));
    assertThat(m.apply(entry(split("m", 7, 1))).created()).isNull();
    assertThat(engine.get(KeySpace.user(utf8("q")))).as("no longer this node's").isEmpty();
    assertThat(new RangeStateMachine(engine, N1, 7, KvServiceOptions.defaults()).initialized())
        .isFalse();
  }

  @Test
  void aReplicaThatIsNoLongerAMemberDoesNotCreateTheNewRange() {
    RangeStateMachine m = range(1, "", null, new NodeId(2)); // descriptor voters: n2 only
    m.apply(entry(put(1, "q", "v")));
    assertThat(m.apply(entry(split("m", 7, 1))).created()).isNull();
    assertThat(engine.get(KeySpace.user(utf8("q")))).isEmpty();
  }

  @Test
  void aSplitKeepsTheVoteOfAnUninitializedReplicaOfTheNewRange() {
    WriteBatch voted = new WriteBatch();
    new RaftStore(engine, 7)
        .initialize(
            voted,
            new HardState(9, new NodeId(3), 0),
            io.distdb.raft.Snapshot.initial(Membership.EMPTY));
    engine.write(voted, true);
    RangeStateMachine m = range(1, "", null, N1);
    m.apply(entry(split("m", 7, 1)));
    HardState hs = new RaftStore(engine, 7).load().hardState();
    assertThat(hs.term()).isEqualTo(9);
    assertThat(hs.vote()).isEqualTo(new NodeId(3));
  }

  @Test
  void aMembershipChangeIsMirroredIntoTheDescriptor() {
    RangeStateMachine m = range(1, "", null, N1);
    Membership next = new Membership(List.of(N1), List.of(new NodeId(4)));
    long at = ++index;
    RangeStateMachine.Applied a =
        m.apply(new Entry(at, 6, Entry.Type.CONFIG, RaftCodec.encodeMembership(next)));
    assertThat(a.membershipChanged()).isTrue();
    assertThat(m.descriptor().getLearnersList()).containsExactly(4);
    assertThat(m.descriptor().getConfVer()).isEqualTo(at);
  }

  @Test
  void aSnapshotReplacesTheWholeStateIncludingAWiderOldSpan() {
    RangeStateMachine m = range(1, "", null, N1);
    m.apply(entry(put(1, "b", "1")));
    m.apply(entry(put(2, "q", "2")));
    m.apply(entry(split("m", 7, 1)));
    byte[] data = m.snapshotData();

    KvEngine other = engine(2);
    WriteBatch old = new WriteBatch();
    old.put(KeySpace.user(utf8("z")), utf8("stale")); // from before the split, on a lagging node
    RangeStateMachine.bootstrap(other, old, descriptor(1, "", null, N1));
    other.write(old, true);
    RangeStateMachine lagging = new RangeStateMachine(other, N1, 1, KvServiceOptions.defaults());
    lagging.restore(new io.distdb.raft.Snapshot(9, 6, Membership.ofVoters(N1), data));
    assertThat(lagging.descriptor()).isEqualTo(m.descriptor());
    assertThat(other.get(KeySpace.user(utf8("b")))).isPresent();
    assertThat(other.get(KeySpace.user(utf8("z")))).as("the old span is cleared").isEmpty();
    assertThat(other.get(KeySpace.user(utf8("q")))).as("not in the snapshot").isEmpty();
  }

  // ---------------------------------------------------------------------------------------------

  private RangeStateMachine range(long id, String start, String end, NodeId voter) {
    WriteBatch b = new WriteBatch();
    RangeStateMachine.bootstrap(engine, b, descriptor(id, start, end, voter));
    engine.write(b, true);
    return new RangeStateMachine(engine, N1, id, KvServiceOptions.defaults());
  }

  private static RangeDescriptor descriptor(long id, String start, String end, NodeId voter) {
    RangeDescriptor.Builder d =
        RangeDescriptor.newBuilder()
            .setRangeId(id)
            .setStartKey(ByteString.copyFromUtf8(start))
            .addVoters(voter.value())
            .setVersion(1)
            .setConfVer(RangeStateMachine.INITIAL_INDEX);
    if (end != null) {
      d.setEndKey(ByteString.copyFromUtf8(end));
    }
    return d.build();
  }

  private Entry entry(KvRequest write) {
    Command c =
        Command.newBuilder()
            .setWrite(WriteCommand.newBuilder().setRequest(write).setProposeTimeMillis(1))
            .build();
    return new Entry(++index, 6, Entry.Type.NORMAL, c.toByteArray());
  }

  private Entry entry(Command c) {
    return new Entry(++index, 6, Entry.Type.NORMAL, c.toByteArray());
  }

  private static Command split(String key, long newId, long version) {
    return Command.newBuilder()
        .setSplit(
            SplitCommand.newBuilder()
                .setSplitKey(ByteString.copyFromUtf8(key))
                .setNewRangeId(newId)
                .setExpectedVersion(version))
        .build();
  }

  private static KvRequest put(long seq, String key, String value) {
    return KvRequest.newBuilder()
        .setRequestId(RequestId.newBuilder().setClientId(ByteString.copyFrom(ALICE)).setSeq(seq))
        .setFirstIncompleteSeq(seq)
        .setPut(
            PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value)))
        .build();
  }

  private static KvEngine engine(long seed) {
    SimScheduler scheduler = new SimScheduler(new SimClock());
    return LsmEngine.open(
        new SimFileSystem(SplittableRandomSource.seeded(seed)), scheduler, Path.of("/n"), SMALL);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}

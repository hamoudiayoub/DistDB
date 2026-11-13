package io.distdb.kv;

import com.google.protobuf.InvalidProtocolBufferException;
import io.distdb.common.Bytes;
import io.distdb.common.NodeId;
import io.distdb.raft.Entry;
import io.distdb.raft.HardState;
import io.distdb.raft.Membership;
import io.distdb.raft.RaftCodec;
import io.distdb.raft.Snapshot;
import io.distdb.rpc.proto.Command;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.RangeDescriptor;
import io.distdb.rpc.proto.SplitCommand;
import io.distdb.storage.KvEngine;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import io.distdb.storage.WriteBatch;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The state machine of one range: applies the range's committed log entries to the part of the
 * shared engine that the range owns. Everything it writes is scoped to the range: client data in
 * its span, client sessions under its id, and its descriptor and applied index.
 *
 * <p>As in Phase 5, applying depends only on the entry and the current state, and each entry is
 * applied in one batch that also records its index. On top of that:
 *
 * <ul>
 *   <li>A write for a key outside the span is answered {@code EpochMismatch} and changes nothing:
 *       the range split after the write was proposed, and the key belongs to another group now.
 *   <li>A membership change (a {@code CONFIG} entry) is mirrored into the descriptor, whose {@code
 *       conf_ver} becomes the entry's index.
 *   <li>A {@code Split} narrows this range and creates the right-hand one, both in the same batch.
 * </ul>
 *
 * <p>A replica created by a message has no descriptor yet: it is <b>uninitialized</b>, and only a
 * snapshot (or a split on this node) initializes it.
 *
 * <p>Not thread-safe: the store applies from its mailbox.
 */
public final class RangeStateMachine {

  /**
   * Every range starts with a snapshot at this index and term, never at 0: a replica created empty
   * by a message has a log that matches index 0, so starting there would let a leader send it
   * entries without the data they build on. Starting at 5, the consistency check fails, and the
   * leader sends a snapshot instead.
   */
  static final long INITIAL_INDEX = 5;

  static final long INITIAL_TERM = 5;

  /**
   * What applying one entry did.
   *
   * @param request the client write it carried, or null
   * @param response the answer for that write, or null
   * @param effect whether it changed client data
   * @param created the range a split created here, or null (no split, or this node was already
   *     removed from the new range)
   * @param membershipChanged whether it was a membership change
   */
  public record Applied(
      long index,
      KvStateMachine.RequestKey request,
      KvResponse response,
      boolean effect,
      RangeDescriptor created,
      boolean membershipChanged) {}

  private final KvEngine engine;
  private final NodeId self;
  private final long rangeId;
  private final KvServiceOptions options;
  private final ClientSessions sessions;
  private RangeDescriptor descriptor; // null while uninitialized
  private long applied;

  RangeStateMachine(KvEngine engine, NodeId self, long rangeId, KvServiceOptions options) {
    this.engine = Objects.requireNonNull(engine);
    this.self = Objects.requireNonNull(self);
    this.rangeId = rangeId;
    this.options = Objects.requireNonNull(options);
    this.sessions = new ClientSessions(engine, KeySpace.uint64(rangeId));
    this.descriptor = engine.get(descriptorKey(rangeId)).map(RangeStateMachine::parse).orElse(null);
    this.applied = engine.get(appliedKey(rangeId)).map(v -> KeySpace.readUint64(v, 0)).orElse(0L);
  }

  /** The range as of the last entry applied, or null if this replica is uninitialized. */
  public RangeDescriptor descriptor() {
    return descriptor;
  }

  public boolean initialized() {
    return descriptor != null;
  }

  public long appliedIndex() {
    return applied;
  }

  /** Applies the next committed entry, which must directly follow the last one applied. */
  public Applied apply(Entry e) {
    if (descriptor == null) {
      throw new IllegalStateException("r" + rangeId + " is uninitialized but got " + e);
    }
    if (e.index() != applied + 1) {
      throw new IllegalStateException("applying " + e + " right after " + applied);
    }
    WriteBatch batch = new WriteBatch();
    RangeDescriptor next = descriptor;
    Applied result = new Applied(e.index(), null, null, false, null, false);
    if (e.type() == Entry.Type.CONFIG) {
      Membership m = RaftCodec.decodeMembership(e.data());
      next = Ranges.withMembership(descriptor, m, e.index());
      batch.put(descriptorKey(rangeId), next.toByteArray());
      result = new Applied(e.index(), null, null, false, null, true);
    } else if (e.data().length > 0) {
      Command c = parseCommand(e.data());
      switch (c.getKindCase()) {
        case WRITE -> {
          RangeDescriptor owner = descriptor;
          KvWrites.Result w =
              KvWrites.apply(
                  batch, engine, sessions, c.getWrite(), options, k -> Ranges.contains(owner, k));
          result = new Applied(e.index(), w.request(), w.response(), w.effect(), null, false);
        }
        case EXPIRE_SESSIONS ->
            sessions.expire(
                batch,
                c.getExpireSessions().getNowMillis(),
                Duration.ofMillis(c.getExpireSessions().getTtlMillis()));
        case SPLIT -> {
          RangeDescriptor[] out = split(batch, c.getSplit());
          if (out != null) {
            next = out[0];
            result = new Applied(e.index(), null, null, false, out[1], false);
          }
        }
        case KIND_NOT_SET -> throw new IllegalStateException("empty command at " + e.index());
      }
    }
    batch.put(appliedKey(rangeId), KeySpace.uint64(e.index()));
    engine.write(batch, false); // the log is durable; a lost apply is replayed from it
    descriptor = next;
    applied = e.index();
    return result;
  }

  /**
   * Splits this range at {@code s.split_key}, into {@code batch}: {@code [left, right]}, where
   * right is null if this node was already removed from the new range; or null if the split is
   * stale (another version, or a key outside the span), and nothing changes.
   */
  private RangeDescriptor[] split(WriteBatch batch, SplitCommand s) {
    byte[] key = s.getSplitKey().toByteArray();
    long newId = s.getNewRangeId();
    if (descriptor.getVersion() != s.getExpectedVersion()
        || !Ranges.contains(descriptor, key)
        || Bytes.compare(key, descriptor.getStartKey().toByteArray()) == 0
        || newId == rangeId
        || newId == 0) {
      return null;
    }
    long version = descriptor.getVersion() + 1;
    RangeDescriptor left =
        descriptor.toBuilder().setEndKey(s.getSplitKey()).setVersion(version).build();
    RangeDescriptor.Builder rightBuilder =
        descriptor.toBuilder()
            .setRangeId(newId)
            .setStartKey(s.getSplitKey())
            .setVersion(version)
            .setConfVer(INITIAL_INDEX);
    if (!descriptor.hasEndKey()) {
      rightBuilder.clearEndKey();
    }
    RangeDescriptor right = rightBuilder.build();
    batch.put(descriptorKey(rangeId), left.toByteArray());
    if (engine.get(tombstoneKey(newId)).isPresent() || !Ranges.isMember(right, self)) {
      // This node was already removed from the new range (it has a tombstone), or is not one of
      // its members at all: a replica that lags behind its own removal still applies the entries
      // before it. The data is no longer this node's to keep.
      deleteSpan(batch, right);
      return new RangeDescriptor[] {left, null};
    }
    // The new range's client sessions are a copy of this one's: a write applied here before the
    // split may be retried after it, and the retry now goes to the new range.
    ClientSessions rightSessions = new ClientSessions(engine, KeySpace.uint64(newId));
    sessions.copyTo(batch, rightSessions);
    batch.put(descriptorKey(newId), right.toByteArray());
    batch.put(appliedKey(newId), KeySpace.uint64(INITIAL_INDEX));
    RaftStore rightStore = new RaftStore(engine, newId);
    // A replica of the new range may already exist here, uninitialized, and may have voted in
    // its first election: keep that term and vote, or this node could vote twice in a term.
    HardState hs =
        rightStore
            .hardState()
            .filter(h -> h.term() > INITIAL_TERM)
            .map(h -> new HardState(h.term(), h.vote(), INITIAL_INDEX))
            .orElse(new HardState(INITIAL_TERM, null, INITIAL_INDEX));
    byte[] data =
        encode(
            right,
            sink -> {
              sessions.forEach(KeySpace.uint64(newId), sink::write);
              scanSpan(right, sink::write);
            });
    rightStore.initialize(
        batch, hs, new Snapshot(INITIAL_INDEX, INITIAL_TERM, Ranges.membership(right), data));
    return new RangeDescriptor[] {left, right};
  }

  /**
   * The range's whole replicated state (descriptor, client sessions, client data in the span), for
   * a Raft snapshot at {@link #appliedIndex()}. Materialized in memory: fine at the simulator's
   * sizes, and the reason streaming snapshots is future work.
   */
  public byte[] snapshotData() {
    RangeDescriptor d = Objects.requireNonNull(descriptor, "uninitialized");
    return encode(
        d,
        sink -> {
          sessions.forEach(KeySpace.uint64(rangeId), sink::write);
          scanSpan(d, sink::write);
        });
  }

  /**
   * Replaces this replica's state with snapshot {@code s}, in one batch. The old span is cleared
   * too: a replica that missed splits covered more keys than the snapshot does, and nobody else on
   * this node can own them (the spans of a node's replicas are disjoint).
   */
  public void restore(Snapshot s) {
    RangeDescriptor d = descriptorOf(s);
    WriteBatch batch = new WriteBatch();
    if (descriptor != null) {
      deleteSpan(batch, descriptor);
    }
    deleteSpan(batch, d);
    sessions.deleteAll(batch);
    decode(s.data(), batch); // later operations in a batch win, so the records stay
    batch.put(descriptorKey(rangeId), d.toByteArray());
    batch.put(appliedKey(rangeId), KeySpace.uint64(s.index()));
    engine.write(batch, false); // the snapshot itself is stored durably by RaftStore
    descriptor = d;
    applied = s.index();
  }

  /**
   * Adds to {@code batch} the deletion of everything this replica stored: client data in its span,
   * sessions, descriptor and applied index. The Raft state is {@link RaftStore#destroy}'s.
   */
  void destroy(WriteBatch batch) {
    if (descriptor != null) {
      deleteSpan(batch, descriptor);
    }
    sessions.deleteAll(batch);
    batch.delete(descriptorKey(rangeId));
    batch.delete(appliedKey(rangeId));
  }

  /** Writes a brand-new range {@code d} into {@code batch}: the first range of a new cluster. */
  static void bootstrap(KvEngine engine, WriteBatch batch, RangeDescriptor d) {
    batch.put(descriptorKey(d.getRangeId()), d.toByteArray());
    batch.put(appliedKey(d.getRangeId()), KeySpace.uint64(INITIAL_INDEX));
    new RaftStore(engine, d.getRangeId())
        .initialize(
            batch,
            new HardState(INITIAL_TERM, null, INITIAL_INDEX),
            new Snapshot(INITIAL_INDEX, INITIAL_TERM, Ranges.membership(d), encode(d, sink -> {})));
  }

  /** The descriptor a snapshot carries, without reading the rest. */
  static RangeDescriptor descriptorOf(Snapshot s) {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(s.data()))) {
      byte[] d = new byte[in.readInt()];
      in.readFully(d);
      return parse(d);
    } catch (IOException e) {
      throw new IllegalStateException("unreadable snapshot at " + s.index(), e);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Keys and encoding

  static byte[] descriptorKey(long rangeId) {
    return KeySpace.local(KeySpace.DESCRIPTOR, KeySpace.uint64(rangeId));
  }

  static byte[] appliedKey(long rangeId) {
    return KeySpace.local(KeySpace.APPLIED, KeySpace.uint64(rangeId));
  }

  static byte[] tombstoneKey(long rangeId) {
    return KeySpace.local(KeySpace.TOMBSTONE, KeySpace.uint64(rangeId));
  }

  private interface Sink {
    void write(byte[] key, byte[] value);
  }

  private interface Body {
    void writeTo(Sink sink);
  }

  /** Snapshot format: the descriptor, then (key, value) records until the end. */
  private static byte[] encode(RangeDescriptor d, Body body) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      byte[] desc = d.toByteArray();
      out.writeInt(desc.length);
      out.write(desc);
      body.writeTo((k, v) -> {
        try {
          out.writeInt(k.length);
          out.write(k);
          out.writeInt(v.length);
          out.write(v);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bytes.toByteArray();
  }

  /** Reads a snapshot, putting its records into {@code batch}; returns its descriptor. */
  private static RangeDescriptor decode(byte[] data, WriteBatch batch) {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
      byte[] desc = new byte[in.readInt()];
      in.readFully(desc);
      RangeDescriptor d = parse(desc);
      while (in.available() > 0) {
        byte[] k = new byte[in.readInt()];
        in.readFully(k);
        byte[] v = new byte[in.readInt()];
        in.readFully(v);
        batch.put(k, v);
      }
      return d;
    } catch (IOException e) {
      throw new IllegalStateException("unreadable snapshot", e);
    }
  }

  private void scanSpan(RangeDescriptor d, Sink sink) {
    byte[][] span = Ranges.userSpan(d);
    try (KvIterator it = engine.scan(span[0], span[1])) {
      while (it.hasNext()) {
        KvEntry e = it.next();
        sink.write(e.key(), e.value());
      }
    }
  }

  private void deleteSpan(WriteBatch batch, RangeDescriptor d) {
    scanSpan(d, (k, v) -> batch.delete(k));
  }

  private static RangeDescriptor parse(byte[] bytes) {
    try {
      return RangeDescriptor.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("unreadable range descriptor", e);
    }
  }

  private static Command parseCommand(byte[] data) {
    try {
      return Command.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("unreadable command in the log", e);
    }
  }

  /** The tombstone left on this node for range {@code id}, if any: the conf_ver that removed it. */
  static Optional<Long> tombstone(KvEngine engine, long id) {
    return engine.get(tombstoneKey(id)).map(v -> KeySpace.readUint64(v, 0));
  }

  /**
   * The last log index this node's destroyed replica of range {@code id} held, or 0: it may have
   * acknowledged entries up to there, so a replica added back must not vote before its log is that
   * long again.
   */
  static long tombstoneLastIndex(KvEngine engine, long id) {
    return engine
        .get(tombstoneKey(id))
        .filter(v -> v.length >= 16)
        .map(v -> KeySpace.readUint64(v, 8))
        .orElse(0L);
  }

  /** A tombstone: the conf_ver that removed this node, and the last index its replica held. */
  static byte[] tombstoneValue(long confVer, long lastIndex) {
    byte[] v = new byte[16];
    System.arraycopy(KeySpace.uint64(confVer), 0, v, 0, 8);
    System.arraycopy(KeySpace.uint64(lastIndex), 0, v, 8, 8);
    return v;
  }
}

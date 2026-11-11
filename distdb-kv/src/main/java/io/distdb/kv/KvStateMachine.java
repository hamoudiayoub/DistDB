package io.distdb.kv;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.distdb.raft.Entry;
import io.distdb.raft.Snapshot;
import io.distdb.rpc.KvErrors;
import io.distdb.rpc.proto.Command;
import io.distdb.rpc.proto.DeleteResponse;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.PutResponse;
import io.distdb.rpc.proto.WriteCommand;
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
 * The replicated state machine: applies committed log entries to the engine. Every replica applies
 * the same entries in the same order and must reach the same state, so applying depends only on the
 * entry and the current state. Time comes from inside the command; there is no local clock and no
 * randomness.
 *
 * <p>Each entry is applied in <b>one batch that also records its index</b> (the applied index).
 * After a crash the data and the applied index agree, and Raft replays exactly the entries after
 * it. The batch need not be synced: the log is already on disk, so a lost apply is just redone.
 *
 * <p>Retried writes are deduplicated here, at apply time, by {@link ClientSessions}. A write
 * proposed twice (a client retry that reached a new leader, say) is in the log twice; the second
 * copy is answered from the first one's record and changes nothing.
 *
 * <p>Not thread-safe: the host applies from one thread.
 */
public final class KvStateMachine {

  /** Names a client write: who sent it, and its sequence number. */
  public record RequestKey(ByteString clientId, long seq) {
    static RequestKey of(KvRequest r) {
      return new RequestKey(r.getRequestId().getClientId(), r.getRequestId().getSeq());
    }
  }

  /**
   * What applying one entry did.
   *
   * @param request the client write it carried, or null (no-op, config change, expiry)
   * @param response the answer for that write, or null
   * @param effect whether it changed client data (false for a duplicate, a stale copy, or an entry
   *     that carries no write)
   */
  public record Applied(long index, RequestKey request, KvResponse response, boolean effect) {}

  private final KvEngine engine;
  private final byte[] appliedKey;
  private final ClientSessions sessions;
  private final KvServiceOptions options;
  private long applied;

  public KvStateMachine(KvEngine engine, long groupId, KvServiceOptions options) {
    this.engine = Objects.requireNonNull(engine);
    this.options = Objects.requireNonNull(options);
    this.appliedKey = KeySpace.local(KeySpace.APPLIED, KeySpace.uint64(groupId));
    this.sessions = new ClientSessions(engine);
    this.applied = engine.get(appliedKey).map(v -> KeySpace.readUint64(v, 0)).orElse(0L);
  }

  /** The index of the last entry applied. */
  public long appliedIndex() {
    return applied;
  }

  /** Applies the next entry, which must directly follow the last one applied. */
  public Applied apply(Entry e) {
    if (e.index() != applied + 1) {
      throw new IllegalStateException("applying " + e + " right after " + applied);
    }
    WriteBatch batch = new WriteBatch();
    Applied result = new Applied(e.index(), null, null, false);
    if (e.type() == Entry.Type.NORMAL && e.data().length > 0) {
      Command c = parse(e.data());
      switch (c.getKindCase()) {
        case WRITE -> result = applyWrite(batch, e.index(), c.getWrite());
        case EXPIRE_SESSIONS ->
            sessions.expire(
                batch,
                c.getExpireSessions().getNowMillis(),
                Duration.ofMillis(c.getExpireSessions().getTtlMillis()));
        case KIND_NOT_SET -> throw new IllegalStateException("empty command at " + e.index());
      }
    }
    batch.put(appliedKey, KeySpace.uint64(e.index()));
    engine.write(batch, false); // the log is durable; a lost apply is replayed from it
    applied = e.index();
    return result;
  }

  private Applied applyWrite(WriteBatch batch, long index, WriteCommand w) {
    KvRequest r = w.getRequest();
    RequestKey key = RequestKey.of(r);
    try {
      KvRequests.validateWrite(r, options);
    } catch (KvRequests.InvalidRequest bad) {
      // The leader validates before proposing; every replica still reaches the same answer.
      return new Applied(index, key, KvErrors.invalidArgument(bad.getMessage()), false);
    }
    byte[] clientId = r.getRequestId().getClientId().toByteArray();
    long seq = r.getRequestId().getSeq();
    ClientSessions.Session session = sessions.load(clientId);
    if (seq < session.floor()) {
      return new Applied(
          index,
          key,
          KvErrors.staleRequest("write " + seq + " is below its client's floor " + session.floor()),
          false);
    }
    Optional<KvResponse> earlier = sessions.result(clientId, seq);
    if (earlier.isPresent()) {
      return new Applied(index, key, earlier.get(), false); // a retry: never applied twice
    }
    KvResponse response;
    if (r.hasPut()) {
      batch.put(
          KeySpace.user(r.getPut().getKey().toByteArray()), r.getPut().getValue().toByteArray());
      response = KvResponse.newBuilder().setPut(PutResponse.getDefaultInstance()).build();
    } else {
      batch.delete(KeySpace.user(r.getDelete().getKey().toByteArray()));
      response = KvResponse.newBuilder().setDelete(DeleteResponse.getDefaultInstance()).build();
    }
    sessions.record(
        batch,
        clientId,
        session,
        seq,
        r.getFirstIncompleteSeq(),
        response,
        w.getProposeTimeMillis());
    return new Applied(index, key, response, true);
  }

  /**
   * The whole replicated state (client data and sessions), for a Raft snapshot at {@link
   * #appliedIndex()}. It is materialized in memory: fine for the simulator's sizes, and the reason
   * streaming snapshots is future work.
   */
  public byte[] snapshotData() {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      for (byte[][] range : KeySpace.replicatedRanges()) {
        try (KvIterator it = engine.scan(range[0], range[1])) {
          while (it.hasNext()) {
            KvEntry e = it.next();
            out.writeInt(e.key().length);
            out.write(e.key());
            out.writeInt(e.value().length);
            out.write(e.value());
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bytes.toByteArray();
  }

  /** Replaces the whole replicated state with {@code s}, in one batch. */
  public void restore(Snapshot s) {
    WriteBatch batch = new WriteBatch();
    for (byte[][] range : KeySpace.replicatedRanges()) {
      try (KvIterator it = engine.scan(range[0], range[1])) {
        while (it.hasNext()) {
          batch.delete(it.next().key());
        }
      }
    }
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(s.data()))) {
      while (in.available() > 0) {
        byte[] k = new byte[in.readInt()];
        in.readFully(k);
        byte[] v = new byte[in.readInt()];
        in.readFully(v);
        batch.put(k, v);
      }
    } catch (IOException e) {
      throw new IllegalStateException("unreadable snapshot at " + s.index(), e);
    }
    batch.put(appliedKey, KeySpace.uint64(s.index()));
    engine.write(batch, false); // the snapshot itself is stored durably by RaftStore
    applied = s.index();
  }

  private static Command parse(byte[] data) {
    try {
      return Command.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("unreadable command in the log", e);
    }
  }
}

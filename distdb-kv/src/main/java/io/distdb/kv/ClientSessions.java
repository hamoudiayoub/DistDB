package io.distdb.kv;

import com.google.protobuf.InvalidProtocolBufferException;
import io.distdb.common.Bytes;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.storage.CorruptionException;
import io.distdb.storage.KvEngine;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import io.distdb.storage.WriteBatch;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * The table that makes retried writes safe: for each client, the answers to its recent writes,
 * stored in the engine next to the data (the pattern of section 6.3 of Ongaro's Raft thesis).
 *
 * <p>Records, in the {@link KeySpace#LOCAL local} key space, under a <b>scope</b>: empty for a
 * single group (Phase 5), the range id for one range of a sharded store, so that every range has
 * its own table and a range's snapshot can carry it:
 *
 * <pre>
 *   0x01 's' scope clientId            → floor (8 bytes) | lastActiveMillis (8 bytes)
 *   0x01 'r' scope clientId seq(8, BE) → the KvResponse sent for that write
 * </pre>
 *
 * <p>The rules:
 *
 * <ol>
 *   <li>A write's result is recorded <b>in the same {@link WriteBatch} as the write</b>. After a
 *       crash either both survive or neither does, so a retry after a restart is still recognized.
 *   <li>A write whose seq already has a result is answered from it, never applied again.
 *   <li>Each request says the lowest seq its client is still waiting for. Results below it are
 *       deleted, and the session's <b>floor</b> rises to it. A write below the floor can only be a
 *       stray late copy of one the client has finished with, and is rejected.
 *   <li>A session idle for longer than the TTL is forgotten. That is safe only because clients stop
 *       retrying a write at its deadline, and deadlines are far shorter than the TTL.
 * </ol>
 *
 * <p>Not thread-safe: the caller serializes every use, and every write to user data, under one
 * lock. Checking for a result and recording one must be atomic with respect to other writes.
 */
final class ClientSessions {

  static final int CLIENT_ID_BYTES = 16;

  /** A client's session. Every seq below {@code floor} is finished. */
  record Session(long floor, long lastActiveMillis) {
    static final Session NEW = new Session(0, 0);
  }

  private final KvEngine engine;
  private final byte[] scope;

  ClientSessions(KvEngine engine) {
    this(engine, new byte[0]);
  }

  ClientSessions(KvEngine engine, byte[] scope) {
    this.engine = engine;
    this.scope = scope.clone();
  }

  Session load(byte[] clientId) {
    return engine
        .get(sessionKey(clientId))
        .map(v -> new Session(KeySpace.readUint64(v, 0), KeySpace.readUint64(v, 8)))
        .orElse(Session.NEW);
  }

  /** The response already sent for write {@code seq}, if it was applied. */
  Optional<KvResponse> result(byte[] clientId, long seq) {
    return engine.get(resultKey(clientId, seq)).map(ClientSessions::parse);
  }

  /**
   * Adds to {@code batch}: the result of write {@code seq}, the session's new floor and activity
   * time, and deletions of the results that the client says it no longer needs.
   */
  void record(
      WriteBatch batch,
      byte[] clientId,
      Session session,
      long seq,
      long firstIncomplete,
      KvResponse response,
      long nowMillis) {
    // The floor never goes down, even if requests arrive out of order.
    long floor = Math.max(session.floor(), firstIncomplete);
    if (floor > session.floor()) {
      for (byte[] key : keysIn(resultKey(clientId, session.floor()), resultKey(clientId, floor))) {
        batch.delete(key);
      }
    }
    batch.put(resultKey(clientId, seq), response.toByteArray());
    byte[] value = new byte[16];
    System.arraycopy(KeySpace.uint64(floor), 0, value, 0, 8);
    System.arraycopy(KeySpace.uint64(nowMillis), 0, value, 8, 8);
    batch.put(sessionKey(clientId), value);
  }

  /**
   * Adds to {@code batch} the deletion of every session idle for longer than {@code ttl} at time
   * {@code nowMillis}. In a replicated group the time comes from the log, never from the local
   * clock, so every replica forgets the same sessions.
   *
   * @return how many were forgotten
   */
  int expire(WriteBatch batch, long nowMillis, Duration ttl) {
    List<byte[]> expired = new ArrayList<>();
    byte[][] sessions = sessionSpan();
    int idAt = 2 + scope.length;
    try (KvIterator it = engine.scan(sessions[0], sessions[1])) {
      while (it.hasNext()) {
        KvEntry e = it.next();
        long lastActive = KeySpace.readUint64(e.value(), 8);
        if (nowMillis - lastActive > ttl.toMillis()) {
          expired.add(Arrays.copyOfRange(e.key(), idAt, idAt + CLIENT_ID_BYTES));
        }
      }
    }
    for (byte[] clientId : expired) {
      batch.delete(sessionKey(clientId));
      byte[] prefix = KeySpace.local(KeySpace.RESULT, scope, clientId);
      for (byte[] key : keysIn(prefix, Bytes.prefixEnd(prefix).orElseThrow())) {
        batch.delete(key);
      }
    }
    return expired.size();
  }

  /**
   * Adds to {@code batch} a copy of this whole table (sessions and results) into {@code target}'s
   * scope. A split gives the new range a copy, so a write applied before the split and retried
   * after it is still recognized there.
   */
  void copyTo(WriteBatch batch, ClientSessions target) {
    forEach(target.scope, batch::put);
  }

  /** Every record of this table, with its key rewritten into scope {@code as}. */
  void forEach(byte[] as, BiConsumer<byte[], byte[]> sink) {
    int from = 2 + scope.length;
    for (byte[][] span : spans()) {
      try (KvIterator it = engine.scan(span[0], span[1])) {
        while (it.hasNext()) {
          KvEntry e = it.next();
          byte[] rest = Arrays.copyOfRange(e.key(), from, e.key().length);
          sink.accept(KeySpace.local(e.key()[1], as, rest), e.value());
        }
      }
    }
  }

  /** Adds to {@code batch} the deletion of this whole table. */
  void deleteAll(WriteBatch batch) {
    for (byte[][] span : spans()) {
      for (byte[] key : keysIn(span[0], span[1])) {
        batch.delete(key);
      }
    }
  }

  /** The engine spans this table occupies: results, then sessions. */
  byte[][][] spans() {
    byte[] results = KeySpace.local(KeySpace.RESULT, scope);
    return new byte[][][] {{results, Bytes.prefixEnd(results).orElseThrow()}, sessionSpan()};
  }

  private byte[][] sessionSpan() {
    byte[] sessions = KeySpace.local(KeySpace.SESSION, scope);
    return new byte[][] {sessions, Bytes.prefixEnd(sessions).orElseThrow()};
  }

  /** How many sessions exist; for tests and the debug page. */
  int count() {
    byte[][] sessions = sessionSpan();
    int n = 0;
    try (KvIterator it = engine.scan(sessions[0], sessions[1])) {
      while (it.hasNext()) {
        it.next();
        n++;
      }
    }
    return n;
  }

  private List<byte[]> keysIn(byte[] start, byte[] end) {
    List<byte[]> keys = new ArrayList<>();
    try (KvIterator it = engine.scan(start, end)) {
      while (it.hasNext()) {
        keys.add(it.next().key());
      }
    }
    return keys;
  }

  private byte[] sessionKey(byte[] clientId) {
    return KeySpace.local(KeySpace.SESSION, scope, clientId);
  }

  private byte[] resultKey(byte[] clientId, long seq) {
    return KeySpace.local(KeySpace.RESULT, scope, clientId, KeySpace.uint64(seq));
  }

  /** The session key of a single group (empty scope); for tests. */
  static byte[] sessionKey(byte[] clientId, byte[] scope) {
    return KeySpace.local(KeySpace.SESSION, scope, clientId);
  }

  private static KvResponse parse(byte[] bytes) {
    try {
      return KvResponse.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new CorruptionException("unreadable stored result: " + e.getMessage());
    }
  }
}

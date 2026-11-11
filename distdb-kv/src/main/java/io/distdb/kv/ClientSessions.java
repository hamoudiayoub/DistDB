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

/**
 * The table that makes retried writes safe: for each client, the answers to its recent writes,
 * stored in the engine next to the data (the pattern of section 6.3 of Ongaro's Raft thesis).
 *
 * <p>Records, in the {@link KeySpace#LOCAL local} key space:
 *
 * <pre>
 *   0x01 's' clientId            → floor (8 bytes) | lastActiveMillis (8 bytes)
 *   0x01 'r' clientId seq(8, BE) → the KvResponse sent for that write
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

  ClientSessions(KvEngine engine) {
    this.engine = engine;
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
    byte[] start = KeySpace.local(KeySpace.SESSION);
    byte[] end = KeySpace.local((byte) (KeySpace.SESSION + 1));
    try (KvIterator it = engine.scan(start, end)) {
      while (it.hasNext()) {
        KvEntry e = it.next();
        long lastActive = KeySpace.readUint64(e.value(), 8);
        if (nowMillis - lastActive > ttl.toMillis()) {
          expired.add(Arrays.copyOfRange(e.key(), 2, 2 + CLIENT_ID_BYTES));
        }
      }
    }
    for (byte[] clientId : expired) {
      batch.delete(sessionKey(clientId));
      byte[] prefix = KeySpace.local(KeySpace.RESULT, clientId);
      for (byte[] key : keysIn(prefix, Bytes.prefixEnd(prefix).orElseThrow())) {
        batch.delete(key);
      }
    }
    return expired.size();
  }

  /** How many sessions exist; for tests and the debug page. */
  int count() {
    int n = 0;
    try (KvIterator it =
        engine.scan(
            KeySpace.local(KeySpace.SESSION), KeySpace.local((byte) (KeySpace.SESSION + 1)))) {
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

  static byte[] sessionKey(byte[] clientId) {
    return KeySpace.local(KeySpace.SESSION, clientId);
  }

  static byte[] resultKey(byte[] clientId, long seq) {
    return KeySpace.local(KeySpace.RESULT, clientId, KeySpace.uint64(seq));
  }

  private static KvResponse parse(byte[] bytes) {
    try {
      return KvResponse.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new CorruptionException("unreadable stored result: " + e.getMessage());
    }
  }
}

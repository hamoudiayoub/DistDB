package io.distdb.kv;

import java.util.Arrays;

/**
 * How a node divides its engine's keys between client data and its own bookkeeping. The first byte
 * says which, so the two can never collide, and a scan of client data can never see a node's
 * private records:
 *
 * <pre>
 *   0x01 ...       local: the node's own records (client sessions, Raft logs)
 *   0x02 userKey   client data
 * </pre>
 *
 * <p>CockroachDB uses the same trick: its "local" keys sort before all user data.
 */
public final class KeySpace {

  static final byte LOCAL = 0x01;
  static final byte USER = 0x02;

  /** Bookkeeping kinds inside the local space. */
  static final byte SESSION = 's';

  static final byte RESULT = 'r';

  /**
   * Raft: {@code 'H' group} hard state, {@code 'L' group index} log, {@code 'P' group} snapshot.
   */
  static final byte RAFT_HARD_STATE = 'H';

  static final byte RAFT_LOG = 'L';

  static final byte RAFT_SNAPSHOT = 'P';

  /** {@code 'A' group}: the index of the last log entry applied to the data. */
  static final byte APPLIED = 'A';

  /** {@code 'D' range}: a range's descriptor (replicated). */
  static final byte DESCRIPTOR = 'D';

  /**
   * {@code 'T' range}: left when a replica is destroyed: the conf_ver of the configuration that
   * removed this node. Messages from configurations at or below it are stale.
   */
  static final byte TOMBSTONE = 'T';

  /** {@code 'g' range}: the range's transaction floor and GC safe point (replicated). */
  static final byte TXN_LIMITS = 'g';

  /**
   * The ranges of the replicated state machine: client data, plus client sessions and their results
   * ({@code 'r'} and {@code 's'} are adjacent). Everything else in the local space (Raft logs, the
   * applied index) belongs to this node alone.
   */
  static byte[][][] replicatedRanges() {
    return new byte[][][] {
      {{USER}, {USER + 1}},
      {local(RESULT), local((byte) (SESSION + 1))},
    };
  }

  private KeySpace() {}

  /** The engine key holding client key {@code key}. */
  public static byte[] user(byte[] key) {
    byte[] out = new byte[key.length + 1];
    out[0] = USER;
    System.arraycopy(key, 0, out, 1, key.length);
    return out;
  }

  /** Whether {@code engineKey} holds client data. */
  public static boolean isUser(byte[] engineKey) {
    return engineKey.length > 0 && engineKey[0] == USER;
  }

  /** The client key stored at {@code engineKey}, which must satisfy {@link #isUser}. */
  public static byte[] fromUser(byte[] engineKey) {
    if (!isUser(engineKey)) {
      throw new IllegalArgumentException("not a user key");
    }
    return Arrays.copyOfRange(engineKey, 1, engineKey.length);
  }

  /** The exclusive engine bound for client keys below {@code end}; null means "no bound". */
  public static byte[] userEnd(byte[] end) {
    return end == null ? new byte[] {USER + 1} : user(end);
  }

  /** A local key: {@code 0x01, kind, parts...}. */
  static byte[] local(byte kind, byte[]... parts) {
    int length = 2;
    for (byte[] p : parts) {
      length += p.length;
    }
    byte[] out = new byte[length];
    out[0] = LOCAL;
    out[1] = kind;
    int at = 2;
    for (byte[] p : parts) {
      System.arraycopy(p, 0, out, at, p.length);
      at += p.length;
    }
    return out;
  }

  /** {@code v} as 8 big-endian bytes, which sort like the unsigned number. */
  static byte[] uint64(long v) {
    byte[] out = new byte[8];
    for (int i = 7; i >= 0; i--) {
      out[i] = (byte) v;
      v >>>= 8;
    }
    return out;
  }

  static long readUint64(byte[] b, int offset) {
    long v = 0;
    for (int i = 0; i < 8; i++) {
      v = (v << 8) | (b[offset + i] & 0xFF);
    }
    return v;
  }
}

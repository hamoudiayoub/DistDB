package io.distdb.kv;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.distdb.common.Bytes;
import io.distdb.rpc.KvErrors;
import io.distdb.rpc.proto.CheckTxnStatusRequest;
import io.distdb.rpc.proto.CheckTxnStatusResponse;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.LockInfo;
import io.distdb.rpc.proto.Mutation;
import io.distdb.rpc.proto.PrewriteRequest;
import io.distdb.rpc.proto.PrewriteResponse;
import io.distdb.rpc.proto.RangeDescriptor;
import io.distdb.rpc.proto.ScanLocksRequest;
import io.distdb.rpc.proto.ScanLocksResponse;
import io.distdb.rpc.proto.TxnCommitRequest;
import io.distdb.rpc.proto.TxnCommitResponse;
import io.distdb.rpc.proto.TxnEntry;
import io.distdb.rpc.proto.TxnGetRequest;
import io.distdb.rpc.proto.TxnGetResponse;
import io.distdb.rpc.proto.TxnRollbackRequest;
import io.distdb.rpc.proto.TxnRollbackResponse;
import io.distdb.rpc.proto.TxnScanRequest;
import io.distdb.rpc.proto.TxnScanResponse;
import io.distdb.rpc.proto.WriteRecord;
import io.distdb.storage.KvEngine;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import io.distdb.storage.WriteBatch;
import io.distdb.storage.mvcc.MvccKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Percolator's rules on one range: three column families per key, and the commands that move a
 * transaction through them. Commands are applied from the range's log, so everything here depends
 * only on the command and the engine's state; and each is idempotent, so a repeated command is
 * recognized from that state.
 *
 * <pre>
 *   0x03 key@start_ts   data:  the value a transaction wrote
 *   0x04 key            lock:  {@link LockInfo}, while a transaction is between prewrite and commit
 *   0x05 key@commit_ts  write: {@link WriteRecord}, which makes the data at start_ts visible
 *                              (a ROLLBACK record sits at key@start_ts)
 * </pre>
 *
 * <p>Timestamps are {@code physical_ms << 18 | logical}; a lock's TTL is measured on them.
 */
public final class Percolator {

  static final byte DATA = 0x03;
  static final byte LOCK = 0x04;
  static final byte WRITE = 0x05;
  static final int LOGICAL_BITS = 18;

  /** A range's floor (no prewrite below it) and GC safe point (no read below it). */
  record Limits(long floor, long safePoint) {
    static final Limits NONE = new Limits(0, 0);
  }

  private Percolator() {}

  // -----------------------------------------------------------------------------------------------
  // Keys and spans

  static byte[] dataKey(byte[] key, long startTs) {
    return cf(DATA, MvccKey.encode(key, startTs));
  }

  static byte[] lockKey(byte[] key) {
    return cf(LOCK, MvccKey.prefix(key));
  }

  static byte[] writeKey(byte[] key, long commitTs) {
    return cf(WRITE, MvccKey.encode(key, commitTs));
  }

  /** Every version of {@code key} in column {@code family}: {@code [start, end)}. */
  private static byte[][] versions(byte family, byte[] key) {
    byte[] prefix = cf(family, MvccKey.prefix(key));
    return new byte[][] {prefix, Bytes.prefixEnd(prefix).orElseThrow()};
  }

  /**
   * The engine spans of {@code d}'s transactional data, one per column. The escaped key encoding
   * sorts like the keys, so the keys of a span map to a span of each column.
   */
  static List<byte[][]> spans(RangeDescriptor d) {
    List<byte[][]> out = new ArrayList<>();
    for (byte family : new byte[] {DATA, LOCK, WRITE}) {
      byte[] start = cf(family, MvccKey.prefix(d.getStartKey().toByteArray()));
      byte[] end =
          d.hasEndKey()
              ? cf(family, MvccKey.prefix(d.getEndKey().toByteArray()))
              : new byte[] {(byte) (family + 1)};
      out.add(new byte[][] {start, end});
    }
    return out;
  }

  /** The client key of an engine key in any of the three columns. */
  static byte[] userKeyOf(byte[] engineKey) {
    return MvccKey.userKeyOf(java.util.Arrays.copyOfRange(engineKey, 1, engineKey.length));
  }

  private static byte[] cf(byte family, byte[] rest) {
    byte[] out = new byte[rest.length + 1];
    out[0] = family;
    System.arraycopy(rest, 0, out, 1, rest.length);
    return out;
  }

  static long physicalMillis(long ts) {
    return ts >>> LOGICAL_BITS;
  }

  // -----------------------------------------------------------------------------------------------
  // Commands (applied from the log)

  static KvResponse prewrite(WriteBatch batch, KvEngine engine, PrewriteRequest p, Limits limits) {
    Mutation m = p.getMutation();
    byte[] key = m.getKey().toByteArray();
    long start = p.getStartTs();
    if (start < limits.floor()) {
      return tooOld(limits.floor());
    }
    Optional<LockInfo> lock = lock(engine, key);
    if (lock.isPresent()) {
      if (lock.get().getStartTs() == start) {
        return ok(KvResponse.newBuilder().setPrewrite(PrewriteResponse.getDefaultInstance()));
      }
      return KvErrors.keyLocked(
          lock.get(), "locked by the transaction at " + lock.get().getStartTs());
    }
    try (KvIterator it = scan(engine, versions(WRITE, key))) {
      while (it.hasNext()) {
        KvEntry e = it.next();
        long commit = MvccKey.timestampOf(java.util.Arrays.copyOfRange(e.key(), 1, e.key().length));
        WriteRecord w = record(e.value());
        if (commit < start) {
          break; // older than us: no conflict
        }
        if (w.getStartTs() == start) {
          if (w.getKind() == WriteRecord.Kind.ROLLBACK) {
            return KvErrors.txnAborted("the transaction at " + start + " was rolled back");
          }
          // Already committed here: a late copy of our own prewrite. Change nothing.
          return ok(KvResponse.newBuilder().setPrewrite(PrewriteResponse.getDefaultInstance()));
        }
        if (w.getKind() != WriteRecord.Kind.ROLLBACK) {
          return KvErrors.writeConflict(
              commit, "a write committed at " + commit + " after " + start);
        }
      }
    }
    if (m.getOp() == Mutation.Op.PUT) {
      batch.put(dataKey(key, start), m.getValue().toByteArray());
    }
    batch.put(
        lockKey(key),
        LockInfo.newBuilder()
            .setKey(m.getKey())
            .setPrimary(p.getPrimary())
            .setStartTs(start)
            .setTtlMs(p.getTtlMs())
            .setOp(m.getOp())
            .build()
            .toByteArray());
    return ok(KvResponse.newBuilder().setPrewrite(PrewriteResponse.getDefaultInstance()));
  }

  static KvResponse commit(WriteBatch batch, KvEngine engine, TxnCommitRequest c) {
    byte[] key = c.getKey().toByteArray();
    long start = c.getStartTs();
    Optional<LockInfo> lock = lock(engine, key);
    if (lock.isPresent() && lock.get().getStartTs() == start) {
      if (c.getCommitTs() <= start) {
        return KvErrors.invalidArgument("commit_ts " + c.getCommitTs() + " is not after " + start);
      }
      WriteRecord.Kind kind =
          lock.get().getOp() == Mutation.Op.PUT ? WriteRecord.Kind.PUT : WriteRecord.Kind.DELETE;
      batch.put(
          writeKey(key, c.getCommitTs()),
          WriteRecord.newBuilder().setKind(kind).setStartTs(start).build().toByteArray());
      batch.delete(lockKey(key));
      return ok(KvResponse.newBuilder().setTxnCommit(TxnCommitResponse.getDefaultInstance()));
    }
    Optional<long[]> done = outcome(engine, key, start);
    if (done.isPresent() && done.get()[1] == 1) {
      return ok(KvResponse.newBuilder().setTxnCommit(TxnCommitResponse.getDefaultInstance()));
    }
    return KvErrors.txnAborted("the transaction at " + start + " holds no lock on this key");
  }

  static KvResponse rollback(WriteBatch batch, KvEngine engine, TxnRollbackRequest r) {
    byte[] key = r.getKey().toByteArray();
    long start = r.getStartTs();
    Optional<long[]> done = outcome(engine, key, start);
    if (done.isPresent() && done.get()[1] == 1) {
      return KvErrors.txnCommitted(done.get()[0], "already committed at " + done.get()[0]);
    }
    rollBack(batch, engine, key, start, done.isPresent());
    return ok(KvResponse.newBuilder().setTxnRollback(TxnRollbackResponse.getDefaultInstance()));
  }

  static KvResponse checkTxnStatus(
      WriteBatch batch, KvEngine engine, CheckTxnStatusRequest c, Limits limits) {
    byte[] key = c.getPrimary().toByteArray();
    long start = c.getStartTs();
    Optional<LockInfo> lock = lock(engine, key);
    CheckTxnStatusResponse.Builder out = CheckTxnStatusResponse.newBuilder();
    if (lock.isPresent() && lock.get().getStartTs() == start) {
      boolean expired =
          physicalMillis(c.getCurrentTs()) >= physicalMillis(start) + lock.get().getTtlMs();
      if (!expired && start >= limits.floor()) {
        return ok(
            KvResponse.newBuilder()
                .setCheckTxnStatus(
                    out.setStatus(CheckTxnStatusResponse.Status.LOCKED)
                        .setTtlMs(lock.get().getTtlMs())));
      }
      rollBack(batch, engine, key, start, false); // dead: its TTL ran out, or it outlived GC
      return ok(
          KvResponse.newBuilder()
              .setCheckTxnStatus(out.setStatus(CheckTxnStatusResponse.Status.ROLLED_BACK)));
    }
    Optional<long[]> done = outcome(engine, key, start);
    if (done.isPresent() && done.get()[1] == 1) {
      return ok(
          KvResponse.newBuilder()
              .setCheckTxnStatus(
                  out.setStatus(CheckTxnStatusResponse.Status.COMMITTED)
                      .setCommitTs(done.get()[0])));
    }
    // Rolled back already, or never prewritten here: a ROLLBACK record makes sure that a prewrite
    // still on its way is refused.
    rollBack(batch, engine, key, start, done.isPresent());
    return ok(
        KvResponse.newBuilder()
            .setCheckTxnStatus(out.setStatus(CheckTxnStatusResponse.Status.ROLLED_BACK)));
  }

  /** Removes the transaction's lock and data on {@code key}, and leaves a ROLLBACK record. */
  private static void rollBack(
      WriteBatch batch, KvEngine engine, byte[] key, long start, boolean recordExists) {
    Optional<LockInfo> lock = lock(engine, key);
    if (lock.isPresent() && lock.get().getStartTs() == start) {
      batch.delete(lockKey(key));
      batch.delete(dataKey(key, start));
    }
    if (!recordExists) {
      batch.put(
          writeKey(key, start),
          WriteRecord.newBuilder()
              .setKind(WriteRecord.Kind.ROLLBACK)
              .setStartTs(start)
              .build()
              .toByteArray());
    }
  }

  /**
   * How the transaction at {@code start} ended on {@code key}, from its write records: {@code
   * [commit_ts, 1]} if it committed, {@code [start, 0]} if it was rolled back, empty if neither.
   */
  private static Optional<long[]> outcome(KvEngine engine, byte[] key, long start) {
    try (KvIterator it = scan(engine, versions(WRITE, key))) {
      while (it.hasNext()) {
        KvEntry e = it.next();
        long ts = MvccKey.timestampOf(java.util.Arrays.copyOfRange(e.key(), 1, e.key().length));
        if (ts < start) {
          return Optional.empty(); // records are newest first: nothing of ours below here
        }
        WriteRecord w = record(e.value());
        if (w.getStartTs() == start) {
          return Optional.of(
              w.getKind() == WriteRecord.Kind.ROLLBACK
                  ? new long[] {start, 0}
                  : new long[] {ts, 1});
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Garbage-collects {@code d}'s span at {@code safePoint}: for each key, every version newer than
   * it stays, and so does the newest one at or below it unless that one is a delete; the rest go,
   * with their data. Locks are untouched (the GC worker resolved old ones first).
   */
  static void gc(WriteBatch batch, KvEngine engine, RangeDescriptor d, long safePoint) {
    byte[][] writes = spans(d).get(2);
    byte[] current = null;
    boolean keptOne = false;
    try (KvIterator it = scan(engine, writes)) {
      while (it.hasNext()) {
        KvEntry e = it.next();
        byte[] mvcc = java.util.Arrays.copyOfRange(e.key(), 1, e.key().length);
        byte[] key = MvccKey.userKeyOf(mvcc);
        long ts = MvccKey.timestampOf(mvcc);
        if (current == null || !java.util.Arrays.equals(current, key)) {
          current = key;
          keptOne = false;
        }
        if (ts > safePoint) {
          continue;
        }
        WriteRecord w = record(e.value());
        if (w.getKind() == WriteRecord.Kind.ROLLBACK) {
          if (ts < safePoint) {
            batch.delete(e.key()); // the floor now refuses its late prewrites anyway
          }
          continue;
        }
        if (!keptOne) {
          keptOne = true; // the version a read at the safe point sees
          if (w.getKind() == WriteRecord.Kind.PUT) {
            continue;
          }
        }
        batch.delete(e.key());
        batch.delete(dataKey(key, w.getStartTs()));
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Reads (served after ReadIndex, not through the log)

  static KvResponse get(KvEngine engine, TxnGetRequest g, Limits limits) {
    if (g.getReadTs() < limits.safePoint()) {
      return tooOld(limits.safePoint());
    }
    byte[] key = g.getKey().toByteArray();
    Optional<LockInfo> lock = lock(engine, key);
    if (lock.isPresent() && lock.get().getStartTs() <= g.getReadTs()) {
      return KvErrors.keyLocked(
          lock.get(), "locked by the transaction at " + lock.get().getStartTs());
    }
    TxnGetResponse.Builder out = TxnGetResponse.newBuilder();
    visible(engine, key, g.getReadTs())
        .ifPresent(v -> out.setFound(true).setValue(ByteString.copyFrom(v)));
    return ok(KvResponse.newBuilder().setTxnGet(out));
  }

  static KvResponse scan(KvEngine engine, TxnScanRequest s, Limits limits, int maxLimit) {
    if (s.getReadTs() < limits.safePoint()) {
      return tooOld(limits.safePoint());
    }
    int limit = s.getLimit() <= 0 || s.getLimit() > maxLimit ? maxLimit : s.getLimit();
    byte[] start = s.getStartKey().toByteArray();
    byte[] end = s.hasEndKey() ? s.getEndKey().toByteArray() : null;
    // Every key with a lock or a write record in the span, in order.
    TreeSet<byte[]> keys = new TreeSet<>(Bytes.COMPARATOR);
    for (byte family : new byte[] {LOCK, WRITE}) {
      byte[] from = cf(family, MvccKey.prefix(start));
      byte[] to = end != null ? cf(family, MvccKey.prefix(end)) : new byte[] {(byte) (family + 1)};
      try (KvIterator it = engine.scan(from, to)) {
        while (it.hasNext()) {
          keys.add(userKeyOf(it.next().key()));
        }
      }
    }
    TxnScanResponse.Builder out = TxnScanResponse.newBuilder();
    for (byte[] key : keys) {
      if (out.getEntriesCount() == limit) {
        out.setResumeKey(ByteString.copyFrom(key));
        break;
      }
      Optional<LockInfo> lock = lock(engine, key);
      if (lock.isPresent() && lock.get().getStartTs() <= s.getReadTs()) {
        return KvErrors.keyLocked(
            lock.get(), "locked by the transaction at " + lock.get().getStartTs());
      }
      visible(engine, key, s.getReadTs())
          .ifPresent(v ->
              out.addEntries(
                  TxnEntry.newBuilder()
                      .setKey(ByteString.copyFrom(key))
                      .setValue(ByteString.copyFrom(v))));
    }
    return ok(KvResponse.newBuilder().setTxnScan(out));
  }

  /**
   * Runs a pushed-down program over the entries of {@code [start, end)} visible at {@code read_ts},
   * with the same rules as {@link #scan}: a lock at or below the snapshot stops it with KeyLocked,
   * and the reader resolves it. The program may stop early; the response then says where to resume.
   */
  static KvResponse coprocessor(
      KvEngine engine,
      io.distdb.rpc.proto.CoprocessorRequest c,
      Limits limits,
      io.distdb.rpc.Coprocessor runner,
      int self) {
    if (runner == null) {
      return KvErrors.invalidArgument("this node runs no coprocessor");
    }
    if (c.getReadTs() < limits.safePoint()) {
      return tooOld(limits.safePoint());
    }
    io.distdb.rpc.Coprocessor.Run run;
    try {
      run = runner.start(c.getProgram().toByteArray());
    } catch (IllegalArgumentException e) {
      return KvErrors.invalidArgument("bad coprocessor program: " + e.getMessage());
    }
    byte[] start = c.getStartKey().toByteArray();
    byte[] end = c.hasEndKey() ? c.getEndKey().toByteArray() : null;
    TreeSet<byte[]> keys = new TreeSet<>(Bytes.COMPARATOR);
    for (byte family : new byte[] {LOCK, WRITE}) {
      byte[] from = cf(family, MvccKey.prefix(start));
      byte[] to = end != null ? cf(family, MvccKey.prefix(end)) : new byte[] {(byte) (family + 1)};
      try (KvIterator it = engine.scan(from, to)) {
        while (it.hasNext()) {
          keys.add(userKeyOf(it.next().key()));
        }
      }
    }
    io.distdb.rpc.proto.CoprocessorResponse.Builder out =
        io.distdb.rpc.proto.CoprocessorResponse.newBuilder().setServedBy(self);
    long rows = 0;
    long bytes = 0;
    java.util.Iterator<byte[]> it = keys.iterator();
    while (it.hasNext()) {
      byte[] key = it.next();
      Optional<LockInfo> lock = lock(engine, key);
      if (lock.isPresent() && lock.get().getStartTs() <= c.getReadTs()) {
        return KvErrors.keyLocked(
            lock.get(), "locked by the transaction at " + lock.get().getStartTs());
      }
      Optional<byte[]> v = visible(engine, key, c.getReadTs());
      if (v.isEmpty()) {
        continue;
      }
      rows++;
      bytes += key.length + v.get().length;
      if (!run.accept(key, v.get())) {
        if (run.satisfied()) {
          out.setSatisfied(true);
        } else if (it.hasNext()) {
          out.setResumeKey(ByteString.copyFrom(it.next()));
        }
        break;
      }
    }
    return ok(
        KvResponse.newBuilder()
            .setCoprocessor(
                out.setResult(ByteString.copyFrom(run.finish()))
                    .setRowsScanned(rows)
                    .setBytesScanned(bytes)));
  }

  /** Locks with {@code start_ts < max_ts} from {@code start_key} to the end of {@code d}. */
  static KvResponse scanLocks(KvEngine engine, ScanLocksRequest s, RangeDescriptor d) {
    byte[] from = cf(LOCK, MvccKey.prefix(s.getStartKey().toByteArray()));
    byte[] to = spans(d).get(1)[1];
    int limit = s.getLimit() <= 0 ? 1000 : s.getLimit();
    ScanLocksResponse.Builder out = ScanLocksResponse.newBuilder();
    try (KvIterator it = engine.scan(from, to)) {
      while (it.hasNext()) {
        LockInfo lock = parseLock(it.next().value());
        if (lock.getStartTs() >= s.getMaxTs()) {
          continue;
        }
        if (out.getLocksCount() == limit) {
          out.setResumeKey(lock.getKey());
          break;
        }
        out.addLocks(lock);
      }
    }
    return ok(KvResponse.newBuilder().setScanLocks(out));
  }

  /** The value of {@code key} a read at {@code ts} sees, ignoring locks. */
  private static Optional<byte[]> visible(KvEngine engine, byte[] key, long ts) {
    byte[] from = writeKey(key, ts);
    byte[] to = versions(WRITE, key)[1];
    try (KvIterator it = engine.scan(from, to)) {
      while (it.hasNext()) {
        WriteRecord w = record(it.next().value());
        switch (w.getKind()) {
          case ROLLBACK -> {}
          case DELETE -> {
            return Optional.empty();
          }
          case PUT -> {
            return Optional.of(
                engine
                    .get(dataKey(key, w.getStartTs()))
                    .orElseThrow(
                        () -> new IllegalStateException("a committed version has no data")));
          }
          case UNRECOGNIZED -> throw new IllegalStateException("unreadable write record");
        }
      }
    }
    return Optional.empty();
  }

  static Optional<LockInfo> lock(KvEngine engine, byte[] key) {
    return engine.get(lockKey(key)).map(Percolator::parseLock);
  }

  /** The smallest start_ts of any lock in {@code d}'s span, or {@link Long#MAX_VALUE}. */
  static long minLockTs(KvEngine engine, RangeDescriptor d) {
    long min = Long.MAX_VALUE;
    try (KvIterator it = scan(engine, spans(d).get(1))) {
      while (it.hasNext()) {
        min = Math.min(min, parseLock(it.next().value()).getStartTs());
      }
    }
    return min;
  }

  private static KvIterator scan(KvEngine engine, byte[][] span) {
    return engine.scan(span[0], span[1]);
  }

  private static KvResponse tooOld(long floor) {
    return KvErrors.txnTooOld(floor, "below this range's floor or GC safe point " + floor);
  }

  private static KvResponse ok(KvResponse.Builder b) {
    return b.build();
  }

  private static LockInfo parseLock(byte[] bytes) {
    try {
      return LockInfo.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("unreadable lock", e);
    }
  }

  private static WriteRecord record(byte[] bytes) {
    try {
      return WriteRecord.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("unreadable write record", e);
    }
  }
}

package io.distdb.sql.testing;

import io.distdb.common.Bytes;
import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.kv.KvStore;
import io.distdb.sql.kv.KvTxn;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * An in-memory multi-version store with the isolation Phase 7 gives: snapshot reads, buffered
 * writes, first-committer-wins at commit, and fences. For fast tests of the SQL layer; the
 * cluster's store is {@link io.distdb.sql.kv.TxnKvStore}. Thread-safe.
 */
public final class MemoryKvStore implements KvStore {

  /** One version of a key: written at {@code ts}, a value or (null) a delete. */
  private record Version(long ts, byte[] value) {}

  private final TreeMap<byte[], List<Version>> data = new TreeMap<>(Bytes.COMPARATOR);
  private final Set<Txn> open = new HashSet<>();
  private long clock;
  private long floor;

  @Override
  public synchronized KvTxn begin() {
    Txn t = new Txn(++clock);
    open.add(t);
    return t;
  }

  @Override
  public synchronized long[] timestamps(int n) {
    long[] out = new long[n];
    for (int k = 0; k < n; k++) {
      out[k] = ++clock;
    }
    return out;
  }

  @Override
  public synchronized void fence(long ts) {
    floor = Math.max(floor, ts);
  }

  /** How many keys hold a live value (for tests that check DROP cleans up). */
  public synchronized int liveKeys() {
    int n = 0;
    for (List<Version> versions : data.values()) {
      if (versions.get(versions.size() - 1).value() != null) {
        n++;
      }
    }
    return n;
  }

  private synchronized Optional<byte[]> read(byte[] key, long ts) {
    List<Version> versions = data.get(key);
    if (versions == null) {
      return Optional.empty();
    }
    for (int k = versions.size() - 1; k >= 0; k--) {
      Version v = versions.get(k);
      if (v.ts() <= ts) {
        return Optional.ofNullable(v.value());
      }
    }
    return Optional.empty();
  }

  private synchronized List<KvTxn.Entry> readRange(byte[] start, byte[] end, long ts) {
    NavigableMap<byte[], List<Version>> span =
        end == null ? data.tailMap(start, true) : data.subMap(start, true, end, false);
    List<KvTxn.Entry> out = new ArrayList<>();
    for (Map.Entry<byte[], List<Version>> e : span.entrySet()) {
      List<Version> versions = e.getValue();
      for (int k = versions.size() - 1; k >= 0; k--) {
        Version v = versions.get(k);
        if (v.ts() <= ts) {
          if (v.value() != null) {
            out.add(new KvTxn.Entry(e.getKey(), v.value()));
          }
          break;
        }
      }
    }
    return out;
  }

  private synchronized long commit(Txn t) {
    open.remove(t);
    if (t.writes.isEmpty()) {
      return t.start;
    }
    if (t.start < floor) {
      throw new SqlException(
          SqlState.SERIALIZATION_FAILURE, "the transaction is too old: a schema change passed it");
    }
    for (byte[] k : t.writes.keySet()) {
      List<Version> versions = data.get(k);
      if (versions != null && versions.get(versions.size() - 1).ts() > t.start) {
        throw new SqlException(
            SqlState.SERIALIZATION_FAILURE, "could not serialize access due to concurrent update");
      }
    }
    long ts = ++clock;
    for (Map.Entry<byte[], byte[]> w : t.writes.entrySet()) {
      data.computeIfAbsent(w.getKey(), x -> new ArrayList<>()).add(new Version(ts, w.getValue()));
    }
    return ts;
  }

  private synchronized void abandon(Txn t) {
    open.remove(t);
  }

  private final class Txn implements KvTxn {
    final long start;

    /** Buffered writes; a null value is a delete. */
    final TreeMap<byte[], byte[]> writes = new TreeMap<>(Bytes.COMPARATOR);

    boolean done;

    Txn(long start) {
      this.start = start;
    }

    @Override
    public long startTs() {
      return start;
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
      if (writes.containsKey(key)) {
        return Optional.ofNullable(writes.get(key));
      }
      return read(key, start);
    }

    @Override
    public List<Entry> scan(byte[] startKey, byte[] end, int limit) {
      TreeMap<byte[], byte[]> merged = new TreeMap<>(Bytes.COMPARATOR);
      for (Entry e : readRange(startKey, end, start)) {
        merged.put(e.key(), e.value());
      }
      NavigableMap<byte[], byte[]> own =
          end == null ? writes.tailMap(startKey, true) : writes.subMap(startKey, true, end, false);
      for (Map.Entry<byte[], byte[]> w : own.entrySet()) {
        if (w.getValue() == null) {
          merged.remove(w.getKey());
        } else {
          merged.put(w.getKey(), w.getValue());
        }
      }
      List<Entry> out = new ArrayList<>();
      for (Map.Entry<byte[], byte[]> e : merged.entrySet()) {
        if (limit > 0 && out.size() == limit) {
          break;
        }
        out.add(new Entry(e.getKey(), e.getValue()));
      }
      return out;
    }

    @Override
    public void put(byte[] key, byte[] value) {
      writes.put(key.clone(), value.clone());
    }

    @Override
    public void delete(byte[] key) {
      writes.put(key.clone(), null);
    }

    @Override
    public long commit() {
      done = true;
      return MemoryKvStore.this.commit(this);
    }

    @Override
    public void rollback() {
      if (!done) {
        done = true;
        abandon(this);
      }
    }
  }
}

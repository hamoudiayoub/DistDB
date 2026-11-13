package io.distdb.sql.catalog;

import io.distdb.common.Bytes;
import io.distdb.common.encoding.KeyReader;
import io.distdb.common.encoding.KeyWriter;
import io.distdb.common.encoding.ValueType;
import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.catalog.TableDesc.Column;
import io.distdb.sql.catalog.TableDesc.Index;
import io.distdb.sql.catalog.TableDesc.IndexState;
import io.distdb.sql.kv.KvTxn;
import io.distdb.sql.types.Type;
import io.distdb.sql.types.TypeSpec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Table and index definitions, stored as keys in the reserved table 1, and cached per catalog
 * version:
 *
 * <pre>
 *   /1/"version"          → bumped by every schema change
 *   /1/"next_id"          → the next table id (100 and up)
 *   /1/"names"/{name}     → which table or index has that name
 *   /1/"desc"/{table id}  → the table's descriptor
 * </pre>
 *
 * A transaction reads the version in its own snapshot ({@link #load}) and gets the descriptors of
 * that snapshot: from the cache when the version matches, else read again. Schema changes are
 * transactions too: two of them writing the same name or the same counter conflict, and one
 * retries.
 */
public final class Catalog {

  public static final long SYSTEM_TABLE = 1;
  public static final long FIRST_TABLE_ID = 100;
  private static final int FORMAT = 1;

  /** The descriptors of one catalog version. */
  public record Snapshot(
      long version,
      Map<String, TableDesc> tables,
      Map<Long, io.distdb.sql.stats.TableStats> stats) {

    /** The table's statistics, or null if it was never analyzed. */
    public io.distdb.sql.stats.TableStats statsOf(TableDesc t) {
      return stats.get(t.id());
    }

    public Optional<TableDesc> find(String name) {
      return Optional.ofNullable(tables.get(name));
    }

    public TableDesc table(String name) {
      TableDesc t = tables.get(name);
      if (t == null) {
        throw new SqlException(
            SqlState.UNDEFINED_TABLE, "relation \"" + name + "\" does not exist");
      }
      return t;
    }

    /** The table that has an index named {@code name}, if any. */
    public Optional<TableDesc> tableOfIndex(String name) {
      for (TableDesc t : tables.values()) {
        if (t.primary().name().equals(name)) {
          return Optional.of(t);
        }
        for (Index i : t.indexes()) {
          if (i.name().equals(name)) {
            return Optional.of(t);
          }
        }
      }
      return Optional.empty();
    }

    /** Whether a table or an index already has this name (they share one namespace). */
    public boolean nameTaken(String name) {
      return tables.containsKey(name) || tableOfIndex(name).isPresent();
    }
  }

  private volatile Snapshot cached = new Snapshot(-1, Map.of(), Map.of());

  /** The catalog as of {@code txn}'s snapshot (its own schema writes included). */
  public Snapshot load(KvTxn txn) {
    long version = txn.get(versionKey()).map(Catalog::decodeLong).orElse(0L);
    Snapshot c = cached;
    if (c.version() == version) {
      return c;
    }
    Map<String, TableDesc> tables = new HashMap<>();
    byte[] start = descPrefix();
    byte[] end = Bytes.prefixEnd(start).orElseThrow();
    for (KvTxn.Entry e : txn.scan(start, end, 0)) {
      TableDesc t = decode(e.value());
      tables.put(t.name(), t);
    }
    Map<Long, io.distdb.sql.stats.TableStats> stats = new HashMap<>();
    byte[] statsStart = statsPrefix();
    for (KvTxn.Entry e : txn.scan(statsStart, Bytes.prefixEnd(statsStart).orElseThrow(), 0)) {
      stats.put(statsTableId(e.key()), io.distdb.sql.stats.TableStats.decode(e.value()));
    }
    Snapshot fresh = new Snapshot(version, Map.copyOf(tables), Map.copyOf(stats));
    // A version this transaction wrote itself is not committed: never cache it.
    if (version > c.version() && version != txn.startTs()) {
      cached = fresh;
    }
    return fresh;
  }

  // -----------------------------------------------------------------------------------------------
  // Writes, inside a schema-changing transaction

  /** Allocates a table id. */
  public static long allocateTableId(KvTxn txn) {
    long id = txn.get(nextIdKey()).map(Catalog::decodeLong).orElse(FIRST_TABLE_ID);
    txn.put(nextIdKey(), encodeLong(id + 1));
    return id;
  }

  /** Writes {@code t}'s descriptor and bumps the catalog version past {@code version}. */
  public static void put(KvTxn txn, Snapshot snapshot, TableDesc t) {
    txn.put(descKey(t.id()), encode(t));
    bump(txn, snapshot);
  }

  public static void remove(KvTxn txn, Snapshot snapshot, TableDesc t) {
    txn.delete(descKey(t.id()));
    txn.delete(statsKey(t.id()));
    bump(txn, snapshot);
  }

  /**
   * The new version is the changing transaction's start timestamp: unique, and increasing in commit
   * order, since two schema changes both write this key and cannot overlap.
   */
  private static void bump(KvTxn txn, Snapshot snapshot) {
    txn.put(versionKey(), encodeLong(Math.max(txn.startTs(), snapshot.version() + 1)));
  }

  /**
   * Claims {@code name} for a table or index: concurrent schema changes claiming the same name
   * write the same key, so one of them fails at commit.
   */
  public static void claimName(KvTxn txn, String name, long tableId) {
    txn.put(nameKey(name), encodeLong(tableId));
  }

  public static void releaseName(KvTxn txn, String name) {
    txn.delete(nameKey(name));
  }

  // -----------------------------------------------------------------------------------------------
  // Keys

  public static byte[] versionKey() {
    return new KeyWriter().writeLong(SYSTEM_TABLE).writeString("version").toByteArray();
  }

  static byte[] nextIdKey() {
    return new KeyWriter().writeLong(SYSTEM_TABLE).writeString("next_id").toByteArray();
  }

  static byte[] nameKey(String name) {
    return new KeyWriter()
        .writeLong(SYSTEM_TABLE)
        .writeString("names")
        .writeString(name)
        .toByteArray();
  }

  /** Writes a table's statistics (and bumps the version, so plans see them). */
  public static void putStats(
      KvTxn txn, Snapshot snapshot, long tableId, io.distdb.sql.stats.TableStats stats) {
    txn.put(statsKey(tableId), stats.encode());
    bump(txn, snapshot);
  }

  static byte[] statsPrefix() {
    return new KeyWriter().writeLong(SYSTEM_TABLE).writeString("stats").toByteArray();
  }

  static byte[] statsKey(long id) {
    return new KeyWriter().writeLong(SYSTEM_TABLE).writeString("stats").writeLong(id).toByteArray();
  }

  private static long statsTableId(byte[] key) {
    KeyReader r = new KeyReader(key);
    r.readLong();
    r.readString();
    return r.readLong();
  }

  static byte[] descPrefix() {
    return new KeyWriter().writeLong(SYSTEM_TABLE).writeString("desc").toByteArray();
  }

  static byte[] descKey(long id) {
    return new KeyWriter().writeLong(SYSTEM_TABLE).writeString("desc").writeLong(id).toByteArray();
  }

  static byte[] encodeLong(long v) {
    return new KeyWriter().writeLong(v).toByteArray();
  }

  static long decodeLong(byte[] b) {
    return new KeyReader(b).readLong();
  }

  // -----------------------------------------------------------------------------------------------
  // Descriptors, as tuples

  public static byte[] encode(TableDesc t) {
    KeyWriter w = new KeyWriter(128);
    w.writeLong(FORMAT).writeLong(t.id()).writeString(t.name());
    w.writeLong(t.nextColumnId()).writeLong(t.nextIndexId());
    w.writeLong(t.columns().size());
    for (Column c : t.columns()) {
      w.writeLong(c.id()).writeString(c.name());
      w.writeLong(c.type().type().ordinal());
      w.writeLong(c.type().length()).writeLong(c.type().precision()).writeLong(c.type().scale());
      w.writeBoolean(c.notNull()).writeBoolean(c.hidden());
      if (c.defaultSql() == null) {
        w.writeNull();
      } else {
        w.writeString(c.defaultSql());
      }
    }
    writeIndex(w, t.primary());
    w.writeLong(t.indexes().size());
    for (Index i : t.indexes()) {
      writeIndex(w, i);
    }
    return w.toByteArray();
  }

  private static void writeIndex(KeyWriter w, Index i) {
    w.writeLong(i.id()).writeString(i.name()).writeBoolean(i.unique());
    w.writeLong(i.state().ordinal()).writeLong(i.columnIds().size());
    for (int c : i.columnIds()) {
      w.writeLong(c);
    }
  }

  public static TableDesc decode(byte[] bytes) {
    KeyReader r = new KeyReader(bytes);
    long format = r.readLong();
    if (format != FORMAT) {
      throw new SqlException(SqlState.DATA_CORRUPTED, "unknown descriptor format " + format);
    }
    long id = r.readLong();
    String name = r.readString();
    int nextColumnId = (int) r.readLong();
    int nextIndexId = (int) r.readLong();
    int ncols = (int) r.readLong();
    List<Column> columns = new ArrayList<>(ncols);
    for (int k = 0; k < ncols; k++) {
      int cid = (int) r.readLong();
      String cname = r.readString();
      Type type = Type.values()[(int) r.readLong()];
      TypeSpec spec =
          new TypeSpec(type, (int) r.readLong(), (int) r.readLong(), (int) r.readLong());
      boolean notNull = r.readBoolean();
      boolean hidden = r.readBoolean();
      String defaultSql;
      if (r.peekType() == ValueType.NULL) {
        r.readNull();
        defaultSql = null;
      } else {
        defaultSql = r.readString();
      }
      columns.add(new Column(cid, cname, spec, notNull, hidden, defaultSql));
    }
    Index primary = readIndex(r);
    int nidx = (int) r.readLong();
    List<Index> indexes = new ArrayList<>(nidx);
    for (int k = 0; k < nidx; k++) {
      indexes.add(readIndex(r));
    }
    return new TableDesc(id, name, columns, primary, indexes, nextColumnId, nextIndexId);
  }

  private static Index readIndex(KeyReader r) {
    int id = (int) r.readLong();
    String name = r.readString();
    boolean unique = r.readBoolean();
    IndexState state = IndexState.values()[(int) r.readLong()];
    int n = (int) r.readLong();
    List<Integer> cols = new ArrayList<>(n);
    for (int k = 0; k < n; k++) {
      cols.add((int) r.readLong());
    }
    return new Index(id, name, cols, unique, state);
  }
}

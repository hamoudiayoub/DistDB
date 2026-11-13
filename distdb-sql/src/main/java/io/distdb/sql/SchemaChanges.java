package io.distdb.sql;

import io.distdb.common.Bytes;
import io.distdb.sql.catalog.Catalog;
import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.catalog.TableDesc.Column;
import io.distdb.sql.catalog.TableDesc.Index;
import io.distdb.sql.catalog.TableDesc.IndexState;
import io.distdb.sql.encoding.RowCodec;
import io.distdb.sql.kv.KvStore;
import io.distdb.sql.kv.KvTxn;
import io.distdb.sql.parse.Ast;
import io.distdb.sql.parse.Parser;
import io.distdb.sql.plan.Binder;
import io.distdb.sql.types.Type;
import io.distdb.sql.types.TypeSpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Schema changes. {@code CREATE TABLE} is one transaction. The others must not lose a concurrent
 * writer's rows, and a schema read is not a write, so snapshot isolation alone would let a writer
 * with the old schema commit after the change (write skew). They run in steps, with a <b>fence</b>
 * between them ({@link KvStore#fence}): after it, no transaction that started before the new schema
 * was published can still commit.
 *
 * <ul>
 *   <li>{@code CREATE INDEX}: add the index WRITE_ONLY (writers maintain it), fence, backfill every
 *       row and mark it PUBLIC.
 *   <li>{@code DROP INDEX} / {@code DROP TABLE}: remove the definition, fence, delete the keys.
 * </ul>
 */
final class SchemaChanges {

  /** Attempts for each step's transaction when it conflicts. */
  private static final int ATTEMPTS = 10;

  private SchemaChanges() {}

  // -----------------------------------------------------------------------------------------------
  // CREATE TABLE

  /** Writes a new table's descriptor in {@code txn}; null if it existed and IF NOT EXISTS. */
  static Result createTable(Catalog catalog, KvTxn txn, Ast.CreateTable ct) {
    Catalog.Snapshot snapshot = catalog.load(txn);
    if (snapshot.nameTaken(ct.name())) {
      if (ct.ifNotExists()) {
        return Result.command(
            "CREATE TABLE", "relation \"" + ct.name() + "\" already exists, skipping");
      }
      throw new SqlException(
          SqlState.DUPLICATE_TABLE, "relation \"" + ct.name() + "\" already exists");
    }
    List<Column> columns = new ArrayList<>();
    Set<String> names = new HashSet<>();
    List<String> pk = ct.primaryKey();
    List<List<String>> uniques = new ArrayList<>(ct.uniques());
    int id = 1;
    for (Ast.ColumnDef c : ct.columns()) {
      if (!names.add(c.name())) {
        throw new SqlException(
            SqlState.DUPLICATE_COLUMN, "column \"" + c.name() + "\" specified more than once");
      }
      if (c.primaryKey()) {
        if (pk != null) {
          throw new SqlException(
              SqlState.INVALID_TABLE_DEFINITION,
              "multiple primary keys for table \"" + ct.name() + "\" are not allowed");
        }
        pk = List.of(c.name());
      }
      if (c.unique()) {
        uniques.add(List.of(c.name()));
      }
      columns.add(new Column(id++, c.name(), c.type(), c.notNull(), false, c.defaultSql()));
    }
    if (pk != null) {
      for (String name : pk) {
        int p = position(columns, name);
        if (p < 0) {
          throw new SqlException(
              SqlState.UNDEFINED_COLUMN, "column \"" + name + "\" named in key does not exist");
        }
        Column c = columns.get(p);
        columns.set(p, new Column(c.id(), c.name(), c.type(), true, false, c.defaultSql()));
      }
    } else {
      if (names.contains(TableDesc.ROWID)) {
        throw new SqlException(
            SqlState.DUPLICATE_COLUMN,
            "a table without a primary key cannot have a column named \"rowid\"");
      }
      columns.add(new Column(id++, TableDesc.ROWID, TypeSpec.of(Type.INT), true, true, null));
      pk = List.of(TableDesc.ROWID);
    }
    Index primary =
        new Index(
            TableDesc.PRIMARY_INDEX_ID,
            ct.name() + "_pkey",
            ids(columns, pk),
            true,
            IndexState.PUBLIC);
    List<Index> indexes = new ArrayList<>();
    int indexId = 2;
    Set<String> indexNames = new HashSet<>(Set.of(primary.name()));
    for (List<String> u : uniques) {
      List<Integer> cols = ids(columns, u);
      if (cols.equals(primary.columnIds())) {
        continue;
      }
      String name = ct.name() + "_" + String.join("_", u) + "_key";
      if (!indexNames.add(name) || snapshot.nameTaken(name)) {
        throw new SqlException(
            SqlState.DUPLICATE_TABLE, "relation \"" + name + "\" already exists");
      }
      indexes.add(new Index(indexId++, name, cols, true, IndexState.PUBLIC));
    }
    long tableId = Catalog.allocateTableId(txn);
    TableDesc t = new TableDesc(tableId, ct.name(), columns, primary, indexes, id, indexId);
    // Defaults must bind (type-check) now, not at the first INSERT.
    for (Column c : columns) {
      if (c.defaultSql() != null) {
        new Binder(snapshot, List.of()).checkDefault(Parser.parseExpression(c.defaultSql()), c);
      }
    }
    Catalog.claimName(txn, t.name(), tableId);
    for (Index i : indexes) {
      Catalog.claimName(txn, i.name(), tableId);
    }
    Catalog.claimName(txn, primary.name(), tableId);
    Catalog.put(txn, snapshot, t);
    return Result.command("CREATE TABLE");
  }

  private static int position(List<Column> columns, String name) {
    for (int p = 0; p < columns.size(); p++) {
      if (columns.get(p).name().equals(name)) {
        return p;
      }
    }
    return -1;
  }

  private static List<Integer> ids(List<Column> columns, List<String> names) {
    List<Integer> out = new ArrayList<>();
    for (String n : names) {
      int p = position(columns, n);
      if (p < 0) {
        throw new SqlException(
            SqlState.UNDEFINED_COLUMN, "column \"" + n + "\" named in key does not exist");
      }
      out.add(columns.get(p).id());
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // CREATE INDEX

  static Result createIndex(Catalog catalog, KvStore store, Ast.CreateIndex ci) {
    // Step 1: the index exists, WRITE_ONLY: writers maintain it, readers ignore it.
    record Added(TableDesc table, Index index) {}
    Committed<Optional<Added>> step1 =
        retry(
            store,
            txn -> {
              Catalog.Snapshot snapshot = catalog.load(txn);
              TableDesc t = snapshot.table(ci.table());
              String name = ci.name() != null ? ci.name() : freeName(snapshot, t, ci.columns());
              if (snapshot.nameTaken(name)) {
                if (ci.ifNotExists()) {
                  return Optional.empty();
                }
                throw new SqlException(
                    SqlState.DUPLICATE_TABLE, "relation \"" + name + "\" already exists");
              }
              List<Integer> cols = new ArrayList<>();
              for (String c : ci.columns()) {
                cols.add(t.columns().get(t.positionOrThrow(c)).id());
              }
              Index index =
                  new Index(t.nextIndexId(), name, cols, ci.unique(), IndexState.WRITE_ONLY);
              TableDesc changed = t.withIndex(index);
              Catalog.claimName(txn, name, t.id());
              Catalog.put(txn, snapshot, changed);
              return Optional.of(new Added(changed, index));
            });
    Optional<Added> added = step1.value();
    if (added.isEmpty()) {
      return Result.command(
          "CREATE INDEX", "relation \"" + ci.name() + "\" already exists, skipping");
    }
    long c1 = step1.commitTs();
    // Step 2: every transaction that might still use the old schema is over.
    store.fence(c1);
    // Step 3: backfill, check uniqueness, publish.
    Index index = added.get().index();
    long tableId = added.get().table().id();
    try {
      retry(
          store,
          txn -> {
            Catalog.Snapshot snapshot = catalog.load(txn);
            TableDesc t = byId(snapshot, tableId);
            Index current = t == null ? null : t.index(index.id());
            if (current == null) {
              throw new SqlException(
                  SqlState.UNDEFINED_OBJECT,
                  "index \"" + index.name() + "\" was dropped meanwhile");
            }
            backfill(txn, t, current);
            Catalog.put(txn, snapshot, t.withIndex(current.withState(IndexState.PUBLIC)));
            return null;
          });
    } catch (SqlException e) {
      if (e.state().equals(SqlState.UNIQUE_VIOLATION)) {
        removeIndex(catalog, store, tableId, index.id());
        throw new SqlException(
            SqlState.UNIQUE_VIOLATION,
            "could not create unique index \"" + index.name() + "\": " + e.getMessage(),
            e);
      }
      throw e;
    }
    return Result.command("CREATE INDEX");
  }

  /** Writes the index entry of every row, refusing a duplicate in a unique index. */
  private static void backfill(KvTxn txn, TableDesc t, Index index) {
    byte[] start = RowCodec.indexPrefix(t.id(), TableDesc.PRIMARY_INDEX_ID);
    byte[] end = Bytes.prefixEnd(start).orElseThrow();
    while (true) {
      List<KvTxn.Entry> page = txn.scan(start, end, 256);
      for (KvTxn.Entry e : page) {
        Object[] row = RowCodec.decodeRow(t, e.key(), e.value());
        byte[] key = RowCodec.indexKey(t, index, row);
        byte[] value = RowCodec.indexValue(t, index, row);
        if (RowCodec.uniqueEntry(t, index, row)) {
          Optional<byte[]> existing = txn.get(key);
          if (existing.isPresent() && !Arrays.equals(existing.get(), value)) {
            throw new SqlException(
                SqlState.UNIQUE_VIOLATION, "a key is duplicated in table \"" + t.name() + "\"");
          }
        }
        txn.put(key, value);
      }
      if (page.size() < 256) {
        return;
      }
      start = Bytes.successor(page.get(page.size() - 1).key());
    }
  }

  private static String freeName(Catalog.Snapshot snapshot, TableDesc t, List<String> columns) {
    String base = t.name() + "_" + String.join("_", columns) + "_idx";
    String name = base;
    for (int n = 1; snapshot.nameTaken(name); n++) {
      name = base + n;
    }
    return name;
  }

  private static TableDesc byId(Catalog.Snapshot snapshot, long id) {
    for (TableDesc t : snapshot.tables().values()) {
      if (t.id() == id) {
        return t;
      }
    }
    return null;
  }

  // -----------------------------------------------------------------------------------------------
  // DROP INDEX, DROP TABLE

  static Result dropIndex(Catalog catalog, KvStore store, Ast.DropIndex di) {
    record Target(long table, int index) {}
    Committed<Optional<Target>> step1 =
        retry(
            store,
            txn -> {
              Catalog.Snapshot snapshot = catalog.load(txn);
              Optional<TableDesc> owner = snapshot.tableOfIndex(di.name());
              if (owner.isEmpty()) {
                if (di.ifExists()) {
                  return Optional.empty();
                }
                throw new SqlException(
                    SqlState.UNDEFINED_OBJECT, "index \"" + di.name() + "\" does not exist");
              }
              TableDesc t = owner.get();
              if (t.primary().name().equals(di.name())) {
                throw new SqlException(
                    SqlState.DEPENDENT_OBJECTS_STILL_EXIST,
                    "cannot drop index "
                        + di.name()
                        + " because constraint "
                        + di.name()
                        + " on table "
                        + t.name()
                        + " requires it");
              }
              Index i =
                  t.indexes().stream()
                      .filter(x -> x.name().equals(di.name()))
                      .findFirst()
                      .orElseThrow();
              Catalog.releaseName(txn, i.name());
              Catalog.put(txn, snapshot, t.withoutIndex(i.id()));
              return Optional.of(new Target(t.id(), i.id()));
            });
    Optional<Target> target = step1.value();
    if (target.isEmpty()) {
      return Result.command("DROP INDEX", "index \"" + di.name() + "\" does not exist, skipping");
    }
    store.fence(step1.commitTs());
    deleteSpan(store, RowCodec.indexPrefix(target.get().table(), target.get().index()));
    return Result.command("DROP INDEX");
  }

  /** Removes a WRITE_ONLY index whose backfill failed. */
  private static void removeIndex(Catalog catalog, KvStore store, long tableId, int indexId) {
    Committed<Object> removed =
        retry(
            store,
            txn -> {
              Catalog.Snapshot snapshot = catalog.load(txn);
              TableDesc t = byId(snapshot, tableId);
              if (t != null && t.index(indexId) != null) {
                Catalog.releaseName(txn, t.index(indexId).name());
                Catalog.put(txn, snapshot, t.withoutIndex(indexId));
              }
              return null;
            });
    store.fence(removed.commitTs());
    deleteSpan(store, RowCodec.indexPrefix(tableId, indexId));
  }

  static Result dropTable(Catalog catalog, KvStore store, Ast.DropTable dt) {
    Committed<Optional<Long>> step1 =
        retry(
            store,
            txn -> {
              Catalog.Snapshot snapshot = catalog.load(txn);
              Optional<TableDesc> found = snapshot.find(dt.name());
              if (found.isEmpty()) {
                if (dt.ifExists()) {
                  return Optional.empty();
                }
                throw new SqlException(
                    SqlState.UNDEFINED_TABLE, "table \"" + dt.name() + "\" does not exist");
              }
              TableDesc t = found.get();
              Catalog.releaseName(txn, t.name());
              Catalog.releaseName(txn, t.primary().name());
              for (Index i : t.indexes()) {
                Catalog.releaseName(txn, i.name());
              }
              Catalog.remove(txn, snapshot, t);
              return Optional.of(t.id());
            });
    Optional<Long> dropped = step1.value();
    if (dropped.isEmpty()) {
      return Result.command("DROP TABLE", "table \"" + dt.name() + "\" does not exist, skipping");
    }
    store.fence(step1.commitTs());
    deleteSpan(store, RowCodec.tablePrefix(dropped.get()));
    return Result.command("DROP TABLE");
  }

  /**
   * Deletes every key with {@code prefix}, a thousand per transaction: nobody writes there any more
   * (the definition is gone and the fence passed), so the chunks need not be atomic.
   */
  private static void deleteSpan(KvStore store, byte[] prefix) {
    byte[] end = Bytes.prefixEnd(prefix).orElseThrow();
    while (true) {
      boolean more =
          retry(
                  store,
                  txn -> {
                    List<KvTxn.Entry> page = txn.scan(prefix, end, 1000);
                    for (KvTxn.Entry e : page) {
                      txn.delete(e.key());
                    }
                    return page.size() == 1000;
                  })
              .value();
      if (!more) {
        return;
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** A step's result and the timestamp it committed at. */
  private record Committed<T>(T value, long commitTs) {}

  /** Runs {@code work} in a transaction and commits it, again on a serialization failure. */
  private static <T> Committed<T> retry(KvStore store, Function<KvTxn, T> work) {
    for (int attempt = 1; ; attempt++) {
      KvTxn txn = store.begin();
      try {
        T out = work.apply(txn);
        return new Committed<>(out, txn.commit());
      } catch (SqlException e) {
        Session.rollbackQuietly(txn);
        if (!e.retryable() || attempt >= ATTEMPTS) {
          throw e;
        }
      } catch (RuntimeException e) {
        Session.rollbackQuietly(txn);
        throw e;
      }
    }
  }
}

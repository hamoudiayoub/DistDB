package io.distdb.sql.exec;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.catalog.TableDesc.Index;
import io.distdb.sql.encoding.RowCodec;
import io.distdb.sql.kv.KvTxn;
import io.distdb.sql.plan.Bound;
import io.distdb.sql.plan.Planner;
import io.distdb.sql.plan.Scalar;
import io.distdb.sql.types.Values;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * INSERT, UPDATE and DELETE. Each first collects the rows it will change (so it never sees its own
 * writes: no Halloween problem), then writes every row with all of its index entries in the
 * statement's transaction. Uniqueness is checked by reading the key in the same transaction; two
 * concurrent inserts of one key both write it, so snapshot isolation's first-committer-wins rule
 * lets only one commit.
 */
public final class Writes {

  private Writes() {}

  /** The rows changed, and the RETURNING rows (empty without RETURNING). */
  public record Outcome(long count, List<Object[]> returning) {}

  public static Outcome insert(Bound.Insert b, ExecContext ctx) {
    TableDesc t = b.table();
    List<Object[]> source = Operators.drain(Operators.build(Planner.plan(b.source()), ctx));
    long[] rowids = t.hasRowid() ? ctx.store.timestamps(source.size()) : null;
    int rowidPosition = t.hasRowid() ? t.positions(t.primary())[0] : -1;
    boolean[] targeted = new boolean[t.columns().size()];
    for (int p : b.targets()) {
      targeted[p] = true;
    }
    List<Object[]> returning = new ArrayList<>();
    long count = 0;
    Object[] none = new Object[0];
    for (int i = 0; i < source.size(); i++) {
      Object[] src = source.get(i);
      Object[] row = new Object[t.columns().size()];
      for (int k = 0; k < b.targets().length; k++) {
        int p = b.targets()[k];
        row[p] =
            src[k] == null
                ? null
                : Values.assign(src[k], b.sourceTypes()[k], t.columns().get(p).type());
      }
      for (int p = 0; p < row.length; p++) {
        if (targeted[p]) {
          continue;
        }
        if (p == rowidPosition) {
          row[p] = rowids[i];
        } else if (b.defaults().get(p) != null) {
          Scalar d = b.defaults().get(p);
          Object v = Eval.eval(d, none, ctx);
          row[p] = v == null ? null : Values.assign(v, d.type(), t.columns().get(p).type());
        }
      }
      checkNotNull(t, row);
      if (insertRow(ctx.txn, t, row, b.onConflictDoNothing())) {
        count++;
        if (b.returning().present()) {
          returning.add(Eval.all(b.returning().exprs(), row, ctx));
        }
      }
    }
    return new Outcome(count, returning);
  }

  /**
   * Writes a new row and its index entries; false (writing nothing) when {@code skipConflicts} and
   * a unique key is taken.
   */
  static boolean insertRow(KvTxn txn, TableDesc t, Object[] row, boolean skipConflicts) {
    byte[] pk = RowCodec.primaryKey(t, row);
    if (txn.get(pk).isPresent()) {
      if (skipConflicts) {
        return false;
      }
      throw duplicate(t.primary(), t, row);
    }
    for (Index i : t.indexes()) {
      if (RowCodec.uniqueEntry(t, i, row) && txn.get(RowCodec.indexKey(t, i, row)).isPresent()) {
        if (skipConflicts) {
          return false;
        }
        throw duplicate(i, t, row);
      }
    }
    txn.put(pk, RowCodec.rowValue(t, row));
    for (Index i : t.indexes()) {
      txn.put(RowCodec.indexKey(t, i, row), RowCodec.indexValue(t, i, row));
    }
    return true;
  }

  public static Outcome update(Bound.Update b, ExecContext ctx) {
    TableDesc t = b.table();
    List<Object[]> rows = Operators.drain(Operators.build(Planner.plan(b.source()), ctx));
    List<Object[]> returning = new ArrayList<>();
    for (Object[] old : rows) {
      Object[] row = old.clone();
      for (int k = 0; k < b.targets().length; k++) {
        int p = b.targets()[k];
        Scalar e = b.values().get(k);
        Object v = Eval.eval(e, old, ctx);
        row[p] = v == null ? null : Values.assign(v, e.type(), t.columns().get(p).type());
      }
      checkNotNull(t, row);
      byte[] oldPk = RowCodec.primaryKey(t, old);
      byte[] newPk = RowCodec.primaryKey(t, row);
      if (!Arrays.equals(oldPk, newPk)) {
        if (ctx.txn.get(newPk).isPresent()) {
          throw duplicate(t.primary(), t, row);
        }
        ctx.txn.delete(oldPk);
      }
      // Written even when unchanged: a concurrent update of the same row must conflict.
      ctx.txn.put(newPk, RowCodec.rowValue(t, row));
      for (Index i : t.indexes()) {
        byte[] oldKey = RowCodec.indexKey(t, i, old);
        byte[] newKey = RowCodec.indexKey(t, i, row);
        byte[] newValue = RowCodec.indexValue(t, i, row);
        if (!Arrays.equals(oldKey, newKey)) {
          if (RowCodec.uniqueEntry(t, i, row) && ctx.txn.get(newKey).isPresent()) {
            throw duplicate(i, t, row);
          }
          ctx.txn.delete(oldKey);
          ctx.txn.put(newKey, newValue);
        } else if (!Arrays.equals(RowCodec.indexValue(t, i, old), newValue)) {
          ctx.txn.put(newKey, newValue);
        }
      }
      if (b.returning().present()) {
        returning.add(Eval.all(b.returning().exprs(), row, ctx));
      }
    }
    return new Outcome(rows.size(), returning);
  }

  public static Outcome delete(Bound.Delete b, ExecContext ctx) {
    TableDesc t = b.table();
    List<Object[]> rows = Operators.drain(Operators.build(Planner.plan(b.source()), ctx));
    List<Object[]> returning = new ArrayList<>();
    for (Object[] row : rows) {
      deleteRow(ctx.txn, t, row);
      if (b.returning().present()) {
        returning.add(Eval.all(b.returning().exprs(), row, ctx));
      }
    }
    return new Outcome(rows.size(), returning);
  }

  static void deleteRow(KvTxn txn, TableDesc t, Object[] row) {
    txn.delete(RowCodec.primaryKey(t, row));
    for (Index i : t.indexes()) {
      txn.delete(RowCodec.indexKey(t, i, row));
    }
  }

  private static void checkNotNull(TableDesc t, Object[] row) {
    for (int p = 0; p < row.length; p++) {
      TableDesc.Column c = t.columns().get(p);
      if (row[p] == null && c.notNull()) {
        throw new SqlException(
            SqlState.NOT_NULL_VIOLATION,
            "null value in column \""
                + c.name()
                + "\" of relation \""
                + t.name()
                + "\" violates not-null constraint");
      }
    }
  }

  private static SqlException duplicate(Index i, TableDesc t, Object[] row) {
    StringBuilder cols = new StringBuilder();
    StringBuilder vals = new StringBuilder();
    for (int p : t.positions(i)) {
      if (!cols.isEmpty()) {
        cols.append(", ");
        vals.append(", ");
      }
      TableDesc.Column c = t.columns().get(p);
      cols.append(c.name());
      vals.append(row[p] == null ? "null" : Values.toText(row[p], c.type().type()));
    }
    return new SqlException(
        SqlState.UNIQUE_VIOLATION,
        "duplicate key value violates unique constraint \""
            + i.name()
            + "\" (Key ("
            + cols
            + ")=("
            + vals
            + ") already exists.)");
  }
}

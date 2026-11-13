package io.distdb.sql.plan;

import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.catalog.TableDesc.Index;
import io.distdb.sql.parse.Ast.JoinKind;
import io.distdb.sql.parse.Ast.SetOpKind;
import io.distdb.sql.types.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * A query plan: a tree of relational operators, each producing rows of {@link #columns()}. The
 * binder builds a logical plan ({@link Scan}, {@link Join}); the {@link Planner} turns it into a
 * physical one, choosing access paths ({@link FullScan}, {@link PointGet}, {@link IndexScan}) and
 * join algorithms ({@link HashJoin}, {@link LookupJoin}, {@link NestedLoopJoin}). The other nodes
 * are both.
 */
public sealed interface Plan {

  List<Col> columns();

  /** An output column: the table (alias) it came from, if any, its name and type. */
  record Col(String table, String name, Type type) {}

  // -----------------------------------------------------------------------------------------------
  // Logical

  /** Every row of a table; the planner picks how. */
  record Scan(TableDesc table, String alias, List<Col> columns) implements Plan {}

  /** A join; CROSS joins become INNER joins on TRUE. */
  record Join(JoinKind kind, Plan left, Plan right, Scalar on, List<Col> columns) implements Plan {}

  // -----------------------------------------------------------------------------------------------
  // Physical access paths

  record FullScan(TableDesc table, String alias, List<Col> columns) implements Plan {}

  /** The row whose whole primary key equals {@code key} (evaluated when the scan opens). */
  record PointGet(TableDesc table, String alias, List<Scalar> key, List<Col> columns)
      implements Plan {}

  /** One end of a range: a value, and whether it is included. */
  record Bound(Scalar value, boolean inclusive) {}

  /**
   * The rows whose first index columns equal {@code prefix} and whose next column is within {@code
   * low} and {@code high} (either may be null); {@code index} may be the primary one.
   */
  record IndexScan(
      TableDesc table,
      Index index,
      String alias,
      List<Scalar> prefix,
      Bound low,
      Bound high,
      List<Col> columns)
      implements Plan {}

  // -----------------------------------------------------------------------------------------------
  // Physical joins

  record NestedLoopJoin(JoinKind kind, Plan left, Plan right, Scalar on, List<Col> columns)
      implements Plan {}

  /** Builds a hash table of the right side on {@code rightKeys}, probes it with the left. */
  record HashJoin(
      JoinKind kind,
      Plan left,
      Plan right,
      List<Scalar> leftKeys,
      List<Scalar> rightKeys,
      Scalar residual,
      List<Col> columns)
      implements Plan {}

  /**
   * For each left row, looks up the matching right row by a unique key ({@code index}: the primary
   * key or a unique index), from {@code keys} evaluated on the left row.
   */
  record LookupJoin(
      JoinKind kind,
      Plan left,
      TableDesc table,
      Index index,
      String alias,
      List<Scalar> keys,
      Scalar residual,
      List<Col> columns)
      implements Plan {}

  // -----------------------------------------------------------------------------------------------
  // Both

  record Filter(Plan input, Scalar predicate) implements Plan {
    @Override
    public List<Col> columns() {
      return input.columns();
    }
  }

  record Project(Plan input, List<Scalar> exprs, List<Col> columns) implements Plan {}

  /** Output rows: the group values, then the aggregates. */
  record Aggregate(Plan input, List<Scalar> groups, List<Scalar.AggCall> aggs, List<Col> columns)
      implements Plan {}

  record SortKey(Scalar expr, boolean desc, boolean nullsFirst) {}

  record Sort(Plan input, List<SortKey> keys) implements Plan {
    @Override
    public List<Col> columns() {
      return input.columns();
    }
  }

  /** {@code limit} null: no limit. */
  record Limit(Plan input, Scalar limit, Scalar offset) implements Plan {
    @Override
    public List<Col> columns() {
      return input.columns();
    }
  }

  record Distinct(Plan input) implements Plan {
    @Override
    public List<Col> columns() {
      return input.columns();
    }
  }

  record Values(List<List<Scalar>> rows, List<Col> columns) implements Plan {}

  record SetOp(SetOpKind kind, boolean all, Plan left, Plan right, List<Col> columns)
      implements Plan {}

  /** The single empty row a SELECT without FROM reads. */
  record OneRow() implements Plan {
    @Override
    public List<Col> columns() {
      return List.of();
    }
  }

  /** The columns of {@code left} followed by those of {@code right}. */
  static List<Col> concat(List<Col> left, List<Col> right) {
    List<Col> out = new ArrayList<>(left);
    out.addAll(right);
    return List.copyOf(out);
  }
}

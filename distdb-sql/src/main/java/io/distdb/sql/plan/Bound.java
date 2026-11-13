package io.distdb.sql.plan;

import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.plan.Plan.Col;
import io.distdb.sql.types.Type;
import java.util.List;

/** A bound statement: a query plan, or a write with the plan that finds its rows. */
public sealed interface Bound {

  /** Expressions over the table's row, for {@code RETURNING}; empty when there is none. */
  record Returning(List<Scalar> exprs, List<Col> columns) {
    public static final Returning NONE = new Returning(List.of(), List.of());

    public boolean present() {
      return !exprs.isEmpty();
    }
  }

  record Query(Plan plan) implements Bound {}

  /**
   * {@code targets[i]}: the table column (position) that the source's column {@code i} fills, after
   * an assignment cast from {@code sourceTypes[i]}. {@code defaults}: per table column, its default
   * (bound over no row), or null for NULL.
   */
  record Insert(
      TableDesc table,
      Plan source,
      int[] targets,
      Type[] sourceTypes,
      List<Scalar> defaults,
      boolean onConflictDoNothing,
      Returning returning)
      implements Bound {}

  /**
   * {@code source} produces the table's rows to change; {@code values[i]}, evaluated on such a row,
   * is the new value of column {@code targets[i]}.
   */
  record Update(
      TableDesc table, Plan source, int[] targets, List<Scalar> values, Returning returning)
      implements Bound {}

  record Delete(TableDesc table, Plan source, Returning returning) implements Bound {}
}

package io.distdb.sql.plan;

import io.distdb.sql.plan.Plan.Aggregate;
import io.distdb.sql.plan.Plan.Distinct;
import io.distdb.sql.plan.Plan.Filter;
import io.distdb.sql.plan.Plan.FullScan;
import io.distdb.sql.plan.Plan.HashJoin;
import io.distdb.sql.plan.Plan.IndexScan;
import io.distdb.sql.plan.Plan.Join;
import io.distdb.sql.plan.Plan.Limit;
import io.distdb.sql.plan.Plan.LookupJoin;
import io.distdb.sql.plan.Plan.NestedLoopJoin;
import io.distdb.sql.plan.Plan.OneRow;
import io.distdb.sql.plan.Plan.PointGet;
import io.distdb.sql.plan.Plan.Project;
import io.distdb.sql.plan.Plan.Scan;
import io.distdb.sql.plan.Plan.SetOp;
import io.distdb.sql.plan.Plan.Sort;
import io.distdb.sql.plan.Plan.Values;
import io.distdb.sql.plan.Scalar.OuterRef;
import io.distdb.sql.plan.Scalar.Subquery;
import java.util.ArrayList;
import java.util.List;

/** Walking plan trees. */
public final class Plans {

  private Plans() {}

  /** The plans directly under {@code p}. */
  public static List<Plan> children(Plan p) {
    return switch (p) {
      case Scan s -> List.of();
      case FullScan s -> List.of();
      case PointGet g -> List.of();
      case IndexScan s -> List.of();
      case OneRow o -> List.of();
      case Values v -> List.of();
      case Join j -> List.of(j.left(), j.right());
      case NestedLoopJoin j -> List.of(j.left(), j.right());
      case HashJoin j -> List.of(j.left(), j.right());
      case LookupJoin j -> List.of(j.left());
      case Filter f -> List.of(f.input());
      case Project pr -> List.of(pr.input());
      case Aggregate a -> List.of(a.input());
      case Sort s -> List.of(s.input());
      case Limit l -> List.of(l.input());
      case Distinct d -> List.of(d.input());
      case SetOp s -> List.of(s.left(), s.right());
    };
  }

  /** The expressions held by {@code p} itself (not its children). */
  public static List<Scalar> scalars(Plan p) {
    List<Scalar> out = new ArrayList<>();
    switch (p) {
      case PointGet g -> out.addAll(g.key());
      case IndexScan s -> {
        out.addAll(s.prefix());
        if (s.low() != null) {
          out.add(s.low().value());
        }
        if (s.high() != null) {
          out.add(s.high().value());
        }
      }
      case Values v -> v.rows().forEach(out::addAll);
      case Join j -> out.add(j.on());
      case NestedLoopJoin j -> out.add(j.on());
      case HashJoin j -> {
        out.addAll(j.leftKeys());
        out.addAll(j.rightKeys());
        if (j.residual() != null) {
          out.add(j.residual());
        }
      }
      case LookupJoin j -> {
        out.addAll(j.keys());
        if (j.residual() != null) {
          out.add(j.residual());
        }
      }
      case Filter f -> out.add(f.predicate());
      case Project pr -> out.addAll(pr.exprs());
      case Aggregate a -> {
        out.addAll(a.groups());
        out.addAll(a.aggs());
      }
      case Sort s -> s.keys().forEach(k -> out.add(k.expr()));
      case Limit l -> {
        if (l.limit() != null) {
          out.add(l.limit());
        }
        if (l.offset() != null) {
          out.add(l.offset());
        }
      }
      default -> {
        // no expressions
      }
    }
    return out;
  }

  /**
   * Whether {@code p} refers to a row outside itself: an outer reference deeper than the subqueries
   * it sits in. Such a subquery must run again for each outer row.
   */
  public static boolean correlated(Plan p) {
    return reachesOut(p, 0);
  }

  private static boolean reachesOut(Plan p, int nesting) {
    for (Scalar s : scalars(p)) {
      if (reachesOut(s, nesting)) {
        return true;
      }
    }
    for (Plan c : children(p)) {
      if (reachesOut(c, nesting)) {
        return true;
      }
    }
    return false;
  }

  private static boolean reachesOut(Scalar s, int nesting) {
    boolean[] found = {false};
    Scalars.visit(
        s,
        x -> {
          if (x instanceof OuterRef o && o.depth() > nesting) {
            found[0] = true;
          }
          if (x instanceof Subquery q && reachesOut(q.plan(), nesting + 1)) {
            found[0] = true;
          }
        });
    return found[0];
  }
}

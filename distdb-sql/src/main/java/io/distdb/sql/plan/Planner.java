package io.distdb.sql.plan;

import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.catalog.TableDesc.Index;
import io.distdb.sql.catalog.TableDesc.IndexState;
import io.distdb.sql.parse.Ast.JoinKind;
import io.distdb.sql.plan.Plan.Bound;
import io.distdb.sql.plan.Scalar.CmpOp;
import io.distdb.sql.plan.Scalar.ColRef;
import io.distdb.sql.plan.Scalar.Compare;
import io.distdb.sql.plan.Scalar.Const;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a logical plan into a physical one, with three rules:
 *
 * <ol>
 *   <li><b>Predicate pushdown.</b> Each conjunct of a WHERE (or of an inner join's ON) moves below
 *       a join to the side whose columns it reads, as long as that keeps outer joins' meaning.
 *   <li><b>Access paths.</b> A table scan under conjuncts {@code col = v} and {@code col < v} (with
 *       {@code v} not reading the row) becomes a point get when they fix the whole primary key, a
 *       narrower scan of the primary key or of an index when they fix a prefix or bound a column,
 *       and a full scan otherwise. The whole predicate stays as a filter on top.
 *   <li><b>Join algorithms.</b> An equi-join becomes a hash join; when its right side is a table
 *       whose unique key the join fixes and its left side looks small, a lookup join; anything
 *       else, a nested loop.
 * </ol>
 *
 * There are no statistics yet (Phase 9): "looks small" means a point or unique lookup, a short
 * VALUES list, or a small LIMIT.
 */
public final class Planner {

  private Planner() {}

  public static Plan plan(Plan p) {
    return switch (p) {
      case Plan.Scan s -> new Plan.FullScan(s.table(), s.alias(), s.columns());
      case Plan.Filter f -> filter(f);
      case Plan.Join j -> join(j, List.of());
      case Plan.Project pr -> new Plan.Project(plan(pr.input()), scalars(pr.exprs()), pr.columns());
      case Plan.Aggregate a -> {
        List<Scalar.AggCall> aggs = new ArrayList<>();
        for (Scalar.AggCall c : a.aggs()) {
          aggs.add((Scalar.AggCall) scalar(c));
        }
        yield new Plan.Aggregate(plan(a.input()), scalars(a.groups()), aggs, a.columns());
      }
      case Plan.Sort s -> {
        List<Plan.SortKey> keys = new ArrayList<>();
        for (Plan.SortKey k : s.keys()) {
          keys.add(new Plan.SortKey(scalar(k.expr()), k.desc(), k.nullsFirst()));
        }
        yield new Plan.Sort(plan(s.input()), keys);
      }
      case Plan.Limit l ->
          new Plan.Limit(plan(l.input()), scalarOrNull(l.limit()), scalarOrNull(l.offset()));
      case Plan.Distinct d -> new Plan.Distinct(plan(d.input()));
      case Plan.SetOp s ->
          new Plan.SetOp(s.kind(), s.all(), plan(s.left()), plan(s.right()), s.columns());
      case Plan.Values v -> {
        List<List<Scalar>> rows = new ArrayList<>();
        for (List<Scalar> r : v.rows()) {
          rows.add(scalars(r));
        }
        yield new Plan.Values(rows, v.columns());
      }
      case Plan.OneRow o -> o;
      // Already physical.
      case Plan.FullScan s -> s;
      case Plan.PointGet g -> g;
      case Plan.IndexScan s -> s;
      case Plan.NestedLoopJoin j -> j;
      case Plan.HashJoin j -> j;
      case Plan.LookupJoin j -> j;
    };
  }

  /** Plans the subqueries inside an expression. */
  static Scalar scalar(Scalar s) {
    return Scalars.map(
        s,
        x -> {
          if (x instanceof Scalar.Subquery q) {
            return new Scalar.Subquery(
                q.kind(),
                plan(q.plan()),
                q.operand() == null ? null : scalar(q.operand()),
                q.not(),
                q.correlated(),
                q.type());
          }
          if (x instanceof Scalar.OuterRef) {
            return x;
          }
          return null;
        });
  }

  private static Scalar scalarOrNull(Scalar s) {
    return s == null ? null : scalar(s);
  }

  private static List<Scalar> scalars(List<Scalar> list) {
    List<Scalar> out = new ArrayList<>(list.size());
    for (Scalar s : list) {
      out.add(scalar(s));
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Filters

  private static Plan filter(Plan.Filter f) {
    List<Scalar> conjuncts = Scalars.conjuncts(scalar(f.predicate()));
    return switch (f.input()) {
      case Plan.Scan s -> accessPath(s, conjuncts);
      case Plan.Join j -> join(j, conjuncts);
      default -> wrap(plan(f.input()), conjuncts);
    };
  }

  private static Plan wrap(Plan input, List<Scalar> conjuncts) {
    Scalar p = Scalars.and(conjuncts);
    return p == null ? input : new Plan.Filter(input, p);
  }

  // -----------------------------------------------------------------------------------------------
  // Access paths

  /** A conjunct {@code column op value}, with {@code value} independent of the row. */
  private record Sarg(int column, CmpOp op, Scalar value) {}

  private static Sarg sarg(Scalar c) {
    if (!(c instanceof Compare cmp) || cmp.op() == CmpOp.NE || Scalars.hasSubquery(c)) {
      return null;
    }
    if (cmp.left() instanceof ColRef l && !Scalars.readsRow(cmp.right())) {
      return new Sarg(l.index(), cmp.op(), cmp.right());
    }
    if (cmp.right() instanceof ColRef r && !Scalars.readsRow(cmp.left())) {
      return new Sarg(r.index(), cmp.op().flip(), cmp.left());
    }
    return null;
  }

  private static Plan accessPath(Plan.Scan s, List<Scalar> conjuncts) {
    TableDesc t = s.table();
    Map<Integer, Scalar> eq = new HashMap<>();
    Map<Integer, Bound> low = new HashMap<>();
    Map<Integer, Bound> high = new HashMap<>();
    for (Scalar c : conjuncts) {
      Sarg a = sarg(c);
      if (a == null) {
        continue;
      }
      switch (a.op()) {
        case EQ -> eq.putIfAbsent(a.column(), a.value());
        case GT -> low.putIfAbsent(a.column(), new Bound(a.value(), false));
        case GE -> low.putIfAbsent(a.column(), new Bound(a.value(), true));
        case LT -> high.putIfAbsent(a.column(), new Bound(a.value(), false));
        case LE -> high.putIfAbsent(a.column(), new Bound(a.value(), true));
        default -> {
          // not sargable
        }
      }
    }
    // The whole primary key fixed: one get.
    int[] pk = t.positions(t.primary());
    boolean point = pk.length > 0;
    for (int p : pk) {
      point &= eq.containsKey(p);
    }
    if (point) {
      List<Scalar> key = new ArrayList<>();
      for (int p : pk) {
        key.add(eq.get(p));
      }
      return wrap(new Plan.PointGet(t, s.alias(), key, s.columns()), conjuncts);
    }
    // Else the index (primary first) with the longest fixed prefix, then a bounded column.
    Index best = null;
    int bestScore = 0;
    List<Index> candidates = new ArrayList<>();
    candidates.add(t.primary());
    for (Index i : t.indexes()) {
      if (i.state() == IndexState.PUBLIC) {
        candidates.add(i);
      }
    }
    for (Index i : candidates) {
      int[] cols = t.positions(i);
      int k = 0;
      while (k < cols.length && eq.containsKey(cols[k])) {
        k++;
      }
      boolean ranged = k < cols.length && (low.containsKey(cols[k]) || high.containsKey(cols[k]));
      int score = k * 2 + (ranged ? 1 : 0);
      if (score > bestScore) {
        best = i;
        bestScore = score;
      }
    }
    if (best == null) {
      return wrap(new Plan.FullScan(t, s.alias(), s.columns()), conjuncts);
    }
    int[] cols = t.positions(best);
    List<Scalar> prefix = new ArrayList<>();
    int k = 0;
    while (k < cols.length && eq.containsKey(cols[k])) {
      prefix.add(eq.get(cols[k]));
      k++;
    }
    Bound lo = k < cols.length ? low.get(cols[k]) : null;
    Bound hi = k < cols.length ? high.get(cols[k]) : null;
    return wrap(new Plan.IndexScan(t, best, s.alias(), prefix, lo, hi, s.columns()), conjuncts);
  }

  // -----------------------------------------------------------------------------------------------
  // Joins

  private static Plan join(Plan.Join j, List<Scalar> where) {
    int width = j.left().columns().size();
    JoinKind kind = j.kind();
    List<Scalar> pushLeft = new ArrayList<>();
    List<Scalar> pushRight = new ArrayList<>();
    List<Scalar> keep = new ArrayList<>();
    List<Scalar> on = new ArrayList<>();
    // WHERE conjuncts: below the join when only one side is read, and an outer join's
    // preserved side is the one read.
    for (Scalar c : where) {
      Side side = side(c, width);
      if (side == Side.LEFT && (kind == JoinKind.INNER || kind == JoinKind.LEFT)) {
        pushLeft.add(c);
      } else if (side == Side.RIGHT && (kind == JoinKind.INNER || kind == JoinKind.RIGHT)) {
        pushRight.add(Binder.shift(c, -width));
      } else if (side == Side.BOTH && kind == JoinKind.INNER) {
        on.add(c);
      } else {
        keep.add(c);
      }
    }
    // ON conjuncts: below the join on the side that is not preserved.
    for (Scalar c : Scalars.conjuncts(scalar(j.on()))) {
      Side side = side(c, width);
      if (side == Side.LEFT && (kind == JoinKind.INNER || kind == JoinKind.RIGHT)) {
        pushLeft.add(c);
      } else if (side == Side.RIGHT && (kind == JoinKind.INNER || kind == JoinKind.LEFT)) {
        pushRight.add(Binder.shift(c, -width));
      } else {
        on.add(c);
      }
    }
    Plan left = j.left();
    Plan right = j.right();
    if (!pushLeft.isEmpty()) {
      left = new Plan.Filter(left, Scalars.and(pushLeft));
    }
    if (!pushRight.isEmpty()) {
      right = new Plan.Filter(right, Scalars.and(pushRight));
    }
    // Equality conjuncts between the two sides become hash (or lookup) keys.
    List<Scalar> leftKeys = new ArrayList<>();
    List<Scalar> rightKeys = new ArrayList<>();
    List<Scalar> residual = new ArrayList<>();
    for (Scalar c : on) {
      if (c instanceof Compare cmp && cmp.op() == CmpOp.EQ && !Scalars.hasSubquery(c)) {
        Side l = side(cmp.left(), width);
        Side r = side(cmp.right(), width);
        if (l == Side.LEFT && r == Side.RIGHT) {
          leftKeys.add(cmp.left());
          rightKeys.add(Binder.shift(cmp.right(), -width));
          continue;
        }
        if (l == Side.RIGHT && r == Side.LEFT) {
          leftKeys.add(cmp.right());
          rightKeys.add(Binder.shift(cmp.left(), -width));
          continue;
        }
      }
      residual.add(c);
    }
    Plan joined = physicalJoin(j, kind, left, right, leftKeys, rightKeys, residual);
    return wrap(joined, keep);
  }

  private static Plan physicalJoin(
      Plan.Join j,
      JoinKind kind,
      Plan left,
      Plan right,
      List<Scalar> leftKeys,
      List<Scalar> rightKeys,
      List<Scalar> residual) {
    Plan pl = plan(left);
    if (!leftKeys.isEmpty() && (kind == JoinKind.INNER || kind == JoinKind.LEFT) && small(pl)) {
      Plan lookup = lookupJoin(j, kind, pl, right, leftKeys, rightKeys, residual);
      if (lookup != null) {
        return lookup;
      }
    }
    Plan pr = plan(right);
    if (!leftKeys.isEmpty()) {
      return new Plan.HashJoin(
          kind, pl, pr, leftKeys, rightKeys, Scalars.and(residual), j.columns());
    }
    Scalar on = Scalars.and(residual);
    return new Plan.NestedLoopJoin(
        kind,
        pl,
        pr,
        on == null ? new Const(true, io.distdb.sql.types.Type.BOOL) : on,
        j.columns());
  }

  /**
   * A lookup join when the right side is a table (perhaps filtered) and the keys fix its primary
   * key or a public unique index; null otherwise.
   */
  private static Plan lookupJoin(
      Plan.Join j,
      JoinKind kind,
      Plan left,
      Plan right,
      List<Scalar> leftKeys,
      List<Scalar> rightKeys,
      List<Scalar> residual) {
    Plan.Scan scan;
    Scalar rightFilter = null;
    if (right instanceof Plan.Scan s) {
      scan = s;
    } else if (right instanceof Plan.Filter f && f.input() instanceof Plan.Scan s) {
      scan = s;
      rightFilter = f.predicate();
    } else {
      return null;
    }
    TableDesc t = scan.table();
    Map<Integer, Scalar> fixed = new HashMap<>();
    for (int k = 0; k < rightKeys.size(); k++) {
      if (rightKeys.get(k) instanceof ColRef c) {
        fixed.putIfAbsent(c.index(), leftKeys.get(k));
      }
    }
    List<Index> candidates = new ArrayList<>();
    candidates.add(t.primary());
    for (Index i : t.indexes()) {
      if (i.unique() && i.state() == IndexState.PUBLIC) {
        candidates.add(i);
      }
    }
    for (Index i : candidates) {
      List<Scalar> keys = new ArrayList<>();
      for (int p : t.positions(i)) {
        Scalar v = fixed.get(p);
        if (v == null) {
          keys = null;
          break;
        }
        keys.add(v);
      }
      if (keys == null) {
        continue;
      }
      // Every equality not used as the key, and the right side's own filter, are checked on the
      // joined row.
      int width = left.columns().size();
      List<Scalar> check = new ArrayList<>(residual);
      for (int k = 0; k < rightKeys.size(); k++) {
        check.add(new Compare(CmpOp.EQ, leftKeys.get(k), Binder.shift(rightKeys.get(k), width)));
      }
      if (rightFilter != null) {
        check.add(Binder.shift(rightFilter, width));
      }
      return new Plan.LookupJoin(
          kind, left, t, i, scan.alias(), keys, Scalars.and(check), j.columns());
    }
    return null;
  }

  /** Whether a plan looks like it produces few rows (without statistics). */
  private static boolean small(Plan p) {
    return switch (p) {
      case Plan.PointGet g -> true;
      case Plan.OneRow o -> true;
      case Plan.Values v -> v.rows().size() <= 100;
      case Plan.IndexScan s ->
          s.index().unique() && s.prefix().size() == s.index().columnIds().size();
      case Plan.Filter f -> small(f.input());
      case Plan.Project pr -> small(pr.input());
      case Plan.LookupJoin l -> small(l.left());
      case Plan.Limit l -> l.limit() instanceof Const c && c.value() instanceof Long n && n <= 100;
      default -> false;
    };
  }

  private enum Side {
    NONE,
    LEFT,
    RIGHT,
    BOTH
  }

  /** Which side of a join (left: positions below {@code width}) an expression reads. */
  private static Side side(Scalar s, int width) {
    if (Scalars.hasSubquery(s)) {
      return Side.BOTH; // never moved: its outer references point at the join's row
    }
    boolean l = false;
    boolean r = false;
    for (int c : Scalars.columnsRead(s)) {
      if (c < width) {
        l = true;
      } else {
        r = true;
      }
    }
    return l && r ? Side.BOTH : (l ? Side.LEFT : (r ? Side.RIGHT : Side.NONE));
  }
}

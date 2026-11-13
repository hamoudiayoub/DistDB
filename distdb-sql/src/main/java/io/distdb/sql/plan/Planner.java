package io.distdb.sql.plan;

import io.distdb.sql.catalog.Catalog;
import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.catalog.TableDesc.Index;
import io.distdb.sql.catalog.TableDesc.IndexState;
import io.distdb.sql.parse.Ast.JoinKind;
import io.distdb.sql.plan.Estimator.Est;
import io.distdb.sql.plan.Plan.Bound;
import io.distdb.sql.plan.Plan.Col;
import io.distdb.sql.plan.Scalar.CmpOp;
import io.distdb.sql.plan.Scalar.ColRef;
import io.distdb.sql.plan.Scalar.Compare;
import io.distdb.sql.plan.Scalar.Const;
import io.distdb.sql.types.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a logical plan into a physical one, by cost (Phase 9) where Phase 8 used rules of thumb:
 *
 * <ol>
 *   <li><b>Predicate pushdown.</b> Each conjunct of a WHERE (or of a join's ON) moves below a join
 *       to the side whose columns it reads, as long as that keeps outer joins' meaning.
 *   <li><b>Access paths.</b> For a table scan under conjuncts {@code col = v} and {@code col < v}
 *       ({@code v} not reading the row), each candidate (a point get when they fix the primary key,
 *       a range of the primary key or of an index, a full scan) is costed with the table's
 *       statistics, and the cheapest wins. The whole predicate stays as a filter on top.
 *   <li><b>Join order.</b> The inner joins of a query are ordered by dynamic programming over sets
 *       of tables (System R): the cheapest plan for each set, from two cheaper ones a predicate
 *       connects. A projection on top restores the columns' order.
 *   <li><b>Join algorithms.</b> Hash join, lookup join (when the right side is a table whose unique
 *       key the join fixes) and nested loop, whichever costs least.
 * </ol>
 *
 * {@link Pushdown} then moves work to the ranges.
 */
public final class Planner {

  /** Inner joins of more tables than this keep their written order. */
  static final int MAX_DP_TABLES = 8;

  private final Estimator estimator;

  private Planner(Catalog.Snapshot catalog, boolean pushdown) {
    this.estimator = new Estimator(catalog, pushdown);
  }

  /** Plans without statistics or pushdown. */
  public static Plan plan(Plan p) {
    return new Planner(null, false).node(p);
  }

  public static Plan plan(Plan p, boolean pushdown) {
    return plan(p, pushdown, null);
  }

  /**
   * Plans {@code p} with the statistics in {@code catalog} (null: none) and, when {@code pushdown},
   * moves filters, projections, partial aggregates and limits to the ranges.
   */
  public static Plan plan(Plan p, boolean pushdown, Catalog.Snapshot catalog) {
    Plan physical = new Planner(catalog, pushdown).node(p);
    return pushdown ? Pushdown.apply(physical) : physical;
  }

  private Plan node(Plan p) {
    return switch (p) {
      case Plan.Scan s -> new Plan.FullScan(s.table(), s.alias(), s.columns());
      case Plan.Filter f -> filter(f);
      case Plan.Join j -> join(j, List.of());
      case Plan.Project pr -> new Plan.Project(node(pr.input()), scalars(pr.exprs()), pr.columns());
      case Plan.Aggregate a -> {
        List<Scalar.AggCall> aggs = new ArrayList<>();
        for (Scalar.AggCall c : a.aggs()) {
          aggs.add((Scalar.AggCall) scalar(c));
        }
        yield new Plan.Aggregate(node(a.input()), scalars(a.groups()), aggs, a.columns());
      }
      case Plan.Sort s -> {
        List<Plan.SortKey> keys = new ArrayList<>();
        for (Plan.SortKey k : s.keys()) {
          keys.add(new Plan.SortKey(scalar(k.expr()), k.desc(), k.nullsFirst()));
        }
        yield new Plan.Sort(node(s.input()), keys);
      }
      case Plan.Limit l ->
          new Plan.Limit(node(l.input()), scalarOrNull(l.limit()), scalarOrNull(l.offset()));
      case Plan.Distinct d -> new Plan.Distinct(node(d.input()));
      case Plan.SetOp s ->
          new Plan.SetOp(s.kind(), s.all(), node(s.left()), node(s.right()), s.columns());
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
      case Plan.RemoteScan s -> s;
      case Plan.MergeAggregate m -> m;
    };
  }

  /** Plans the subqueries inside an expression. */
  private Scalar scalar(Scalar s) {
    return Scalars.map(
        s,
        x -> {
          if (x instanceof Scalar.Subquery q) {
            return new Scalar.Subquery(
                q.kind(),
                node(q.plan()),
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

  private Scalar scalarOrNull(Scalar s) {
    return s == null ? null : scalar(s);
  }

  private List<Scalar> scalars(List<Scalar> list) {
    List<Scalar> out = new ArrayList<>(list.size());
    for (Scalar s : list) {
      out.add(scalar(s));
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Filters

  private Plan filter(Plan.Filter f) {
    List<Scalar> conjuncts = Scalars.conjuncts(scalar(f.predicate()));
    return switch (f.input()) {
      case Plan.Scan s -> accessPath(s, conjuncts);
      case Plan.Join j -> join(j, conjuncts);
      default -> wrap(node(f.input()), conjuncts);
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

  private Plan accessPath(Plan.Scan s, List<Scalar> conjuncts) {
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
    // The whole primary key fixed: one get, always the cheapest.
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
    // Every index (primary first) that the conjuncts narrow, and the full scan: by cost.
    Plan best = wrap(new Plan.FullScan(t, s.alias(), s.columns()), conjuncts);
    double bestCost = estimator.est(best).cost();
    List<Index> candidates = new ArrayList<>();
    candidates.add(t.primary());
    for (Index i : t.indexes()) {
      if (i.state() == IndexState.PUBLIC) {
        candidates.add(i);
      }
    }
    for (Index i : candidates) {
      int[] cols = t.positions(i);
      List<Scalar> prefix = new ArrayList<>();
      int k = 0;
      while (k < cols.length && eq.containsKey(cols[k])) {
        prefix.add(eq.get(cols[k]));
        k++;
      }
      Bound lo = k < cols.length ? low.get(cols[k]) : null;
      Bound hi = k < cols.length ? high.get(cols[k]) : null;
      if (prefix.isEmpty() && lo == null && hi == null) {
        continue;
      }
      Plan candidate =
          wrap(new Plan.IndexScan(t, i, s.alias(), prefix, lo, hi, s.columns()), conjuncts);
      double cost = estimator.est(candidate).cost();
      if (cost < bestCost) {
        best = candidate;
        bestCost = cost;
      }
    }
    return best;
  }

  // -----------------------------------------------------------------------------------------------
  // Joins

  private Plan join(Plan.Join j, List<Scalar> where) {
    if (j.kind() == JoinKind.INNER) {
      Plan ordered = joinOrder(j, where);
      if (ordered != null) {
        return ordered;
      }
    }
    int width = j.left().columns().size();
    JoinKind kind = j.kind();
    List<Scalar> pushLeft = new ArrayList<>();
    List<Scalar> pushRight = new ArrayList<>();
    List<Scalar> keep = new ArrayList<>();
    List<Scalar> on = new ArrayList<>();
    // WHERE conjuncts: below the join when only one side is read, and an outer join's preserved
    // side is the one read.
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
    Plan joined = cheapestJoin(kind, node(left), right, on, j.columns(), width);
    return wrap(joined, keep);
  }

  /**
   * The cheapest physical join of {@code left} (planned) and {@code right} (logical, so a lookup
   * join can see its table) on {@code on} (over the joined row).
   */
  private Plan cheapestJoin(
      JoinKind kind, Plan left, Plan right, List<Scalar> on, List<Col> columns, int width) {
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
    Plan pr = node(right);
    List<Plan> candidates = new ArrayList<>();
    if (!leftKeys.isEmpty()) {
      candidates.add(
          new Plan.HashJoin(kind, left, pr, leftKeys, rightKeys, Scalars.and(residual), columns));
      if (kind == JoinKind.INNER || kind == JoinKind.LEFT) {
        Plan lookup = lookupJoin(kind, left, right, leftKeys, rightKeys, residual, columns);
        if (lookup != null) {
          candidates.add(lookup);
        }
      }
    } else {
      Scalar cond = Scalars.and(residual);
      candidates.add(
          new Plan.NestedLoopJoin(
              kind, left, pr, cond == null ? new Const(true, Type.BOOL) : cond, columns));
    }
    Plan best = null;
    double bestCost = Double.MAX_VALUE;
    for (Plan c : candidates) {
      double cost = estimator.est(c).cost();
      if (cost < bestCost) {
        best = c;
        bestCost = cost;
      }
    }
    return best;
  }

  /**
   * A lookup join when the right side is a table (perhaps filtered) and the keys fix its primary
   * key or a public unique index; null otherwise.
   */
  private static Plan lookupJoin(
      JoinKind kind,
      Plan left,
      Plan right,
      List<Scalar> leftKeys,
      List<Scalar> rightKeys,
      List<Scalar> residual,
      List<Col> columns) {
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
      return new Plan.LookupJoin(kind, left, t, i, scan.alias(), keys, Scalars.and(check), columns);
    }
    return null;
  }

  // -----------------------------------------------------------------------------------------------
  // Join order

  /** A relation of an inner-join group: its logical plan and where its columns start. */
  private record Leaf(Plan logical, int offset, int width) {}

  /** The cheapest plan found for a set of leaves, and where each leaf's columns sit in its row. */
  private record Cand(Plan plan, int[] layout, double cost, double rows) {}

  /**
   * Orders an inner-join group by dynamic programming over sets of its leaves; null when the group
   * is too large, or a single join (which {@link #join} plans directly).
   */
  private Plan joinOrder(Plan.Join top, List<Scalar> where) {
    List<Leaf> leaves = new ArrayList<>();
    List<Scalar> conjuncts = new ArrayList<>();
    if (!flatten(top, 0, leaves, conjuncts)) {
      return null;
    }
    conjuncts.addAll(where);
    int n = leaves.size();
    if (n < 3 || n > MAX_DP_TABLES) {
      return null;
    }
    int total = top.columns().size();
    int[] leafOf = new int[total];
    for (int i = 0; i < n; i++) {
      for (int c = 0; c < leaves.get(i).width(); c++) {
        leafOf[leaves.get(i).offset() + c] = i;
      }
    }
    // Conjuncts on one leaf go into it; on several, they join; with subqueries or none, on top.
    List<List<Scalar>> local = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      local.add(new ArrayList<>());
    }
    List<Scalar> joinConds = new ArrayList<>();
    List<Integer> condMasks = new ArrayList<>();
    List<Scalar> above = new ArrayList<>();
    for (Scalar c : conjuncts) {
      List<Integer> read = Scalars.columnsRead(c);
      if (Scalars.hasSubquery(c) || read.isEmpty()) {
        above.add(c);
        continue;
      }
      int mask = 0;
      for (int col : read) {
        mask |= 1 << leafOf[col];
      }
      if (Integer.bitCount(mask) == 1) {
        int i = Integer.numberOfTrailingZeros(mask);
        local.get(i).add(Binder.shift(c, -leaves.get(i).offset()));
      } else {
        joinConds.add(c);
        condMasks.add(mask);
      }
    }
    Map<Integer, Cand> best = new HashMap<>();
    Map<Integer, Plan> logicalLeaf = new HashMap<>();
    for (int i = 0; i < n; i++) {
      Leaf leaf = leaves.get(i);
      Plan logical =
          local.get(i).isEmpty()
              ? leaf.logical()
              : new Plan.Filter(leaf.logical(), Scalars.and(local.get(i)));
      logicalLeaf.put(1 << i, logical);
      Plan physical = node(logical);
      int[] layout = new int[total];
      java.util.Arrays.fill(layout, -1);
      for (int c = 0; c < leaf.width(); c++) {
        layout[leaf.offset() + c] = c;
      }
      Est e = estimator.est(physical);
      best.put(1 << i, new Cand(physical, layout, e.cost(), e.rows()));
    }
    int all = (1 << n) - 1;
    for (int size = 2; size <= n; size++) {
      for (int mask = 1; mask <= all; mask++) {
        if (Integer.bitCount(mask) != size) {
          continue;
        }
        boolean connected = false;
        for (int s1 = (mask - 1) & mask; s1 > 0; s1 = (s1 - 1) & mask) {
          if (links(s1, mask ^ s1, condMasks)) {
            connected = true;
            break;
          }
        }
        Cand winner = null;
        for (int s1 = (mask - 1) & mask; s1 > 0; s1 = (s1 - 1) & mask) {
          int s2 = mask ^ s1;
          Cand l = best.get(s1);
          Cand r = best.get(s2);
          if (l == null || r == null || (connected && !links(s1, s2, condMasks))) {
            continue;
          }
          Cand c = combine(l, r, s1, s2, joinConds, condMasks, logicalLeaf.get(s2), total);
          if (winner == null || c.cost() < winner.cost()) {
            winner = c;
          }
        }
        if (winner != null) {
          best.put(mask, winner);
        }
      }
    }
    Cand result = best.get(all);
    if (result == null) {
      return null;
    }
    // Back to the order the query wrote.
    List<Scalar> exprs = new ArrayList<>();
    for (int g = 0; g < total; g++) {
      exprs.add(new ColRef(result.layout()[g], top.columns().get(g).type()));
    }
    Plan ordered = new Plan.Project(result.plan(), exprs, top.columns());
    return wrap(ordered, above);
  }

  private static boolean links(int s1, int s2, List<Integer> condMasks) {
    for (int m : condMasks) {
      if ((m & ~(s1 | s2)) == 0 && (m & s1) != 0 && (m & s2) != 0) {
        return true;
      }
    }
    return false;
  }

  private Cand combine(
      Cand l,
      Cand r,
      int s1,
      int s2,
      List<Scalar> conds,
      List<Integer> condMasks,
      Plan rightLogicalLeaf,
      int total) {
    int width = l.plan().columns().size();
    int[] layout = new int[total];
    for (int g = 0; g < total; g++) {
      layout[g] =
          l.layout()[g] >= 0 ? l.layout()[g] : (r.layout()[g] >= 0 ? r.layout()[g] + width : -1);
    }
    List<Scalar> on = new ArrayList<>();
    for (int k = 0; k < conds.size(); k++) {
      int m = condMasks.get(k);
      if ((m & ~(s1 | s2)) == 0 && (m & s1) != 0 && (m & s2) != 0) {
        on.add(
            Scalars.map(
                conds.get(k),
                x -> x instanceof ColRef c ? new ColRef(layout[c.index()], c.type()) : null));
      }
    }
    List<Col> cols = Plan.concat(l.plan().columns(), r.plan().columns());
    // A lookup join needs the right side as a table: only a single leaf can be one.
    Plan right =
        Integer.bitCount(s2) == 1 && rightLogicalLeaf != null ? rightLogicalLeaf : r.plan();
    Plan joined = cheapestJoin(JoinKind.INNER, l.plan(), right, on, cols, width);
    Est e = estimator.est(joined);
    return new Cand(joined, layout, e.cost(), e.rows());
  }

  /**
   * The leaves and conditions of a tree of inner joins, columns at their place in its row; false
   * when a nested join's condition holds a subquery (its outer references name that join's own row,
   * which reordering would change), so the group keeps its written order.
   */
  private boolean flatten(Plan p, int offset, List<Leaf> leaves, List<Scalar> conjuncts) {
    if (p instanceof Plan.Join j && j.kind() == JoinKind.INNER) {
      if (!flatten(j.left(), offset, leaves, conjuncts)
          || !flatten(j.right(), offset + j.left().columns().size(), leaves, conjuncts)) {
        return false;
      }
      for (Scalar c : Scalars.conjuncts(scalar(j.on()))) {
        if (offset > 0 && Scalars.hasSubquery(c)) {
          return false;
        }
        conjuncts.add(Binder.shift(c, offset));
      }
      return true;
    }
    leaves.add(new Leaf(p, offset, p.columns().size()));
    return true;
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

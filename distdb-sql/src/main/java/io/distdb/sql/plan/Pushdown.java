package io.distdb.sql.plan;

import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.exec.FragmentCodec;
import io.distdb.sql.plan.Plan.Col;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Moves work to the ranges (a rewrite of the physical plan):
 *
 * <ul>
 *   <li>a scan of a table's primary index (whole, or a key range) becomes a {@link
 *       Plan.RemoteScan}, and the conjuncts above it that a range can evaluate go with it;
 *   <li>a projection above it lets the ranges send only the columns it reads (projection pruning);
 *   <li>an aggregation above it becomes partial aggregation on the ranges and a {@link
 *       Plan.MergeAggregate} on the gateway;
 *   <li>a constant LIMIT without ORDER BY caps what each range sends.
 * </ul>
 *
 * Point gets and secondary-index scans stay on the gateway: an index entry's row may live in
 * another range.
 */
final class Pushdown {

  private static final Set<String> MERGEABLE = Set.of("count", "sum", "avg", "min", "max");

  private Pushdown() {}

  static Plan apply(Plan p) {
    return switch (p) {
      case Plan.Aggregate a -> {
        Plan.RemoteScan partial = partialAggregate(a);
        yield partial != null
            ? new Plan.MergeAggregate(partial, a.groups().size(), a.aggs(), a.columns())
            : new Plan.Aggregate(
                apply(a.input()), scalars(a.groups()), aggs(a.aggs()), a.columns());
      }
      case Plan.Limit l -> {
        Plan input = apply(l.input());
        long cap = constantCap(l);
        if (cap >= 0
            && input instanceof Plan.Project pr
            && pr.input() instanceof Plan.RemoteScan r
            && !r.aggregating()) {
          input = new Plan.Project(withLimit(r, cap), pr.exprs(), pr.columns());
        } else if (cap >= 0 && input instanceof Plan.RemoteScan r && !r.aggregating()) {
          input = withLimit(r, cap);
        }
        yield new Plan.Limit(input, l.limit(), l.offset());
      }
      case Plan.Project pr -> {
        Plan below = remote(pr.input(), columnsRead(pr.exprs()));
        yield new Plan.Project(
            below != null ? below : apply(pr.input()), scalars(pr.exprs()), pr.columns());
      }
      case Plan.Filter f -> {
        Plan below = remote(f, null);
        yield below != null ? below : new Plan.Filter(apply(f.input()), scalar(f.predicate()));
      }
      case Plan.FullScan s -> remote(s, null);
      case Plan.IndexScan s -> {
        Plan below = remote(s, null);
        yield below != null ? below : s;
      }
      case Plan.NestedLoopJoin j ->
          new Plan.NestedLoopJoin(
              j.kind(), apply(j.left()), apply(j.right()), scalar(j.on()), j.columns());
      case Plan.HashJoin j ->
          new Plan.HashJoin(
              j.kind(),
              apply(j.left()),
              apply(j.right()),
              scalars(j.leftKeys()),
              scalars(j.rightKeys()),
              j.residual() == null ? null : scalar(j.residual()),
              j.columns());
      case Plan.LookupJoin j ->
          new Plan.LookupJoin(
              j.kind(),
              apply(j.left()),
              j.table(),
              j.index(),
              j.alias(),
              scalars(j.keys()),
              j.residual() == null ? null : scalar(j.residual()),
              j.columns());
      case Plan.Sort s -> {
        List<Plan.SortKey> keys = new ArrayList<>();
        for (Plan.SortKey k : s.keys()) {
          keys.add(new Plan.SortKey(scalar(k.expr()), k.desc(), k.nullsFirst()));
        }
        yield new Plan.Sort(apply(s.input()), keys);
      }
      case Plan.Distinct d -> new Plan.Distinct(apply(d.input()));
      case Plan.SetOp s ->
          new Plan.SetOp(s.kind(), s.all(), apply(s.left()), apply(s.right()), s.columns());
      case Plan.Values v -> {
        List<List<Scalar>> rows = new ArrayList<>();
        for (List<Scalar> r : v.rows()) {
          rows.add(scalars(r));
        }
        yield new Plan.Values(rows, v.columns());
      }
      default -> p;
    };
  }

  // -----------------------------------------------------------------------------------------------
  // Subqueries inside expressions get the same treatment

  private static Scalar scalar(Scalar s) {
    return Scalars.map(
        s,
        x -> {
          if (x instanceof Scalar.Subquery q) {
            return new Scalar.Subquery(
                q.kind(),
                apply(q.plan()),
                q.operand() == null ? null : scalar(q.operand()),
                q.not(),
                q.correlated(),
                q.type());
          }
          return x instanceof Scalar.OuterRef ? x : null;
        });
  }

  private static List<Scalar> scalars(List<Scalar> list) {
    List<Scalar> out = new ArrayList<>(list.size());
    for (Scalar s : list) {
      out.add(scalar(s));
    }
    return out;
  }

  private static List<Scalar.AggCall> aggs(List<Scalar.AggCall> list) {
    List<Scalar.AggCall> out = new ArrayList<>(list.size());
    for (Scalar.AggCall a : list) {
      out.add((Scalar.AggCall) scalar(a));
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Scans

  /** The primary-index scan under {@code p} (a scan, or a filter over one), or null. */
  private record Source(Plan.FullScan full, Plan.IndexScan range, List<Scalar> conjuncts) {
    TableDesc table() {
      return full != null ? full.table() : range.table();
    }

    String alias() {
      return full != null ? full.alias() : range.alias();
    }

    List<Col> columns() {
      return full != null ? full.columns() : range.columns();
    }
  }

  private static Source source(Plan p) {
    List<Scalar> conjuncts = new ArrayList<>();
    Plan s = p;
    if (s instanceof Plan.Filter f) {
      conjuncts.addAll(Scalars.conjuncts(f.predicate()));
      s = f.input();
    }
    if (s instanceof Plan.FullScan full) {
      return new Source(full, null, conjuncts);
    }
    if (s instanceof Plan.IndexScan range && range.index().id() == TableDesc.PRIMARY_INDEX_ID) {
      return new Source(null, range, conjuncts);
    }
    return null;
  }

  /**
   * {@code p} as a remote scan, keeping {@code read} columns (null: all), with the conjuncts a
   * range can evaluate pushed and the others in a filter above; null if {@code p} is no
   * primary-index scan.
   */
  private static Plan remote(Plan p, List<Integer> read) {
    Source src = source(p);
    if (src == null) {
      return null;
    }
    List<Scalar> pushed = new ArrayList<>();
    List<Scalar> kept = new ArrayList<>();
    for (Scalar c : src.conjuncts()) {
      (FragmentCodec.pushableAfterBinding(c) ? pushed : kept).add(c);
    }
    int width = src.table().columns().size();
    boolean[] keep = new boolean[width];
    boolean correlated = false;
    for (Scalar c : kept) {
      correlated |= readsThroughSubquery(c);
    }
    if (read == null || correlated) {
      // A correlated subquery reads the row through outer references, which columnsRead does not
      // see: keep every column.
      java.util.Arrays.fill(keep, true);
    } else {
      for (int c : read) {
        keep[c] = true;
      }
      for (Scalar c : kept) {
        for (int i : Scalars.columnsRead(c)) {
          keep[i] = true;
        }
      }
    }
    Plan.RemoteScan r = scan(src, Scalars.and(pushed), keep, List.of(), List.of());
    Scalar residual = Scalars.and(kept);
    return residual == null ? r : new Plan.Filter(r, scalar(residual));
  }

  private static Plan.RemoteScan scan(
      Source src, Scalar filter, boolean[] keep, List<Scalar> groups, List<Scalar.AggCall> aggs) {
    List<Scalar> prefix = src.range() != null ? src.range().prefix() : List.of();
    Plan.Bound low = src.range() != null ? src.range().low() : null;
    Plan.Bound high = src.range() != null ? src.range().high() : null;
    List<Col> cols = src.columns();
    if (!groups.isEmpty() || !aggs.isEmpty()) {
      cols = new ArrayList<>();
      for (int g = 0; g < groups.size(); g++) {
        cols.add(new Col(null, "group" + g, groups.get(g).type()));
      }
      for (Scalar.AggCall a : aggs) {
        cols.add(new Col(null, a.function(), a.type()));
        if (a.function().equals("avg")) {
          cols.add(new Col(null, "count", io.distdb.sql.types.Type.INT));
        }
      }
    }
    return new Plan.RemoteScan(
        src.table(),
        src.alias(),
        prefix,
        low,
        high,
        filter == null ? null : scalar(filter),
        keep,
        groups,
        aggs,
        -1,
        List.copyOf(cols));
  }

  /** The partial aggregation of {@code a} on the ranges, or null if it cannot be pushed. */
  private static Plan.RemoteScan partialAggregate(Plan.Aggregate a) {
    Source src = source(a.input());
    if (src == null) {
      return null;
    }
    for (Scalar c : src.conjuncts()) {
      if (!FragmentCodec.pushableAfterBinding(c)) {
        return null;
      }
    }
    for (Scalar g : a.groups()) {
      if (!FragmentCodec.pushableAfterBinding(g)) {
        return null;
      }
    }
    for (Scalar.AggCall c : a.aggs()) {
      if (c.distinct()
          || !MERGEABLE.contains(c.function())
          || (c.arg() != null && !FragmentCodec.pushableAfterBinding(c.arg()))) {
        return null;
      }
    }
    boolean[] none = new boolean[src.table().columns().size()];
    return scan(src, Scalars.and(src.conjuncts()), none, a.groups(), a.aggs());
  }

  private static Plan.RemoteScan withLimit(Plan.RemoteScan r, long cap) {
    return new Plan.RemoteScan(
        r.table(),
        r.alias(),
        r.prefix(),
        r.low(),
        r.high(),
        r.filter(),
        r.keep(),
        r.groups(),
        r.aggs(),
        cap,
        r.columns());
  }

  /** {@code limit + offset} when both are constants (each range must send that many), or -1. */
  private static long constantCap(Plan.Limit l) {
    if (!(l.limit() instanceof Scalar.Const c) || !(c.value() instanceof Long n) || n < 0) {
      return -1;
    }
    long offset = 0;
    if (l.offset() != null) {
      if (!(l.offset() instanceof Scalar.Const o) || !(o.value() instanceof Long m) || m < 0) {
        return -1;
      }
      offset = m;
    }
    return n + offset;
  }

  private static boolean readsThroughSubquery(Scalar e) {
    boolean[] reads = {false};
    Scalars.visit(e, x -> reads[0] |= x instanceof Scalar.Subquery q && q.correlated());
    return reads[0];
  }

  private static List<Integer> columnsRead(List<Scalar> exprs) {
    List<Integer> out = new ArrayList<>();
    for (Scalar e : exprs) {
      for (int c : Scalars.columnsRead(e)) {
        if (!out.contains(c)) {
          out.add(c);
        }
      }
      // A correlated subquery reads the row through outer references: keep every column.
      boolean[] reads = {false};
      Scalars.visit(e, x -> reads[0] |= x instanceof Scalar.Subquery q && q.correlated());
      if (reads[0]) {
        return null;
      }
    }
    return out;
  }
}

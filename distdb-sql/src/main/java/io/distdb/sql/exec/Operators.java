package io.distdb.sql.exec;

import io.distdb.common.Bytes;
import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.catalog.TableDesc.Index;
import io.distdb.sql.encoding.RowCodec;
import io.distdb.sql.kv.KvTxn;
import io.distdb.sql.parse.Ast.JoinKind;
import io.distdb.sql.parse.Ast.SetOpKind;
import io.distdb.sql.plan.Plan;
import io.distdb.sql.plan.Scalar;
import io.distdb.sql.types.Values;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Builds the iterators for a physical plan. */
public final class Operators {

  /** Rows fetched per scan request. */
  static final int PAGE = 256;

  private Operators() {}

  public static Operator build(Plan p, ExecContext ctx) {
    return switch (p) {
      case Plan.FullScan s ->
          new SpanScan(ctx, s.table(), s.table().primary(), () -> fullSpan(s.table()));
      case Plan.IndexScan s -> new SpanScan(ctx, s.table(), s.index(), () -> span(s, ctx));
      case Plan.PointGet g -> new PointGet(ctx, g);
      case Plan.Filter f -> new Filter(build(f.input(), ctx), f.predicate(), ctx);
      case Plan.Project pr -> new Project(build(pr.input(), ctx), pr.exprs(), ctx);
      case Plan.NestedLoopJoin j ->
          new HashJoin(
              ctx,
              j.kind(),
              build(j.left(), ctx),
              build(j.right(), ctx),
              List.of(),
              List.of(),
              j.on(),
              j.left().columns().size(),
              j.right().columns().size());
      case Plan.HashJoin j ->
          new HashJoin(
              ctx,
              j.kind(),
              build(j.left(), ctx),
              build(j.right(), ctx),
              j.leftKeys(),
              j.rightKeys(),
              j.residual(),
              j.left().columns().size(),
              j.right().columns().size());
      case Plan.LookupJoin j -> new LookupJoin(ctx, j, build(j.left(), ctx));
      case Plan.Aggregate a -> new Aggregate(ctx, build(a.input(), ctx), a);
      case Plan.Sort s -> new Sort(ctx, build(s.input(), ctx), s.keys());
      case Plan.Limit l -> new Limit(ctx, build(l.input(), ctx), l);
      case Plan.Distinct d -> new Distinct(build(d.input(), ctx));
      case Plan.Values v -> new ValuesOp(ctx, v.rows());
      case Plan.SetOp s ->
          new SetOp(s.kind(), s.all(), build(s.left(), ctx), build(s.right(), ctx));
      case Plan.OneRow o -> new ValuesOp(ctx, List.of(List.of()));
      case Plan.Scan s -> throw new IllegalStateException("a logical scan was not planned");
      case Plan.Join j -> throw new IllegalStateException("a logical join was not planned");
    };
  }

  /** Runs {@code op} to the end and returns every row. */
  public static List<Object[]> drain(Operator op) {
    List<Object[]> out = new ArrayList<>();
    op.open();
    try {
      for (Object[] r = op.next(); r != null; r = op.next()) {
        out.add(r);
      }
    } finally {
      op.close();
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Scans

  /** A key range; {@code empty} when a bound is NULL (nothing can match). */
  record Span(byte[] start, byte[] end, boolean empty) {}

  static Span fullSpan(TableDesc t) {
    byte[] start = RowCodec.indexPrefix(t.id(), TableDesc.PRIMARY_INDEX_ID);
    return new Span(start, Bytes.prefixEnd(start).orElseThrow(), false);
  }

  private static Span span(Plan.IndexScan s, ExecContext ctx) {
    Object[] none = new Object[0];
    List<Object> prefix = new ArrayList<>();
    for (Scalar e : s.prefix()) {
      Object v = Eval.eval(e, none, ctx);
      if (v == null) {
        return new Span(null, null, true);
      }
      prefix.add(v);
    }
    byte[] base = RowCodec.indexPrefix(s.table(), s.index(), prefix);
    byte[] start = base;
    byte[] end = Bytes.prefixEnd(base).orElseThrow();
    if (s.low() != null) {
      Object v = Eval.eval(s.low().value(), none, ctx);
      if (v == null) {
        return new Span(null, null, true);
      }
      List<Object> withLow = new ArrayList<>(prefix);
      withLow.add(v);
      byte[] key = RowCodec.indexPrefix(s.table(), s.index(), withLow);
      start = s.low().inclusive() ? key : Bytes.prefixEnd(key).orElseThrow();
    }
    if (s.high() != null) {
      Object v = Eval.eval(s.high().value(), none, ctx);
      if (v == null) {
        return new Span(null, null, true);
      }
      List<Object> withHigh = new ArrayList<>(prefix);
      withHigh.add(v);
      byte[] key = RowCodec.indexPrefix(s.table(), s.index(), withHigh);
      end = s.high().inclusive() ? Bytes.prefixEnd(key).orElseThrow() : key;
    }
    return new Span(start, end, false);
  }

  /**
   * Reads an index's entries in a key range, page by page, and yields the rows: directly for the
   * primary index, through a get of the primary key for a secondary one.
   */
  private static final class SpanScan implements Operator {
    private final ExecContext ctx;
    private final TableDesc table;
    private final Index index;
    private final java.util.function.Supplier<Span> spanOf;
    private Span span;
    private List<KvTxn.Entry> page = List.of();
    private int pos;
    private byte[] resume;
    private boolean exhausted;

    SpanScan(
        ExecContext ctx, TableDesc table, Index index, java.util.function.Supplier<Span> spanOf) {
      this.ctx = ctx;
      this.table = table;
      this.index = index;
      this.spanOf = spanOf;
    }

    @Override
    public void open() {
      span = spanOf.get();
      exhausted = span.empty() || Arrays.compareUnsigned(span.start(), span.end()) >= 0;
      resume = span.start();
      page = List.of();
      pos = 0;
    }

    @Override
    public Object[] next() {
      while (pos >= page.size()) {
        if (exhausted) {
          return null;
        }
        page = ctx.txn.scan(resume, span.end(), PAGE);
        pos = 0;
        if (page.size() < PAGE) {
          exhausted = true;
        } else {
          resume = Bytes.successor(page.get(page.size() - 1).key());
        }
      }
      KvTxn.Entry e = page.get(pos++);
      if (index.id() == TableDesc.PRIMARY_INDEX_ID) {
        return RowCodec.decodeRow(table, e.key(), e.value());
      }
      byte[] pk = RowCodec.primaryKeyOf(table, index, e.key(), e.value());
      Optional<byte[]> row = ctx.txn.get(pk);
      if (row.isEmpty()) {
        throw RowCodec.corrupt(table, index);
      }
      return RowCodec.decodeRow(table, pk, row.get());
    }

    @Override
    public void close() {
      page = List.of();
    }
  }

  private static final class PointGet implements Operator {
    private final ExecContext ctx;
    private final Plan.PointGet plan;
    private Object[] row;

    PointGet(ExecContext ctx, Plan.PointGet plan) {
      this.ctx = ctx;
      this.plan = plan;
    }

    @Override
    public void open() {
      row =
          fetch(
              ctx, plan.table(), plan.table().primary(), Eval.all(plan.key(), new Object[0], ctx));
    }

    @Override
    public Object[] next() {
      Object[] r = row;
      row = null;
      return r;
    }

    @Override
    public void close() {
      row = null;
    }
  }

  /** The row whose unique key in {@code index} is {@code key}, or null. */
  static Object[] fetch(ExecContext ctx, TableDesc t, Index index, Object[] key) {
    for (Object v : key) {
      if (v == null) {
        return null;
      }
    }
    Object[] probe = new Object[t.columns().size()];
    int[] positions = t.positions(index);
    for (int k = 0; k < positions.length; k++) {
      probe[positions[k]] = key[k];
    }
    byte[] pk;
    if (index.id() == TableDesc.PRIMARY_INDEX_ID) {
      pk = RowCodec.primaryKey(t, probe);
    } else {
      byte[] ik = RowCodec.indexKey(t, index, probe);
      Optional<byte[]> entry = ctx.txn.get(ik);
      if (entry.isEmpty()) {
        return null;
      }
      pk = RowCodec.primaryKeyOf(t, index, ik, entry.get());
    }
    Optional<byte[]> value = ctx.txn.get(pk);
    if (value.isEmpty()) {
      if (index.id() != TableDesc.PRIMARY_INDEX_ID) {
        throw RowCodec.corrupt(t, index);
      }
      return null;
    }
    return RowCodec.decodeRow(t, pk, value.get());
  }

  // -----------------------------------------------------------------------------------------------
  // Row by row

  private static final class Filter implements Operator {
    private final Operator input;
    private final Scalar predicate;
    private final ExecContext ctx;

    Filter(Operator input, Scalar predicate, ExecContext ctx) {
      this.input = input;
      this.predicate = predicate;
      this.ctx = ctx;
    }

    @Override
    public void open() {
      input.open();
    }

    @Override
    public Object[] next() {
      for (Object[] r = input.next(); r != null; r = input.next()) {
        if (Eval.holds(predicate, r, ctx)) {
          return r;
        }
      }
      return null;
    }

    @Override
    public void close() {
      input.close();
    }
  }

  private static final class Project implements Operator {
    private final Operator input;
    private final List<Scalar> exprs;
    private final ExecContext ctx;

    Project(Operator input, List<Scalar> exprs, ExecContext ctx) {
      this.input = input;
      this.exprs = exprs;
      this.ctx = ctx;
    }

    @Override
    public void open() {
      input.open();
    }

    @Override
    public Object[] next() {
      Object[] r = input.next();
      return r == null ? null : Eval.all(exprs, r, ctx);
    }

    @Override
    public void close() {
      input.close();
    }
  }

  private static final class ValuesOp implements Operator {
    private final ExecContext ctx;
    private final List<List<Scalar>> rows;
    private int next;

    ValuesOp(ExecContext ctx, List<List<Scalar>> rows) {
      this.ctx = ctx;
      this.rows = rows;
    }

    @Override
    public void open() {
      next = 0;
    }

    @Override
    public Object[] next() {
      return next < rows.size() ? Eval.all(rows.get(next++), new Object[0], ctx) : null;
    }

    @Override
    public void close() {}
  }

  // -----------------------------------------------------------------------------------------------
  // Joins

  private static Object[] concat(Object[] left, Object[] right, int rightWidth) {
    Object[] out = Arrays.copyOf(left, left.length + rightWidth);
    if (right != null) {
      System.arraycopy(right, 0, out, left.length, rightWidth);
    }
    return out;
  }

  private static Object[] nullsThen(int leftWidth, Object[] right) {
    Object[] out = new Object[leftWidth + right.length];
    System.arraycopy(right, 0, out, leftWidth, right.length);
    return out;
  }

  /** A join key, or null when a part of it is NULL (such a row matches nothing). */
  private static List<Object> key(List<Scalar> exprs, Object[] row, ExecContext ctx) {
    List<Object> k = new ArrayList<>(exprs.size());
    for (Scalar e : exprs) {
      Object v = Eval.eval(e, row, ctx);
      if (v == null) {
        return null;
      }
      k.add(Values.hashKey(v));
    }
    return k;
  }

  /**
   * A hash join (on {@code leftKeys = rightKeys}) or, with no keys, a nested loop: builds the right
   * side in memory, then streams the left. Every kind: INNER, LEFT, RIGHT and FULL.
   */
  private static final class HashJoin implements Operator {
    private final ExecContext ctx;
    private final JoinKind kind;
    private final Operator left;
    private final Operator right;
    private final List<Scalar> leftKeys;
    private final List<Scalar> rightKeys;
    private final Scalar residual;
    private final int leftWidth;
    private final int rightWidth;
    private List<Object[]> rightRows;
    private Map<List<Object>, List<Integer>> table;
    private boolean[] matched;
    private Object[] current;
    private List<Integer> candidates;
    private int candidate;
    private boolean currentMatched;
    private int unmatchedScan = -1;

    HashJoin(
        ExecContext ctx,
        JoinKind kind,
        Operator left,
        Operator right,
        List<Scalar> leftKeys,
        List<Scalar> rightKeys,
        Scalar residual,
        int leftWidth,
        int rightWidth) {
      this.ctx = ctx;
      this.kind = kind;
      this.left = left;
      this.right = right;
      this.leftKeys = leftKeys;
      this.rightKeys = rightKeys;
      this.residual = residual;
      this.leftWidth = leftWidth;
      this.rightWidth = rightWidth;
    }

    @Override
    public void open() {
      rightRows = drain(right);
      matched = new boolean[rightRows.size()];
      if (!rightKeys.isEmpty()) {
        table = new HashMap<>();
        for (int i = 0; i < rightRows.size(); i++) {
          List<Object> k = key(rightKeys, rightRows.get(i), ctx);
          if (k != null) {
            table.computeIfAbsent(k, x -> new ArrayList<>()).add(i);
          }
        }
      }
      left.open();
      current = null;
      unmatchedScan = -1;
    }

    private List<Integer> candidatesFor(Object[] l) {
      if (rightKeys.isEmpty()) {
        List<Integer> all = new ArrayList<>(rightRows.size());
        for (int i = 0; i < rightRows.size(); i++) {
          all.add(i);
        }
        return all;
      }
      List<Object> k = key(leftKeys, l, ctx);
      if (k == null) {
        return List.of();
      }
      return table.getOrDefault(k, List.of());
    }

    @Override
    public Object[] next() {
      while (true) {
        if (unmatchedScan >= 0) {
          while (unmatchedScan < rightRows.size()) {
            int i = unmatchedScan++;
            if (!matched[i]) {
              return nullsThen(leftWidth, rightRows.get(i));
            }
          }
          return null;
        }
        if (current == null) {
          current = left.next();
          if (current == null) {
            if (kind == JoinKind.RIGHT || kind == JoinKind.FULL) {
              unmatchedScan = 0;
              continue;
            }
            return null;
          }
          candidates = candidatesFor(current);
          candidate = 0;
          currentMatched = false;
        }
        while (candidate < candidates.size()) {
          int i = candidates.get(candidate++);
          Object[] row = concat(current, rightRows.get(i), rightWidth);
          if (residual == null || Eval.holds(residual, row, ctx)) {
            matched[i] = true;
            currentMatched = true;
            return row;
          }
        }
        Object[] l = current;
        current = null;
        if (!currentMatched && (kind == JoinKind.LEFT || kind == JoinKind.FULL)) {
          return concat(l, null, rightWidth);
        }
      }
    }

    @Override
    public void close() {
      left.close();
      rightRows = null;
      table = null;
    }
  }

  /** For each left row, a get by the right table's unique key. INNER or LEFT. */
  private static final class LookupJoin implements Operator {
    private final ExecContext ctx;
    private final Plan.LookupJoin plan;
    private final Operator left;
    private final int rightWidth;

    LookupJoin(ExecContext ctx, Plan.LookupJoin plan, Operator left) {
      this.ctx = ctx;
      this.plan = plan;
      this.left = left;
      this.rightWidth = plan.table().columns().size();
    }

    @Override
    public void open() {
      left.open();
    }

    @Override
    public Object[] next() {
      for (Object[] l = left.next(); l != null; l = left.next()) {
        Object[] r = fetch(ctx, plan.table(), plan.index(), Eval.all(plan.keys(), l, ctx));
        if (r != null) {
          Object[] row = concat(l, r, rightWidth);
          if (plan.residual() == null || Eval.holds(plan.residual(), row, ctx)) {
            return row;
          }
        }
        if (plan.kind() == JoinKind.LEFT) {
          return concat(l, null, rightWidth);
        }
      }
      return null;
    }

    @Override
    public void close() {
      left.close();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Aggregation

  /** One aggregate's running state. */
  private interface Acc {
    void add(Object v);

    Object result();
  }

  private static Acc accumulator(Scalar.AggCall a) {
    Acc acc =
        switch (a.function()) {
          case "count" ->
              new Acc() {
                long n;

                @Override
                public void add(Object v) {
                  if (v != null || a.arg() == null) {
                    n++;
                  }
                }

                @Override
                public Object result() {
                  return n;
                }
              };
          case "sum" ->
              new Acc() {
                Object sum;

                @Override
                public void add(Object v) {
                  if (v != null) {
                    sum = sum == null ? v : Values.add(sum, v);
                  }
                }

                @Override
                public Object result() {
                  return sum;
                }
              };
          case "avg" ->
              new Acc() {
                Object sum;
                long n;

                @Override
                public void add(Object v) {
                  if (v == null) {
                    return;
                  }
                  Object x = v instanceof Long l ? BigDecimal.valueOf(l) : v;
                  sum = sum == null ? x : Values.add(sum, x);
                  n++;
                }

                @Override
                public Object result() {
                  if (n == 0) {
                    return null;
                  }
                  if (sum instanceof Double d) {
                    return d / n;
                  }
                  return Values.divideNumeric((BigDecimal) sum, BigDecimal.valueOf(n));
                }
              };
          case "min", "max" ->
              new Acc() {
                Object best;

                @Override
                public void add(Object v) {
                  if (v == null) {
                    return;
                  }
                  int cmp = best == null ? 0 : Values.compare(v, best);
                  if (best == null || (a.function().equals("max") ? cmp > 0 : cmp < 0)) {
                    best = v;
                  }
                }

                @Override
                public Object result() {
                  return best;
                }
              };
          default -> throw new IllegalStateException(a.function());
        };
    if (!a.distinct()) {
      return acc;
    }
    Set<Object> seen = new HashSet<>();
    return new Acc() {
      @Override
      public void add(Object v) {
        if (v != null && seen.add(Values.hashKey(v))) {
          acc.add(v);
        }
      }

      @Override
      public Object result() {
        return acc.result();
      }
    };
  }

  /** Hash aggregation; groups come out in the order they were first seen. */
  private static final class Aggregate implements Operator {
    private final ExecContext ctx;
    private final Operator input;
    private final Plan.Aggregate plan;
    private java.util.Iterator<Object[]> out;

    Aggregate(ExecContext ctx, Operator input, Plan.Aggregate plan) {
      this.ctx = ctx;
      this.input = input;
      this.plan = plan;
    }

    @Override
    public void open() {
      record Group(Object[] values, Acc[] accs) {}
      Map<List<Object>, Group> groups = new LinkedHashMap<>();
      int ng = plan.groups().size();
      input.open();
      try {
        for (Object[] r = input.next(); r != null; r = input.next()) {
          Object[] g = Eval.all(plan.groups(), r, ctx);
          List<Object> k = new ArrayList<>(ng);
          for (Object v : g) {
            k.add(Values.hashKey(v));
          }
          Group group = groups.get(k);
          if (group == null) {
            Acc[] accs = new Acc[plan.aggs().size()];
            for (int i = 0; i < accs.length; i++) {
              accs[i] = accumulator(plan.aggs().get(i));
            }
            group = new Group(g, accs);
            groups.put(k, group);
          }
          for (int i = 0; i < group.accs().length; i++) {
            Scalar arg = plan.aggs().get(i).arg();
            group.accs()[i].add(arg == null ? Boolean.TRUE : Eval.eval(arg, r, ctx));
          }
        }
      } finally {
        input.close();
      }
      if (groups.isEmpty() && ng == 0) {
        // No GROUP BY: one row even for no input (count 0, sum NULL).
        Acc[] accs = new Acc[plan.aggs().size()];
        for (int i = 0; i < accs.length; i++) {
          accs[i] = accumulator(plan.aggs().get(i));
        }
        groups.put(List.of(), new Group(new Object[0], accs));
      }
      List<Object[]> rows = new ArrayList<>();
      for (Group g : groups.values()) {
        Object[] row = Arrays.copyOf(g.values(), ng + g.accs().length);
        for (int i = 0; i < g.accs().length; i++) {
          row[ng + i] = g.accs()[i].result();
        }
        rows.add(row);
      }
      out = rows.iterator();
    }

    @Override
    public Object[] next() {
      return out.hasNext() ? out.next() : null;
    }

    @Override
    public void close() {
      out = null;
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Sort, limit, distinct, set operations

  private static final class Sort implements Operator {
    private final ExecContext ctx;
    private final Operator input;
    private final List<Plan.SortKey> keys;
    private java.util.Iterator<Object[]> out;

    Sort(ExecContext ctx, Operator input, List<Plan.SortKey> keys) {
      this.ctx = ctx;
      this.input = input;
      this.keys = keys;
    }

    @Override
    public void open() {
      record Keyed(Object[] keys, Object[] row) {}
      List<Keyed> rows = new ArrayList<>();
      List<Scalar> exprs = keys.stream().map(Plan.SortKey::expr).toList();
      for (Object[] r : drain(input)) {
        rows.add(new Keyed(Eval.all(exprs, r, ctx), r));
      }
      rows.sort((a, b) -> {
        for (int k = 0; k < keys.size(); k++) {
          Plan.SortKey key = keys.get(k);
          Object x = a.keys()[k];
          Object y = b.keys()[k];
          int c;
          if (x == null || y == null) {
            c = x == null && y == null ? 0 : (x == null ? -1 : 1);
            c = key.nullsFirst() ? c : -c;
          } else {
            c = Values.compare(x, y);
            c = key.desc() ? -c : c;
          }
          if (c != 0) {
            return c;
          }
        }
        return 0;
      });
      out = rows.stream().map(Keyed::row).iterator();
    }

    @Override
    public Object[] next() {
      return out.hasNext() ? out.next() : null;
    }

    @Override
    public void close() {
      out = null;
    }
  }

  private static final class Limit implements Operator {
    private final ExecContext ctx;
    private final Operator input;
    private final Plan.Limit plan;
    private long remaining;

    Limit(ExecContext ctx, Operator input, Plan.Limit plan) {
      this.ctx = ctx;
      this.input = input;
      this.plan = plan;
    }

    @Override
    public void open() {
      Object[] none = new Object[0];
      Object limit = plan.limit() == null ? null : Eval.eval(plan.limit(), none, ctx);
      Object offset = plan.offset() == null ? null : Eval.eval(plan.offset(), none, ctx);
      if (limit instanceof Long l && l < 0) {
        throw new SqlException(SqlState.INVALID_ROW_COUNT, "LIMIT must not be negative");
      }
      if (offset instanceof Long o && o < 0) {
        throw new SqlException("2201X", "OFFSET must not be negative");
      }
      remaining = limit == null ? Long.MAX_VALUE : (Long) limit;
      input.open();
      long skip = offset == null ? 0 : (Long) offset;
      while (skip-- > 0 && remaining > 0 && input.next() != null) {
        // skipped
      }
    }

    @Override
    public Object[] next() {
      if (remaining <= 0) {
        return null;
      }
      Object[] r = input.next();
      if (r != null) {
        remaining--;
      }
      return r;
    }

    @Override
    public void close() {
      input.close();
    }
  }

  private static List<Object> rowKey(Object[] row) {
    List<Object> k = new ArrayList<>(row.length);
    for (Object v : row) {
      k.add(Values.hashKey(v));
    }
    return k;
  }

  private static final class Distinct implements Operator {
    private final Operator input;
    private Set<List<Object>> seen;

    Distinct(Operator input) {
      this.input = input;
    }

    @Override
    public void open() {
      seen = new HashSet<>();
      input.open();
    }

    @Override
    public Object[] next() {
      for (Object[] r = input.next(); r != null; r = input.next()) {
        if (seen.add(rowKey(r))) {
          return r;
        }
      }
      return null;
    }

    @Override
    public void close() {
      input.close();
      seen = null;
    }
  }

  private static final class SetOp implements Operator {
    private final SetOpKind kind;
    private final boolean all;
    private final Operator left;
    private final Operator right;
    private java.util.Iterator<Object[]> out;

    SetOp(SetOpKind kind, boolean all, Operator left, Operator right) {
      this.kind = kind;
      this.all = all;
      this.left = left;
      this.right = right;
    }

    @Override
    public void open() {
      List<Object[]> l = drain(left);
      List<Object[]> r = drain(right);
      List<Object[]> rows = new ArrayList<>();
      switch (kind) {
        case UNION -> {
          if (all) {
            rows.addAll(l);
            rows.addAll(r);
          } else {
            Set<List<Object>> seen = new HashSet<>();
            for (List<Object[]> side : List.of(l, r)) {
              for (Object[] row : side) {
                if (seen.add(rowKey(row))) {
                  rows.add(row);
                }
              }
            }
          }
        }
        case INTERSECT, EXCEPT -> {
          Map<List<Object>, Integer> counts = new HashMap<>();
          for (Object[] row : r) {
            counts.merge(rowKey(row), 1, Integer::sum);
          }
          Set<List<Object>> emitted = new HashSet<>();
          for (Object[] row : l) {
            List<Object> k = rowKey(row);
            int c = counts.getOrDefault(k, 0);
            boolean keep;
            if (kind == SetOpKind.INTERSECT) {
              keep = c > 0;
              if (keep && all) {
                counts.put(k, c - 1);
              }
            } else {
              keep = c == 0;
              if (!keep && all) {
                counts.put(k, c - 1);
              }
            }
            if (keep && (all || emitted.add(k))) {
              rows.add(row);
            }
          }
        }
      }
      out = rows.iterator();
    }

    @Override
    public Object[] next() {
      return out.hasNext() ? out.next() : null;
    }

    @Override
    public void close() {
      out = null;
    }
  }
}

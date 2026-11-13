package io.distdb.sql.plan;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.catalog.Catalog;
import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.parse.Ast;
import io.distdb.sql.parse.Ast.Expr;
import io.distdb.sql.parse.Ast.JoinKind;
import io.distdb.sql.parse.Ast.SelectItem;
import io.distdb.sql.parse.Parser;
import io.distdb.sql.plan.Plan.Col;
import io.distdb.sql.plan.Scalar.AggCall;
import io.distdb.sql.plan.Scalar.ColRef;
import io.distdb.sql.plan.Scalar.Const;
import io.distdb.sql.types.Type;
import io.distdb.sql.types.TypeSpec;
import io.distdb.sql.types.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Turns a parsed statement into a logical plan: resolves names in scopes, decides types (inserting
 * casts as PostgreSQL would), and builds the relational operators.
 *
 * <p>A scope holds the columns visible at one query level, each as an expression over that level's
 * input row. A subquery's scope points to its enclosing one; a name found there becomes an {@link
 * Scalar.OuterRef}, and the subquery is correlated. With {@code GROUP BY} or an aggregate, the
 * select list is rewritten over the aggregation's output: every column must be grouped or inside an
 * aggregate.
 */
public final class Binder {

  /** One visible column: how to compute it from the level's row, and how it can be named. */
  private record Entry(
      String qualifier, String name, Scalar expr, boolean starHidden, boolean unqualifiedHidden) {}

  private static final class Scope {
    final Scope parent;
    final List<Entry> entries;

    Scope(Scope parent, List<Entry> entries) {
      this.parent = parent;
      this.entries = entries;
    }
  }

  /** A common table expression: a bound plan, shared by every reference. */
  private record CteDef(Plan plan, List<String> names) {}

  /** What an expression may contain where it is being bound. */
  private enum Clause {
    PLAIN,
    WHERE,
    GROUP_BY,
    /** Aggregates allowed: the select list, HAVING, ORDER BY. */
    AGGREGATING,
    /** Inside an aggregate's argument: no nested aggregates. */
    IN_AGGREGATE,
    VALUES,
    LIMIT
  }

  private final Catalog.Snapshot catalog;
  private final List<Type> params;
  private Map<String, CteDef> ctesInScope = Map.of();
  private boolean sawAggregate;

  /**
   * @param paramHints types a client declared for {@code $1, $2, ...} (UNKNOWN: infer)
   */
  public Binder(Catalog.Snapshot catalog, List<Type> paramHints) {
    this.catalog = Objects.requireNonNull(catalog);
    this.params = new ArrayList<>(paramHints);
  }

  /**
   * Parameter types, inferred from context where the client did not give them (TEXT if no clue).
   */
  public List<Type> paramTypes() {
    List<Type> out = new ArrayList<>();
    for (Type t : params) {
      out.add(t == Type.UNKNOWN ? Type.TEXT : t);
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Statements

  public Bound bind(Ast.Statement s) {
    Bound b =
        switch (s) {
          case Ast.QueryStatement q -> new Bound.Query(bindQuery(q.query(), null, Map.of()));
          case Ast.Insert i -> bindInsert(i);
          case Ast.Update u -> bindUpdate(u);
          case Ast.Delete d -> bindDelete(d);
          default -> throw new IllegalArgumentException("not a DML statement: " + s);
        };
    // Parameters nothing typed are text.
    for (int k = 0; k < params.size(); k++) {
      if (params.get(k) == Type.UNKNOWN) {
        params.set(k, Type.TEXT);
      }
    }
    return b;
  }

  private Bound bindInsert(Ast.Insert ins) {
    TableDesc t = catalog.table(ins.table());
    List<Integer> targets = new ArrayList<>();
    if (ins.columns() == null) {
      for (int p = 0; p < t.columns().size(); p++) {
        if (!t.columns().get(p).hidden()) {
          targets.add(p);
        }
      }
    } else {
      for (String c : ins.columns()) {
        int p = t.positionOrThrow(c);
        if (targets.contains(p)) {
          throw new SqlException(
              SqlState.DUPLICATE_COLUMN, "column \"" + c + "\" specified more than once");
        }
        targets.add(p);
      }
    }
    List<Scalar> defaults = defaults(t);
    Plan source;
    Ast.Query q = ins.source();
    if (q.body() instanceof Ast.Values v
        && q.orderBy().isEmpty()
        && q.limit() == null
        && q.offset() == null
        && q.with().isEmpty()) {
      source = bindInsertValues(v, t, targets, defaults);
    } else {
      source = bindQueryKeepingUnknown(q, null, Map.of());
    }
    if (source.columns().size() > targets.size()) {
      throw new SqlException(
          SqlState.SYNTAX_ERROR, "INSERT has more expressions than target columns");
    }
    if (source.columns().size() < targets.size() && ins.columns() != null) {
      throw new SqlException(
          SqlState.SYNTAX_ERROR, "INSERT has more target columns than expressions");
    }
    int n = source.columns().size();
    int[] tg = new int[n];
    Type[] types = new Type[n];
    for (int k = 0; k < n; k++) {
      tg[k] = targets.get(k);
      types[k] = source.columns().get(k).type();
      checkAssignable(t, tg[k], types[k]);
    }
    return new Bound.Insert(
        t, source, tg, types, defaults, ins.onConflictDoNothing(), returning(t, ins.returning()));
  }

  /** VALUES directly under INSERT: untyped literals and parameters take the column's type. */
  private Plan bindInsertValues(
      Ast.Values v, TableDesc t, List<Integer> targets, List<Scalar> defaults) {
    Scope empty = new Scope(null, List.of());
    List<List<Scalar>> rows = new ArrayList<>();
    int width = -1;
    for (List<Expr> row : v.rows()) {
      if (width >= 0 && row.size() != width) {
        throw new SqlException(SqlState.SYNTAX_ERROR, "VALUES lists must all be the same length");
      }
      width = row.size();
      if (width > targets.size()) {
        throw new SqlException(
            SqlState.SYNTAX_ERROR, "INSERT has more expressions than target columns");
      }
      List<Scalar> out = new ArrayList<>();
      for (int k = 0; k < row.size(); k++) {
        int p = targets.get(k);
        Type ct = t.columns().get(p).type().type();
        if (row.get(k) instanceof Ast.Default) {
          Scalar d = defaults.get(p);
          out.add(d == null ? new Const(null, ct) : d);
        } else {
          Scalar s = expr(row.get(k), empty, Clause.VALUES);
          out.add(s.type() == Type.UNKNOWN ? coerce(s, ct) : s);
        }
      }
      rows.add(out);
    }
    // Each column's type: the common type of its rows.
    List<Col> cols = new ArrayList<>();
    for (int k = 0; k < width; k++) {
      Type ct = Type.UNKNOWN;
      for (List<Scalar> r : rows) {
        ct = commonType(ct, r.get(k).type(), "VALUES");
      }
      if (ct == Type.UNKNOWN) {
        ct = t.columns().get(targets.get(k)).type().type();
      }
      for (List<Scalar> r : rows) {
        r.set(k, coerce(r.get(k), ct));
      }
      cols.add(new Col(null, "column" + (k + 1), ct));
    }
    return new Plan.Values(rows.stream().map(List::copyOf).toList(), cols);
  }

  /** The column defaults of {@code t}, bound over no row (null: NULL). */
  private List<Scalar> defaults(TableDesc t) {
    List<Scalar> out = new ArrayList<>();
    Scope empty = new Scope(null, List.of());
    for (TableDesc.Column c : t.columns()) {
      if (c.defaultSql() == null) {
        out.add(null);
      } else {
        Scalar d = expr(Parser.parseExpression(c.defaultSql()), empty, Clause.VALUES);
        checkAssignableType(c, d.type());
        out.add(coerce(d, c.type().type()));
      }
    }
    return out;
  }

  /** Checks that a column's default expression binds and fits the column's type. */
  public void checkDefault(Expr e, TableDesc.Column c) {
    Scalar d = expr(e, new Scope(null, List.of()), Clause.VALUES);
    checkAssignableType(c, d.type());
  }

  private void checkAssignable(TableDesc t, int position, Type from) {
    checkAssignableType(t.columns().get(position), from);
  }

  private static void checkAssignableType(TableDesc.Column c, Type from) {
    Type to = c.type().type();
    boolean ok =
        from == to
            || from == Type.UNKNOWN
            || to == Type.TEXT
            || (from.isNumeric() && to.isNumeric());
    if (!ok) {
      throw new SqlException(
          SqlState.DATATYPE_MISMATCH,
          "column \""
              + c.name()
              + "\" is of type "
              + c.type().sqlName()
              + " but expression is of type "
              + from.sqlName());
    }
  }

  private Bound bindUpdate(Ast.Update u) {
    TableDesc t = catalog.table(u.table());
    String alias = u.alias() == null ? t.name() : u.alias();
    Scope scope = tableScope(t, alias);
    Plan source = new Plan.Scan(t, alias, tableCols(t, alias));
    if (u.where() != null) {
      source = new Plan.Filter(source, bool(expr(u.where(), scope, Clause.WHERE), "WHERE"));
    }
    List<Scalar> defaults = defaults(t);
    int[] targets = new int[u.set().size()];
    List<Scalar> values = new ArrayList<>();
    for (int k = 0; k < targets.length; k++) {
      Ast.Assignment a = u.set().get(k);
      int p = t.positionOrThrow(a.column());
      for (int j = 0; j < k; j++) {
        if (targets[j] == p) {
          throw new SqlException(
              SqlState.SYNTAX_ERROR, "multiple assignments to same column \"" + a.column() + "\"");
        }
      }
      targets[k] = p;
      TableDesc.Column c = t.columns().get(p);
      Scalar v;
      if (a.value() instanceof Ast.Default) {
        v = defaults.get(p) == null ? new Const(null, c.type().type()) : defaults.get(p);
      } else {
        v = expr(a.value(), scope, Clause.WHERE);
        if (v.type() == Type.UNKNOWN) {
          v = coerce(v, c.type().type());
        }
        checkAssignableType(c, v.type());
      }
      values.add(v);
    }
    return new Bound.Update(t, source, targets, values, returning(t, u.returning()));
  }

  private Bound bindDelete(Ast.Delete d) {
    TableDesc t = catalog.table(d.table());
    String alias = d.alias() == null ? t.name() : d.alias();
    Scope scope = tableScope(t, alias);
    Plan source = new Plan.Scan(t, alias, tableCols(t, alias));
    if (d.where() != null) {
      source = new Plan.Filter(source, bool(expr(d.where(), scope, Clause.WHERE), "WHERE"));
    }
    return new Bound.Delete(t, source, returning(t, d.returning()));
  }

  private Bound.Returning returning(TableDesc t, List<SelectItem> items) {
    if (items.isEmpty()) {
      return Bound.Returning.NONE;
    }
    Scope scope = tableScope(t, t.name());
    List<Scalar> exprs = new ArrayList<>();
    List<Col> cols = new ArrayList<>();
    for (SelectItem item : items) {
      for (Item i : selectItem(item, scope, Clause.WHERE)) {
        exprs.add(i.expr());
        cols.add(new Col(null, i.name(), i.expr().type()));
      }
    }
    return new Bound.Returning(exprs, cols);
  }

  // -----------------------------------------------------------------------------------------------
  // Queries

  /**
   * Binds a query whose subqueries may refer to {@code parent} (null at the top level). Its plan's
   * columns are the query's output columns.
   */
  private Plan bindQuery(Ast.Query q, Scope parent, Map<String, CteDef> ctes) {
    return textForUnknown(bindQueryKeepingUnknown(q, parent, ctes));
  }

  /** As {@link #bindQuery}, but an untyped literal column stays UNKNOWN (for INSERT). */
  private Plan bindQueryKeepingUnknown(Ast.Query q, Scope parent, Map<String, CteDef> ctes) {
    Map<String, CteDef> saved = ctesInScope;
    try {
      return bindQueryIn(q, parent, ctes);
    } finally {
      ctesInScope = saved;
    }
  }

  /** Output columns still untyped (a bare {@code 'x'}) become text, as in PostgreSQL. */
  private static Plan textForUnknown(Plan p) {
    if (p.columns().stream().noneMatch(c -> c.type() == Type.UNKNOWN)) {
      return p;
    }
    List<Scalar> exprs = new ArrayList<>();
    List<Col> cols = new ArrayList<>();
    for (int k = 0; k < p.columns().size(); k++) {
      Col c = p.columns().get(k);
      Scalar ref = new ColRef(k, c.type());
      if (c.type() == Type.UNKNOWN) {
        exprs.add(new Scalar.Cast(ref, TypeSpec.of(Type.TEXT)));
        cols.add(new Col(c.table(), c.name(), Type.TEXT));
      } else {
        exprs.add(ref);
        cols.add(c);
      }
    }
    return new Plan.Project(p, exprs, List.copyOf(cols));
  }

  private Plan bindQueryIn(Ast.Query q, Scope parent, Map<String, CteDef> ctes) {
    Map<String, CteDef> visible = ctes;
    if (!q.with().isEmpty()) {
      visible = new HashMap<>(ctes);
      for (Ast.Cte cte : q.with()) {
        Plan p = bindQuery(cte.query(), parent, visible);
        List<String> names = new ArrayList<>();
        for (int k = 0; k < p.columns().size(); k++) {
          names.add(k < cte.columns().size() ? cte.columns().get(k) : p.columns().get(k).name());
        }
        if (cte.columns().size() > p.columns().size()) {
          throw new SqlException(
              SqlState.INVALID_COLUMN_REFERENCE,
              "WITH query \""
                  + cte.name()
                  + "\" has "
                  + p.columns().size()
                  + " columns available but "
                  + cte.columns().size()
                  + " columns specified");
        }
        visible.put(cte.name(), new CteDef(p, names));
      }
    }
    ctesInScope = visible;
    if (q.body() instanceof Ast.Select s) {
      return bindSelect(s, q.orderBy(), q.limit(), q.offset(), parent, visible);
    }
    Plan plan = bindBody(q.body(), parent, visible);
    if (!q.orderBy().isEmpty()) {
      List<Plan.SortKey> keys = new ArrayList<>();
      for (Ast.OrderItem o : q.orderBy()) {
        int index = outputColumn(o.expr(), plan.columns(), "ORDER BY");
        if (index < 0) {
          throw new SqlException(
              SqlState.FEATURE_NOT_SUPPORTED,
              "ORDER BY on a UNION/INTERSECT/EXCEPT result must be on result columns");
        }
        keys.add(sortKey(new ColRef(index, plan.columns().get(index).type()), o));
      }
      plan = new Plan.Sort(plan, keys);
    }
    return limit(plan, q.limit(), q.offset());
  }

  private Plan bindBody(Ast.QueryBody body, Scope parent, Map<String, CteDef> ctes) {
    return switch (body) {
      case Ast.Select s -> bindSelect(s, List.of(), null, null, parent, ctes);
      case Ast.Nested n -> bindQuery(n.query(), parent, ctes);
      case Ast.Values v -> bindValues(v, parent);
      case Ast.SetOp op -> bindSetOp(op, parent, ctes);
    };
  }

  private Plan bindValues(Ast.Values v, Scope parent) {
    Scope scope = new Scope(parent, List.of());
    List<List<Scalar>> rows = new ArrayList<>();
    int width = v.rows().get(0).size();
    for (List<Expr> row : v.rows()) {
      if (row.size() != width) {
        throw new SqlException(SqlState.SYNTAX_ERROR, "VALUES lists must all be the same length");
      }
      List<Scalar> out = new ArrayList<>();
      for (Expr e : row) {
        out.add(expr(e, scope, Clause.VALUES));
      }
      rows.add(out);
    }
    List<Col> cols = new ArrayList<>();
    for (int k = 0; k < width; k++) {
      Type t = Type.UNKNOWN;
      for (List<Scalar> r : rows) {
        t = commonType(t, r.get(k).type(), "VALUES");
      }
      if (t == Type.UNKNOWN) {
        t = Type.TEXT;
      }
      for (List<Scalar> r : rows) {
        r.set(k, coerce(r.get(k), t));
      }
      cols.add(new Col(null, "column" + (k + 1), t));
    }
    return new Plan.Values(rows.stream().map(List::copyOf).toList(), cols);
  }

  private Plan bindSetOp(Ast.SetOp op, Scope parent, Map<String, CteDef> ctes) {
    Plan left = bindBody(op.left(), parent, ctes);
    Plan right = bindBody(op.right(), parent, ctes);
    String name = op.kind().name();
    if (left.columns().size() != right.columns().size()) {
      throw new SqlException(
          SqlState.SYNTAX_ERROR, "each " + name + " query must have the same number of columns");
    }
    List<Scalar> le = new ArrayList<>();
    List<Scalar> re = new ArrayList<>();
    List<Col> cols = new ArrayList<>();
    boolean castLeft = false;
    boolean castRight = false;
    for (int k = 0; k < left.columns().size(); k++) {
      Type lt = left.columns().get(k).type();
      Type rt = right.columns().get(k).type();
      Type t = commonType(lt, rt, name);
      if (t == Type.UNKNOWN) {
        t = Type.TEXT;
      }
      castLeft |= lt != t;
      castRight |= rt != t;
      le.add(coerceColumn(new ColRef(k, lt), t));
      re.add(coerceColumn(new ColRef(k, rt), t));
      cols.add(new Col(null, left.columns().get(k).name(), t));
    }
    if (castLeft) {
      left = new Plan.Project(left, le, cols);
    }
    if (castRight) {
      right = new Plan.Project(right, re, cols);
    }
    return new Plan.SetOp(op.kind(), op.all(), left, right, List.copyOf(cols));
  }

  private Scalar coerceColumn(Scalar s, Type t) {
    return s.type() == t ? s : new Scalar.Cast(s, TypeSpec.of(t));
  }

  /** One select-list item after star expansion. */
  private record Item(Scalar expr, String name) {}

  private Plan bindSelect(
      Ast.Select s,
      List<Ast.OrderItem> orderBy,
      Expr limit,
      Expr offset,
      Scope parent,
      Map<String, CteDef> ctes) {
    // FROM
    Plan plan = new Plan.OneRow();
    List<Entry> entries = new ArrayList<>();
    for (Ast.TableRef ref : s.from()) {
      From f = from(ref, parent, ctes);
      if (plan instanceof Plan.OneRow) {
        plan = f.plan();
        entries.addAll(f.entries());
      } else {
        int width = plan.columns().size();
        for (Entry e : f.entries()) {
          entries.add(shift(e, width));
        }
        plan =
            new Plan.Join(
                JoinKind.INNER,
                plan,
                f.plan(),
                new Const(true, Type.BOOL),
                Plan.concat(plan.columns(), f.plan().columns()));
      }
    }
    Scope scope = new Scope(parent, entries);
    List<Col> inputCols = plan.columns();
    // WHERE
    if (s.where() != null) {
      plan = new Plan.Filter(plan, bool(expr(s.where(), scope, Clause.WHERE), "WHERE"));
    }
    // The select list, HAVING and ORDER BY may hold aggregates.
    boolean savedSaw = sawAggregate;
    sawAggregate = false;
    List<Item> items = new ArrayList<>();
    for (SelectItem item : s.items()) {
      items.addAll(selectItem(item, scope, Clause.AGGREGATING));
    }
    Scalar having =
        s.having() == null ? null : bool(expr(s.having(), scope, Clause.AGGREGATING), "HAVING");
    // ORDER BY: an output column (by name or number), or an expression on the input.
    List<Integer> orderTargets = new ArrayList<>();
    List<Scalar> orderExprs = new ArrayList<>();
    for (Ast.OrderItem o : orderBy) {
      int index =
          outputColumn(
              o.expr(),
              items.stream().map(i -> new Col(null, i.name(), i.expr().type())).toList(),
              "ORDER BY");
      if (index >= 0) {
        orderTargets.add(index);
        orderExprs.add(null);
      } else {
        orderTargets.add(-1);
        orderExprs.add(expr(o.expr(), scope, Clause.AGGREGATING));
      }
    }
    // GROUP BY
    List<Scalar> groups = new ArrayList<>();
    for (Expr g : s.groupBy()) {
      groups.add(groupKey(g, items, scope));
    }
    boolean aggregating = !groups.isEmpty() || sawAggregate || having != null;
    sawAggregate = savedSaw;
    if (aggregating) {
      List<AggCall> aggs = new ArrayList<>();
      for (Item i : items) {
        collectAggs(i.expr(), aggs);
      }
      if (having != null) {
        collectAggs(having, aggs);
      }
      for (Scalar e : orderExprs) {
        if (e != null) {
          collectAggs(e, aggs);
        }
      }
      List<Col> aggCols = new ArrayList<>();
      for (int k = 0; k < groups.size(); k++) {
        aggCols.add(new Col(null, "group" + k, groups.get(k).type()));
      }
      for (AggCall a : aggs) {
        aggCols.add(new Col(null, a.function(), a.type()));
      }
      plan = new Plan.Aggregate(plan, groups, aggs, List.copyOf(aggCols));
      List<Item> rewritten = new ArrayList<>();
      for (Item i : items) {
        rewritten.add(new Item(rewrite(i.expr(), groups, aggs, inputCols), i.name()));
      }
      items = rewritten;
      if (having != null) {
        plan = new Plan.Filter(plan, rewrite(having, groups, aggs, inputCols));
      }
      for (int k = 0; k < orderExprs.size(); k++) {
        if (orderExprs.get(k) != null) {
          orderExprs.set(k, rewrite(orderExprs.get(k), groups, aggs, inputCols));
        }
      }
    }
    // Projection, with hidden columns for ORDER BY expressions not in the select list.
    List<Scalar> exprs = new ArrayList<>();
    List<Col> cols = new ArrayList<>();
    for (Item i : items) {
      exprs.add(i.expr());
      cols.add(new Col(null, i.name(), i.expr().type()));
    }
    int visible = exprs.size();
    for (int k = 0; k < orderExprs.size(); k++) {
      Scalar e = orderExprs.get(k);
      if (e == null) {
        continue;
      }
      int found = exprs.indexOf(e);
      if (found >= 0 && found < visible) {
        orderTargets.set(k, found);
      } else {
        if (s.distinct()) {
          throw new SqlException(
              SqlState.INVALID_COLUMN_REFERENCE,
              "for SELECT DISTINCT, ORDER BY expressions must appear in select list");
        }
        orderTargets.set(k, exprs.size());
        exprs.add(e);
        cols.add(new Col(null, "?order?", e.type()));
      }
    }
    plan = new Plan.Project(plan, List.copyOf(exprs), List.copyOf(cols));
    if (s.distinct()) {
      plan = new Plan.Distinct(plan);
    }
    if (!orderBy.isEmpty()) {
      List<Plan.SortKey> keys = new ArrayList<>();
      for (int k = 0; k < orderBy.size(); k++) {
        int idx = orderTargets.get(k);
        keys.add(sortKey(new ColRef(idx, cols.get(idx).type()), orderBy.get(k)));
      }
      plan = new Plan.Sort(plan, keys);
    }
    if (exprs.size() > visible) {
      List<Scalar> trim = new ArrayList<>();
      for (int k = 0; k < visible; k++) {
        trim.add(new ColRef(k, cols.get(k).type()));
      }
      plan = new Plan.Project(plan, trim, List.copyOf(cols.subList(0, visible)));
    }
    return limit(plan, limit, offset);
  }

  private static Plan.SortKey sortKey(Scalar e, Ast.OrderItem o) {
    // PostgreSQL: NULLs sort as larger than every value, so last ascending and first descending.
    boolean nullsFirst = o.nullsFirst() != null ? o.nullsFirst() : o.desc();
    return new Plan.SortKey(e, o.desc(), nullsFirst);
  }

  /**
   * The output column an ORDER BY or GROUP BY item names, by number or by (unqualified) name; -1
   * when it is an expression instead.
   */
  private static int outputColumn(Expr e, List<Col> outputs, String clause) {
    if (e instanceof Ast.Literal l && l.value() instanceof Long n) {
      if (n < 1 || n > outputs.size()) {
        throw new SqlException(
            SqlState.INVALID_COLUMN_REFERENCE,
            clause + " position " + n + " is not in select list");
      }
      return (int) (n - 1);
    }
    if (e instanceof Ast.ColumnRef c && c.qualifier() == null) {
      int found = -1;
      for (int k = 0; k < outputs.size(); k++) {
        if (outputs.get(k).name().equals(c.name())) {
          if (found >= 0) {
            throw new SqlException(
                SqlState.AMBIGUOUS_COLUMN, clause + " \"" + c.name() + "\" is ambiguous");
          }
          found = k;
        }
      }
      return found;
    }
    return -1;
  }

  /** A GROUP BY item: an input column or expression, else a select-list number or alias. */
  private Scalar groupKey(Expr g, List<Item> items, Scope scope) {
    if (g instanceof Ast.Literal l && l.value() instanceof Long n) {
      if (n < 1 || n > items.size()) {
        throw new SqlException(
            SqlState.INVALID_COLUMN_REFERENCE, "GROUP BY position " + n + " is not in select list");
      }
      Scalar e = items.get((int) (n - 1)).expr();
      if (containsAgg(e)) {
        throw new SqlException(
            SqlState.GROUPING_ERROR, "aggregate functions are not allowed in GROUP BY");
      }
      return e;
    }
    if (g instanceof Ast.ColumnRef c && c.qualifier() == null && !inScope(scope, c.name())) {
      for (Item i : items) {
        if (i.name().equals(c.name())) {
          return i.expr();
        }
      }
    }
    return expr(g, scope, Clause.GROUP_BY);
  }

  private static boolean inScope(Scope scope, String name) {
    for (Entry e : scope.entries) {
      if (e.name().equals(name) && !e.unqualifiedHidden()) {
        return true;
      }
    }
    return false;
  }

  private Plan limit(Plan plan, Expr limit, Expr offset) {
    if (limit == null && offset == null) {
      return plan;
    }
    Scope empty = new Scope(null, List.of());
    Scalar l = limit == null ? null : intExpr(limit, empty, "LIMIT");
    Scalar o = offset == null ? null : intExpr(offset, empty, "OFFSET");
    return new Plan.Limit(plan, l, o);
  }

  private Scalar intExpr(Expr e, Scope scope, String clause) {
    Scalar s = coerce(expr(e, scope, Clause.LIMIT), Type.INT);
    if (s.type() != Type.INT) {
      s = new Scalar.Cast(s, TypeSpec.of(Type.INT));
    }
    return s;
  }

  // -----------------------------------------------------------------------------------------------
  // FROM

  private record From(Plan plan, List<Entry> entries) {}

  private From from(Ast.TableRef ref, Scope parent, Map<String, CteDef> ctes) {
    return switch (ref) {
      case Ast.TableName tn -> {
        CteDef cte = ctes.get(tn.name());
        String alias = tn.alias() == null ? tn.name() : tn.alias();
        if (cte != null) {
          List<Entry> entries = new ArrayList<>();
          for (int k = 0; k < cte.names().size(); k++) {
            entries.add(
                new Entry(
                    alias,
                    cte.names().get(k),
                    new ColRef(k, cte.plan().columns().get(k).type()),
                    false,
                    false));
          }
          yield new From(cte.plan(), entries);
        }
        TableDesc t = catalog.table(tn.name());
        Scope s = tableScope(t, alias);
        yield new From(new Plan.Scan(t, alias, tableCols(t, alias)), s.entries);
      }
      case Ast.Derived d -> {
        Plan p = bindQuery(d.query(), parent, ctes);
        if (d.columns().size() > p.columns().size()) {
          throw new SqlException(
              SqlState.INVALID_COLUMN_REFERENCE,
              "table \""
                  + d.alias()
                  + "\" has "
                  + p.columns().size()
                  + " columns available but "
                  + d.columns().size()
                  + " columns specified");
        }
        List<Entry> entries = new ArrayList<>();
        for (int k = 0; k < p.columns().size(); k++) {
          String name = k < d.columns().size() ? d.columns().get(k) : p.columns().get(k).name();
          entries.add(
              new Entry(d.alias(), name, new ColRef(k, p.columns().get(k).type()), false, false));
        }
        yield new From(p, entries);
      }
      case Ast.Join j -> join(j, parent, ctes);
    };
  }

  private From join(Ast.Join j, Scope parent, Map<String, CteDef> ctes) {
    From left = from(j.left(), parent, ctes);
    From right = from(j.right(), parent, ctes);
    int width = left.plan().columns().size();
    List<Entry> rightShifted = new ArrayList<>();
    for (Entry e : right.entries()) {
      rightShifted.add(shift(e, width));
    }
    List<Col> cols = Plan.concat(left.plan().columns(), right.plan().columns());
    JoinKind kind = j.kind() == JoinKind.CROSS ? JoinKind.INNER : j.kind();
    List<Entry> both = new ArrayList<>(left.entries());
    both.addAll(rightShifted);
    if (!j.using().isEmpty()) {
      List<Entry> merged = new ArrayList<>();
      List<Entry> rest = new ArrayList<>();
      Scalar on = null;
      List<Entry> leftOut = new ArrayList<>(left.entries());
      List<Entry> rightOut = new ArrayList<>(rightShifted);
      for (String name : j.using()) {
        int li = unique(leftOut, name, "left");
        int ri = unique(rightOut, name, "right");
        Entry l = leftOut.get(li);
        Entry r = rightOut.get(ri);
        Type t = commonType(l.expr().type(), r.expr().type(), "JOIN/USING");
        Scalar le = coerce(l.expr(), t);
        Scalar re = coerce(r.expr(), t);
        Scalar eq = new Scalar.Compare(Scalar.CmpOp.EQ, le, re);
        on = on == null ? eq : new Scalar.And(on, eq);
        Scalar value =
            switch (kind) {
              case RIGHT -> re;
              case FULL -> new Scalar.Call("coalesce", List.of(le, re), t);
              default -> le;
            };
        merged.add(new Entry(null, name, value, false, false));
        leftOut.set(li, new Entry(l.qualifier(), l.name(), l.expr(), true, true));
        rightOut.set(ri, new Entry(r.qualifier(), r.name(), r.expr(), true, true));
      }
      rest.addAll(merged);
      rest.addAll(leftOut);
      rest.addAll(rightOut);
      return new From(new Plan.Join(kind, left.plan(), right.plan(), on, cols), rest);
    }
    Scalar on;
    if (j.on() == null) {
      on = new Const(true, Type.BOOL);
    } else {
      Scope scope = new Scope(parent, both);
      on = bool(expr(j.on(), scope, Clause.WHERE), "JOIN/ON");
    }
    return new From(new Plan.Join(kind, left.plan(), right.plan(), on, cols), both);
  }

  private static int unique(List<Entry> entries, String name, String side) {
    int found = -1;
    for (int k = 0; k < entries.size(); k++) {
      Entry e = entries.get(k);
      if (e.name().equals(name) && !e.unqualifiedHidden()) {
        if (found >= 0) {
          throw new SqlException(
              SqlState.AMBIGUOUS_COLUMN,
              "common column name \"" + name + "\" appears more than once in " + side + " table");
        }
        found = k;
      }
    }
    if (found < 0) {
      throw new SqlException(
          SqlState.UNDEFINED_COLUMN,
          "column \"" + name + "\" specified in USING clause does not exist in " + side + " table");
    }
    return found;
  }

  private static Entry shift(Entry e, int by) {
    return new Entry(
        e.qualifier(), e.name(), shift(e.expr(), by), e.starHidden(), e.unqualifiedHidden());
  }

  /** {@code s} with its column references moved {@code by} positions right. */
  static Scalar shift(Scalar s, int by) {
    return Scalars.map(s, x -> x instanceof ColRef c ? new ColRef(c.index() + by, c.type()) : null);
  }

  private Scope tableScope(TableDesc t, String alias) {
    List<Entry> entries = new ArrayList<>();
    for (int p = 0; p < t.columns().size(); p++) {
      TableDesc.Column c = t.columns().get(p);
      entries.add(new Entry(alias, c.name(), new ColRef(p, c.type().type()), c.hidden(), false));
    }
    return new Scope(null, entries);
  }

  private static List<Col> tableCols(TableDesc t, String alias) {
    List<Col> cols = new ArrayList<>();
    for (TableDesc.Column c : t.columns()) {
      cols.add(new Col(alias, c.name(), c.type().type()));
    }
    return List.copyOf(cols);
  }

  private List<Item> selectItem(SelectItem item, Scope scope, Clause clause) {
    return switch (item) {
      case Ast.Star star -> {
        List<Item> out = new ArrayList<>();
        boolean matched = false;
        for (Entry e : scope.entries) {
          if (star.qualifier() != null) {
            if (star.qualifier().equals(e.qualifier())) {
              matched = true;
              // t.* includes t's USING columns, but not a hidden rowid.
              boolean rowid = e.starHidden() && !e.unqualifiedHidden();
              if (!rowid) {
                out.add(new Item(e.expr(), e.name()));
              }
            }
          } else if (!e.starHidden()) {
            out.add(new Item(e.expr(), e.name()));
          }
        }
        if (star.qualifier() != null && !matched) {
          throw new SqlException(
              SqlState.UNDEFINED_TABLE,
              "missing FROM-clause entry for table \"" + star.qualifier() + "\"");
        }
        if (star.qualifier() == null && scope.entries.isEmpty()) {
          throw new SqlException(
              SqlState.SYNTAX_ERROR, "SELECT * with no tables specified is not valid");
        }
        yield out;
      }
      case Ast.ExprItem ei -> {
        // An untyped literal stays UNKNOWN here: a set operation may still type it.
        Scalar e = expr(ei.expr(), scope, clause);
        yield List.of(new Item(e, ei.alias() != null ? ei.alias() : nameOf(ei.expr())));
      }
    };
  }

  /** The column name PostgreSQL gives an unaliased select-list item. */
  private static String nameOf(Expr e) {
    return switch (e) {
      case Ast.ColumnRef c -> c.name();
      case Ast.Func f -> f.name();
      case Ast.Cast c -> {
        String inner = nameOf(c.operand());
        yield inner.equals("?column?") ? c.type().type().sqlName().split(" ")[0] : inner;
      }
      case Ast.Case c -> "case";
      case Ast.Exists x -> "exists";
      case Ast.ScalarSubquery sq -> "?column?";
      default -> "?column?";
    };
  }

  // -----------------------------------------------------------------------------------------------
  // Expressions

  private Scalar expr(Expr e, Scope scope, Clause clause) {
    return switch (e) {
      case Ast.Literal l -> new Const(l.value(), l.type());
      case Ast.ColumnRef c -> resolve(c, scope);
      case Ast.Param p -> param(p.index());
      case Ast.Default d ->
          throw new SqlException(SqlState.SYNTAX_ERROR, "DEFAULT is not allowed in this context");
      case Ast.Unary u -> unary(u, scope, clause);
      case Ast.Binary b ->
          binary(b.op(), expr(b.left(), scope, clause), expr(b.right(), scope, clause));
      case Ast.IsNull n -> new Scalar.IsNull(expr(n.operand(), scope, clause), n.not());
      case Ast.IsBool b ->
          new Scalar.IsBool(bool(expr(b.operand(), scope, clause), "IS"), b.value(), b.not());
      case Ast.IsDistinct d -> {
        Scalar l = expr(d.left(), scope, clause);
        Scalar r = expr(d.right(), scope, clause);
        Type t = commonType(l.type(), r.type(), "IS DISTINCT FROM");
        if (t == Type.UNKNOWN) {
          t = Type.TEXT;
        }
        yield new Scalar.IsDistinct(coerce(l, t), coerce(r, t), d.not());
      }
      case Ast.Between b -> {
        Scalar x = expr(b.operand(), scope, clause);
        Scalar lo = expr(b.low(), scope, clause);
        Scalar hi = expr(b.high(), scope, clause);
        Scalar ge = binary(">=", x, lo);
        Scalar le = binary("<=", x, hi);
        Scalar both = new Scalar.And(ge, le);
        yield b.not() ? new Scalar.Not(both) : both;
      }
      case Ast.InList in -> {
        Scalar x = expr(in.operand(), scope, clause);
        List<Scalar> list = new ArrayList<>();
        Type t = x.type();
        for (Expr item : in.list()) {
          Scalar s = expr(item, scope, clause);
          list.add(s);
          t = commonType(t, s.type(), "IN");
        }
        if (t == Type.UNKNOWN) {
          t = Type.TEXT;
        }
        List<Scalar> coerced = new ArrayList<>();
        for (Scalar s : list) {
          coerced.add(coerce(s, t));
        }
        yield new Scalar.InList(coerce(x, t), coerced, in.not());
      }
      case Ast.InSubquery in -> {
        Scalar x = expr(in.operand(), scope, clause);
        Sub sub = subquery(in.query(), scope);
        if (sub.plan().columns().size() != 1) {
          throw new SqlException(SqlState.SYNTAX_ERROR, "subquery has too many columns");
        }
        Type st = sub.plan().columns().get(0).type();
        Type t = commonType(x.type(), st, "IN");
        if (t == Type.UNKNOWN) {
          t = Type.TEXT;
        }
        Plan p = sub.plan();
        if (st != t) {
          p =
              new Plan.Project(
                  p,
                  List.of(coerceColumn(new ColRef(0, st), t)),
                  List.of(new Col(null, p.columns().get(0).name(), t)));
        }
        yield new Scalar.Subquery(
            Scalar.SubqueryKind.IN, p, coerce(x, t), in.not(), sub.correlated(), Type.BOOL);
      }
      case Ast.Exists x -> {
        Sub sub = subquery(x.query(), scope);
        yield new Scalar.Subquery(
            Scalar.SubqueryKind.EXISTS, sub.plan(), null, false, sub.correlated(), Type.BOOL);
      }
      case Ast.ScalarSubquery sq -> {
        Sub sub = subquery(sq.query(), scope);
        if (sub.plan().columns().size() != 1) {
          throw new SqlException(SqlState.SYNTAX_ERROR, "subquery must return only one column");
        }
        Type t = sub.plan().columns().get(0).type();
        yield new Scalar.Subquery(
            Scalar.SubqueryKind.SCALAR, sub.plan(), null, false, sub.correlated(), t);
      }
      case Ast.Case c -> caseExpr(c, scope, clause);
      case Ast.Cast c -> {
        Scalar x = expr(c.operand(), scope, clause);
        if (x instanceof Scalar.ParamRef p && p.type() == Type.UNKNOWN) {
          yield coerce(x, c.type().type());
        }
        if (x instanceof Const k && k.value() != null && !c.type().hasModifier()) {
          yield new Const(Values.cast(k.value(), k.type(), c.type()), c.type().type());
        }
        if (x instanceof Const k && k.value() == null) {
          yield new Const(null, c.type().type());
        }
        if (x.type() == c.type().type() && !c.type().hasModifier()) {
          yield x;
        }
        yield new Scalar.Cast(x, c.type());
      }
      case Ast.Func f -> function(f, scope, clause);
      case Ast.Like l -> {
        Scalar x = coerce(expr(l.operand(), scope, clause), Type.TEXT);
        Scalar pattern = coerce(expr(l.pattern(), scope, clause), Type.TEXT);
        Scalar escape =
            l.escape() == null ? null : coerce(expr(l.escape(), scope, clause), Type.TEXT);
        if (x.type() != Type.TEXT || pattern.type() != Type.TEXT) {
          throw new SqlException(
              SqlState.UNDEFINED_FUNCTION,
              "operator does not exist: " + x.type().sqlName() + " ~~ " + pattern.type().sqlName());
        }
        yield new Scalar.Like(x, pattern, escape, l.not(), l.caseInsensitive());
      }
    };
  }

  private Scalar unary(Ast.Unary u, Scope scope, Clause clause) {
    Scalar x = expr(u.operand(), scope, clause);
    return switch (u.op()) {
      case "NOT" -> new Scalar.Not(bool(x, "NOT"));
      case "+" -> {
        Scalar n = x.type() == Type.UNKNOWN ? coerce(x, Type.NUMERIC) : x;
        if (!n.type().isNumeric()) {
          throw opError("+", null, n.type());
        }
        yield n;
      }
      default -> {
        Scalar n = x.type() == Type.UNKNOWN ? coerce(x, Type.NUMERIC) : x;
        if (!n.type().isNumeric()) {
          throw opError("-", null, n.type());
        }
        if (n instanceof Const k && k.value() != null) {
          yield new Const(Values.negate(k.value()), k.type());
        }
        yield new Scalar.Neg(n, n.type());
      }
    };
  }

  private Scalar binary(String op, Scalar l, Scalar r) {
    switch (op) {
      case "AND" -> {
        return new Scalar.And(bool(l, "AND"), bool(r, "AND"));
      }
      case "OR" -> {
        return new Scalar.Or(bool(l, "OR"), bool(r, "OR"));
      }
      case "||" -> {
        if (l.type() == Type.TEXT
            || r.type() == Type.TEXT
            || l.type() == Type.UNKNOWN
            || r.type() == Type.UNKNOWN) {
          return new Scalar.Concat(toText(l), toText(r));
        }
        throw opError("||", l.type(), r.type());
      }
      case "=", "<>", "<", "<=", ">", ">=" -> {
        Type t = comparableType(l.type(), r.type(), op);
        Scalar.CmpOp cmp =
            switch (op) {
              case "=" -> Scalar.CmpOp.EQ;
              case "<>" -> Scalar.CmpOp.NE;
              case "<" -> Scalar.CmpOp.LT;
              case "<=" -> Scalar.CmpOp.LE;
              case ">" -> Scalar.CmpOp.GT;
              default -> Scalar.CmpOp.GE;
            };
        return new Scalar.Compare(cmp, coerce(l, t), coerce(r, t));
      }
      default -> {
        // + - * / % ^
        Type lt = l.type();
        Type rt = r.type();
        if (lt == Type.UNKNOWN && rt == Type.UNKNOWN) {
          lt = Type.NUMERIC;
          rt = Type.NUMERIC;
        }
        Type t;
        if (op.equals("^")) {
          t = Type.FLOAT;
          if ((lt != Type.UNKNOWN && !lt.isNumeric()) || (rt != Type.UNKNOWN && !rt.isNumeric())) {
            throw opError(op, l.type(), r.type());
          }
        } else {
          t = commonType(lt, rt, op);
          if (!t.isNumeric()) {
            throw opError(op, l.type(), r.type());
          }
        }
        Scalar.ArithOp a =
            switch (op) {
              case "+" -> Scalar.ArithOp.ADD;
              case "-" -> Scalar.ArithOp.SUB;
              case "*" -> Scalar.ArithOp.MUL;
              case "/" -> Scalar.ArithOp.DIV;
              case "%" -> Scalar.ArithOp.MOD;
              default -> Scalar.ArithOp.POW;
            };
        return new Scalar.Arith(a, coerce(l, t), coerce(r, t), t);
      }
    }
  }

  private Scalar toText(Scalar s) {
    if (s.type() == Type.TEXT) {
      return s;
    }
    if (s.type() == Type.UNKNOWN) {
      return coerce(s, Type.TEXT);
    }
    return new Scalar.Cast(s, TypeSpec.of(Type.TEXT));
  }

  /** The type two operands of a comparison are compared in. */
  private Type comparableType(Type a, Type b, String op) {
    if (a == Type.UNKNOWN && b == Type.UNKNOWN) {
      return Type.TEXT;
    }
    if (a == Type.UNKNOWN) {
      return b;
    }
    if (b == Type.UNKNOWN) {
      return a;
    }
    if (a == b) {
      return a;
    }
    if (a.isNumeric() && b.isNumeric()) {
      return rank(a) > rank(b) ? a : b;
    }
    throw opError(op, a, b);
  }

  private Scalar caseExpr(Ast.Case c, Scope scope, Clause clause) {
    List<Scalar> conditions = new ArrayList<>();
    List<Scalar> results = new ArrayList<>();
    Scalar operand = c.operand() == null ? null : expr(c.operand(), scope, clause);
    Type t = Type.UNKNOWN;
    for (Ast.When w : c.whens()) {
      Scalar cond = expr(w.condition(), scope, clause);
      conditions.add(operand == null ? bool(cond, "CASE/WHEN") : binary("=", operand, cond));
      Scalar res = expr(w.result(), scope, clause);
      results.add(res);
      t = commonType(t, res.type(), "CASE");
    }
    Scalar otherwise = c.otherwise() == null ? null : expr(c.otherwise(), scope, clause);
    if (otherwise != null) {
      t = commonType(t, otherwise.type(), "CASE");
    }
    if (t == Type.UNKNOWN) {
      t = Type.TEXT;
    }
    List<Scalar> coerced = new ArrayList<>();
    for (Scalar r : results) {
      coerced.add(coerce(r, t));
    }
    return new Scalar.Case(
        conditions, coerced, otherwise == null ? new Const(null, t) : coerce(otherwise, t), t);
  }

  private Scalar function(Ast.Func f, Scope scope, Clause clause) {
    if (Functions.isAggregate(f.name())) {
      switch (clause) {
        case WHERE ->
            throw new SqlException(
                SqlState.GROUPING_ERROR, "aggregate functions are not allowed in WHERE");
        case GROUP_BY ->
            throw new SqlException(
                SqlState.GROUPING_ERROR, "aggregate functions are not allowed in GROUP BY");
        case IN_AGGREGATE ->
            throw new SqlException(
                SqlState.GROUPING_ERROR, "aggregate function calls cannot be nested");
        case PLAIN, VALUES, LIMIT ->
            throw new SqlException(
                SqlState.GROUPING_ERROR, "aggregate functions are not allowed here");
        case AGGREGATING -> {
          // allowed
        }
      }
      sawAggregate = true;
      if (f.star()) {
        if (!f.name().equals("count")) {
          throw new SqlException(SqlState.SYNTAX_ERROR, f.name() + "(*) is not valid");
        }
        return Functions.aggregate("count", null, false, this);
      }
      if (f.args().size() != 1) {
        throw Functions.undefined(f.name(), List.of());
      }
      Scalar arg = expr(f.args().get(0), scope, Clause.IN_AGGREGATE);
      return Functions.aggregate(f.name(), arg, f.distinct(), this);
    }
    if (f.star() || f.distinct()) {
      throw new SqlException(
          SqlState.WRONG_OBJECT_TYPE,
          (f.star() ? "*" : "DISTINCT")
              + " specified, but "
              + f.name()
              + " is not an aggregate function");
    }
    List<Scalar> args = new ArrayList<>();
    for (Expr a : f.args()) {
      args.add(expr(a, scope, clause));
    }
    return Functions.call(f.name(), args, this);
  }

  /** A subquery bound with {@code scope} as its enclosing scope. */
  private record Sub(Plan plan, boolean correlated) {}

  private Sub subquery(Ast.Query q, Scope scope) {
    boolean savedSaw = sawAggregate;
    Plan p = bindQuery(q, scope, ctesInScope);
    sawAggregate = savedSaw;
    return new Sub(p, Plans.correlated(p));
  }

  private Scalar resolve(Ast.ColumnRef c, Scope scope) {
    int depth = 0;
    boolean qualifierSeen = false;
    for (Scope s = scope; s != null; s = s.parent, depth++) {
      Entry found = null;
      for (Entry e : s.entries) {
        if (!e.name().equals(c.name())) {
          if (c.qualifier() != null && c.qualifier().equals(e.qualifier())) {
            qualifierSeen = true;
          }
          continue;
        }
        if (c.qualifier() == null ? e.unqualifiedHidden() : !c.qualifier().equals(e.qualifier())) {
          continue;
        }
        if (c.qualifier() != null) {
          qualifierSeen = true;
        }
        if (found != null) {
          throw new SqlException(
              SqlState.AMBIGUOUS_COLUMN, "column reference \"" + c.name() + "\" is ambiguous");
        }
        found = e;
      }
      if (found != null) {
        if (depth == 0) {
          return found.expr();
        }
        return new Scalar.OuterRef(depth, found.expr());
      }
    }
    if (c.qualifier() != null && !qualifierSeen) {
      throw new SqlException(
          SqlState.UNDEFINED_TABLE,
          "missing FROM-clause entry for table \"" + c.qualifier() + "\"");
    }
    String name = c.qualifier() == null ? c.name() : c.qualifier() + "." + c.name();
    throw new SqlException(SqlState.UNDEFINED_COLUMN, "column \"" + name + "\" does not exist");
  }

  private Scalar param(int index) {
    while (params.size() < index) {
      params.add(Type.UNKNOWN);
    }
    return new Scalar.ParamRef(index - 1, params.get(index - 1));
  }

  // -----------------------------------------------------------------------------------------------
  // Aggregation

  private static void collectAggs(Scalar s, List<AggCall> out) {
    Scalars.visit(
        s,
        x -> {
          if (x instanceof AggCall a && !out.contains(a)) {
            out.add(a);
          }
        });
  }

  private static boolean containsAgg(Scalar s) {
    boolean[] found = {false};
    Scalars.visit(s, x -> found[0] |= x instanceof AggCall);
    return found[0];
  }

  /** {@code s} over the aggregation's output: groups first, then aggregates. */
  private static Scalar rewrite(Scalar s, List<Scalar> groups, List<AggCall> aggs, List<Col> in) {
    int g = groups.indexOf(s);
    if (g >= 0) {
      return new ColRef(g, s.type());
    }
    return switch (s) {
      case AggCall a -> new ColRef(groups.size() + aggs.indexOf(a), a.type());
      case ColRef c ->
          throw new SqlException(
              SqlState.GROUPING_ERROR,
              "column \""
                  + qualified(in.get(c.index()))
                  + "\" must appear in the GROUP BY clause or be used in an aggregate function");
      case Const k -> k;
      case Scalar.ParamRef p -> p;
      case Scalar.OuterRef o -> o;
      case Scalar.Subquery q -> {
        if (q.correlated()) {
          throw new SqlException(
              SqlState.FEATURE_NOT_SUPPORTED,
              "correlated subqueries in an aggregating select list are not supported");
        }
        yield q.operand() == null
            ? q
            : new Scalar.Subquery(
                q.kind(),
                q.plan(),
                rewrite(q.operand(), groups, aggs, in),
                q.not(),
                false,
                q.type());
      }
      default -> Scalars.mapChildren(s, x -> rewrite(x, groups, aggs, in));
    };
  }

  private static String qualified(Col c) {
    return c.table() == null ? c.name() : c.table() + "." + c.name();
  }

  // -----------------------------------------------------------------------------------------------
  // Types

  /** {@code s} as type {@code t}: untyped literals and parameters take it, others get a cast. */
  Scalar coerce(Scalar s, Type t) {
    if (s.type() == t || t == Type.UNKNOWN) {
      return s;
    }
    if (s instanceof Const k) {
      if (k.value() == null) {
        return new Const(null, t);
      }
      return new Const(Values.cast(k.value(), k.type(), TypeSpec.of(t)), t);
    }
    if (s instanceof Scalar.ParamRef p && p.type() == Type.UNKNOWN) {
      params.set(p.index(), t);
      return new Scalar.ParamRef(p.index(), t);
    }
    return new Scalar.Cast(s, TypeSpec.of(t));
  }

  private Scalar bool(Scalar s, String context) {
    Scalar b = coerce(s, s.type() == Type.UNKNOWN ? Type.BOOL : s.type());
    if (b.type() != Type.BOOL) {
      throw new SqlException(
          SqlState.DATATYPE_MISMATCH,
          "argument of " + context + " must be type boolean, not type " + b.type().sqlName());
    }
    return b;
  }

  /** The type values of types {@code a} and {@code b} unify to, as for CASE, UNION, VALUES. */
  Type commonType(Type a, Type b, String context) {
    if (a == b) {
      return a;
    }
    if (a == Type.UNKNOWN) {
      return b;
    }
    if (b == Type.UNKNOWN) {
      return a;
    }
    if (a.isNumeric() && b.isNumeric()) {
      return rank(a) > rank(b) ? a : b;
    }
    if (context.length() <= 2 || context.equals("mod")) {
      throw opError(context, a, b);
    }
    throw new SqlException(
        SqlState.DATATYPE_MISMATCH,
        context + " types " + a.sqlName() + " and " + b.sqlName() + " cannot be matched");
  }

  Type commonType(Scalar a, Scalar b, String context) {
    return commonType(a.type(), b.type(), context);
  }

  private static int rank(Type t) {
    return switch (t) {
      case INT -> 1;
      case NUMERIC -> 2;
      case FLOAT -> 3;
      default -> 0;
    };
  }

  private static SqlException opError(String op, Type l, Type r) {
    return new SqlException(
        SqlState.UNDEFINED_FUNCTION,
        "operator does not exist: "
            + (l == null ? "" : l.sqlName() + " ")
            + op
            + " "
            + r.sqlName());
  }
}

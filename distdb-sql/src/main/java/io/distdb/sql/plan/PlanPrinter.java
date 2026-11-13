package io.distdb.sql.plan;

import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.plan.Plan.Col;
import io.distdb.sql.types.Values;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/** Renders a physical plan for {@code EXPLAIN}, one operator per line, children indented. */
public final class PlanPrinter {

  private PlanPrinter() {}

  public static List<String> render(Plan p) {
    List<String> out = new ArrayList<>();
    render(p, 0, out);
    return out;
  }

  private static void render(Plan p, int depth, List<String> out) {
    String pad = "  ".repeat(depth);
    out.add(pad + (depth == 0 ? "" : "-> ") + describe(p));
    for (Scalar s : Plans.scalars(p)) {
      Scalars.visit(
          s,
          x -> {
            if (x instanceof Scalar.Subquery q) {
              out.add(
                  pad
                      + "   "
                      + (q.correlated() ? "correlated " : "")
                      + "subquery ("
                      + q.kind().name().toLowerCase(java.util.Locale.ROOT)
                      + "):");
              render(q.plan(), depth + 2, out);
            }
          });
    }
    for (Plan c : Plans.children(p)) {
      render(c, depth + 1, out);
    }
  }

  private static String describe(Plan p) {
    return switch (p) {
      case Plan.Scan s -> "scan " + name(s.table(), s.alias());
      case Plan.FullScan s -> "full scan " + name(s.table(), s.alias());
      case Plan.PointGet g ->
          "point get "
              + name(g.table(), g.alias())
              + " on "
              + g.table().primary().name()
              + " ("
              + exprs(g.key(), g.columns())
              + ")";
      case Plan.IndexScan s -> {
        StringBuilder b =
            new StringBuilder(
                "index scan " + name(s.table(), s.alias()) + " on " + s.index().name());
        if (!s.prefix().isEmpty()) {
          b.append(" = (").append(exprs(s.prefix(), s.columns())).append(")");
        }
        if (s.low() != null) {
          b.append(s.low().inclusive() ? " from " : " after ")
              .append(expr(s.low().value(), s.columns()));
        }
        if (s.high() != null) {
          b.append(s.high().inclusive() ? " through " : " before ")
              .append(expr(s.high().value(), s.columns()));
        }
        yield b.toString();
      }
      case Plan.Join j -> j.kind().name().toLowerCase(java.util.Locale.ROOT) + " join";
      case Plan.NestedLoopJoin j ->
          "nested loop " + kind(j.kind()) + "join on " + expr(j.on(), j.columns());
      case Plan.HashJoin j -> {
        List<String> keys = new ArrayList<>();
        int width = j.left().columns().size();
        for (int k = 0; k < j.leftKeys().size(); k++) {
          keys.add(
              expr(j.leftKeys().get(k), j.columns())
                  + " = "
                  + expr(Binder.shift(j.rightKeys().get(k), width), j.columns()));
        }
        yield "hash "
            + kind(j.kind())
            + "join on "
            + String.join(" AND ", keys)
            + (j.residual() == null ? "" : " filter " + expr(j.residual(), j.columns()));
      }
      case Plan.LookupJoin j ->
          "lookup "
              + kind(j.kind())
              + "join "
              + name(j.table(), j.alias())
              + " on "
              + j.index().name()
              + " = ("
              + exprs(j.keys(), j.left().columns())
              + ")";
      case Plan.Filter f -> "filter " + expr(f.predicate(), f.input().columns());
      case Plan.Project pr -> "project " + exprs(pr.exprs(), pr.input().columns());
      case Plan.Aggregate a -> {
        StringJoiner aggs = new StringJoiner(", ");
        for (Scalar.AggCall c : a.aggs()) {
          aggs.add(
              c.function()
                  + "("
                  + (c.distinct() ? "DISTINCT " : "")
                  + (c.arg() == null ? "*" : expr(c.arg(), a.input().columns()))
                  + ")");
        }
        yield "hash aggregate"
            + (a.groups().isEmpty() ? "" : " by " + exprs(a.groups(), a.input().columns()))
            + (a.aggs().isEmpty() ? "" : ": " + aggs);
      }
      case Plan.Sort s -> {
        StringJoiner keys = new StringJoiner(", ");
        for (Plan.SortKey k : s.keys()) {
          keys.add(expr(k.expr(), s.columns()) + (k.desc() ? " DESC" : ""));
        }
        yield "sort by " + keys;
      }
      case Plan.Limit l ->
          "limit "
              + (l.limit() == null ? "all" : expr(l.limit(), List.of()))
              + (l.offset() == null ? "" : " offset " + expr(l.offset(), List.of()));
      case Plan.Distinct d -> "distinct";
      case Plan.Values v -> "values (" + v.rows().size() + " rows)";
      case Plan.SetOp s ->
          s.kind().name().toLowerCase(java.util.Locale.ROOT) + (s.all() ? " all" : "");
      case Plan.OneRow o -> "one row";
    };
  }

  private static String kind(io.distdb.sql.parse.Ast.JoinKind k) {
    return k == io.distdb.sql.parse.Ast.JoinKind.INNER
        ? ""
        : k.name().toLowerCase(java.util.Locale.ROOT) + " ";
  }

  private static String name(TableDesc t, String alias) {
    return alias == null || alias.equals(t.name()) ? t.name() : t.name() + " " + alias;
  }

  private static String exprs(List<Scalar> list, List<Col> cols) {
    StringJoiner j = new StringJoiner(", ");
    for (Scalar s : list) {
      j.add(expr(s, cols));
    }
    return j.toString();
  }

  /** An expression in SQL-like form, column references by name. */
  static String expr(Scalar s, List<Col> cols) {
    return switch (s) {
      case Scalar.Const c ->
          c.value() == null
              ? "NULL"
              : (c.value() instanceof String str
                  ? "'" + str.replace("'", "''") + "'"
                  : Values.toText(c.value(), c.type()));
      case Scalar.ColRef c -> {
        if (c.index() < cols.size()) {
          Col col = cols.get(c.index());
          yield col.table() == null ? col.name() : col.table() + "." + col.name();
        }
        yield "#" + c.index();
      }
      case Scalar.OuterRef o -> "outer(" + o.depth() + ")." + expr(o.inner(), List.of());
      case Scalar.ParamRef p -> "$" + (p.index() + 1);
      case Scalar.Arith a ->
          "("
              + expr(a.left(), cols)
              + " "
              + switch (a.op()) {
                case ADD -> "+";
                case SUB -> "-";
                case MUL -> "*";
                case DIV -> "/";
                case MOD -> "%";
                case POW -> "^";
              }
              + " "
              + expr(a.right(), cols)
              + ")";
      case Scalar.Neg n -> "-" + expr(n.operand(), cols);
      case Scalar.Compare c ->
          expr(c.left(), cols)
              + " "
              + switch (c.op()) {
                case EQ -> "=";
                case NE -> "<>";
                case LT -> "<";
                case LE -> "<=";
                case GT -> ">";
                case GE -> ">=";
              }
              + " "
              + expr(c.right(), cols);
      case Scalar.Concat c -> expr(c.left(), cols) + " || " + expr(c.right(), cols);
      case Scalar.And a -> "(" + expr(a.left(), cols) + " AND " + expr(a.right(), cols) + ")";
      case Scalar.Or o -> "(" + expr(o.left(), cols) + " OR " + expr(o.right(), cols) + ")";
      case Scalar.Not n -> "NOT " + expr(n.operand(), cols);
      case Scalar.IsNull n -> expr(n.operand(), cols) + (n.not() ? " IS NOT NULL" : " IS NULL");
      case Scalar.IsBool b ->
          expr(b.operand(), cols)
              + " IS "
              + (b.not() ? "NOT " : "")
              + (b.value() ? "TRUE" : "FALSE");
      case Scalar.IsDistinct d ->
          expr(d.left(), cols)
              + " IS "
              + (d.not() ? "NOT " : "")
              + "DISTINCT FROM "
              + expr(d.right(), cols);
      case Scalar.Case c -> "CASE ... END";
      case Scalar.Cast c -> expr(c.operand(), cols) + "::" + c.target().sqlName();
      case Scalar.InList i ->
          expr(i.operand(), cols) + (i.not() ? " NOT IN (" : " IN (") + exprs(i.list(), cols) + ")";
      case Scalar.Like l ->
          expr(l.operand(), cols)
              + (l.not() ? " NOT" : "")
              + (l.caseInsensitive() ? " ILIKE " : " LIKE ")
              + expr(l.pattern(), cols);
      case Scalar.Call c -> c.function() + "(" + exprs(c.args(), cols) + ")";
      case Scalar.Subquery q ->
          switch (q.kind()) {
            case SCALAR -> "(subquery)";
            case EXISTS -> "EXISTS(subquery)";
            case IN -> expr(q.operand(), cols) + (q.not() ? " NOT IN " : " IN ") + "(subquery)";
          };
      case Scalar.AggCall a -> a.function() + "(...)";
    };
  }
}

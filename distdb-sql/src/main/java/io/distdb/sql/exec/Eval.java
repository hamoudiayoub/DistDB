package io.distdb.sql.exec;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.plan.Scalar;
import io.distdb.sql.types.Values;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Evaluates bound expressions with SQL's three-valued logic: NULL (Java null) propagates through
 * operators, {@code NULL AND FALSE} is FALSE, {@code NULL OR TRUE} is TRUE, and {@code x IN (...)}
 * with no match and a NULL in the list is NULL.
 */
public final class Eval {

  private Eval() {}

  public static Object eval(Scalar s, Object[] row, ExecContext ctx) {
    return switch (s) {
      case Scalar.Const c -> c.value();
      case Scalar.ColRef c -> row[c.index()];
      case Scalar.OuterRef o -> ctx.outer(o);
      case Scalar.ParamRef p -> ctx.params[p.index()];
      case Scalar.Arith a -> {
        Object l = eval(a.left(), row, ctx);
        if (l == null) {
          yield null;
        }
        Object r = eval(a.right(), row, ctx);
        if (r == null) {
          yield null;
        }
        yield switch (a.op()) {
          case ADD -> Values.add(l, r);
          case SUB -> Values.subtract(l, r);
          case MUL -> Values.multiply(l, r);
          case DIV -> Values.divide(l, r);
          case MOD -> Values.modulo(l, r);
          case POW -> Values.power((Double) l, (Double) r);
        };
      }
      case Scalar.Neg n -> {
        Object v = eval(n.operand(), row, ctx);
        yield v == null ? null : Values.negate(v);
      }
      case Scalar.Compare c -> {
        Object l = eval(c.left(), row, ctx);
        if (l == null) {
          yield null;
        }
        Object r = eval(c.right(), row, ctx);
        if (r == null) {
          yield null;
        }
        int cmp = Values.compare(l, r);
        yield switch (c.op()) {
          case EQ -> cmp == 0;
          case NE -> cmp != 0;
          case LT -> cmp < 0;
          case LE -> cmp <= 0;
          case GT -> cmp > 0;
          case GE -> cmp >= 0;
        };
      }
      case Scalar.Concat c -> {
        Object l = eval(c.left(), row, ctx);
        Object r = l == null ? null : eval(c.right(), row, ctx);
        yield l == null || r == null ? null : (String) l + (String) r;
      }
      case Scalar.And a -> {
        Object l = eval(a.left(), row, ctx);
        if (Boolean.FALSE.equals(l)) {
          yield false;
        }
        Object r = eval(a.right(), row, ctx);
        if (Boolean.FALSE.equals(r)) {
          yield false;
        }
        yield l == null || r == null ? null : true;
      }
      case Scalar.Or o -> {
        Object l = eval(o.left(), row, ctx);
        if (Boolean.TRUE.equals(l)) {
          yield true;
        }
        Object r = eval(o.right(), row, ctx);
        if (Boolean.TRUE.equals(r)) {
          yield true;
        }
        yield l == null || r == null ? null : false;
      }
      case Scalar.Not n -> {
        Object v = eval(n.operand(), row, ctx);
        yield v == null ? null : !(Boolean) v;
      }
      case Scalar.IsNull n -> (eval(n.operand(), row, ctx) == null) != n.not();
      case Scalar.IsBool b -> {
        Object v = eval(b.operand(), row, ctx);
        boolean is = v != null && (Boolean) v == b.value();
        yield is != b.not();
      }
      case Scalar.IsDistinct d -> {
        Object l = eval(d.left(), row, ctx);
        Object r = eval(d.right(), row, ctx);
        boolean distinct = (l == null) != (r == null) || (l != null && Values.compare(l, r) != 0);
        yield distinct != d.not();
      }
      case Scalar.Case c -> {
        for (int k = 0; k < c.conditions().size(); k++) {
          if (Boolean.TRUE.equals(eval(c.conditions().get(k), row, ctx))) {
            yield eval(c.results().get(k), row, ctx);
          }
        }
        yield eval(c.otherwise(), row, ctx);
      }
      case Scalar.Cast c -> {
        Object v = eval(c.operand(), row, ctx);
        yield v == null ? null : Values.cast(v, c.operand().type(), c.target());
      }
      case Scalar.InList i -> {
        Object x = eval(i.operand(), row, ctx);
        if (x == null) {
          yield null;
        }
        boolean sawNull = false;
        Boolean result = null;
        for (Scalar item : i.list()) {
          Object v = eval(item, row, ctx);
          if (v == null) {
            sawNull = true;
          } else if (Values.compare(x, v) == 0) {
            result = true;
            break;
          }
        }
        if (result == null) {
          result = sawNull ? null : false;
        }
        yield result == null ? null : result != i.not();
      }
      case Scalar.Like l -> {
        Object x = eval(l.operand(), row, ctx);
        Object p = eval(l.pattern(), row, ctx);
        if (x == null || p == null) {
          yield null;
        }
        String escape = "\\";
        if (l.escape() != null) {
          Object e = eval(l.escape(), row, ctx);
          if (e == null) {
            yield null;
          }
          escape = (String) e;
          if (escape.codePointCount(0, escape.length()) > 1) {
            throw new SqlException(SqlState.INVALID_ESCAPE_SEQUENCE, "invalid escape string");
          }
        }
        boolean m = Like.matches((String) x, (String) p, escape, l.caseInsensitive());
        yield m != l.not();
      }
      case Scalar.Call c -> Builtins.call(c, row, ctx);
      case Scalar.Subquery q -> subquery(q, row, ctx);
      case Scalar.AggCall a ->
          throw new IllegalStateException("an aggregate outside an aggregation: " + a);
    };
  }

  /** Whether a predicate holds: TRUE, not FALSE or NULL. */
  public static boolean holds(Scalar s, Object[] row, ExecContext ctx) {
    return Boolean.TRUE.equals(eval(s, row, ctx));
  }

  /** The values of an IN subquery, with whether one of them was NULL. */
  private record InSet(Set<Object> values, boolean hasNull) {}

  private static Object subquery(Scalar.Subquery q, Object[] row, ExecContext ctx) {
    Object result;
    if (!q.correlated() && ctx.cache.containsKey(q)) {
      result = ctx.cache.get(q);
    } else {
      result = runSubquery(q, ctx.withOuter(row));
      if (!q.correlated()) {
        ctx.cache.put(q, result);
      }
    }
    if (q.kind() != Scalar.SubqueryKind.IN) {
      return result;
    }
    InSet set = (InSet) result;
    Object x = eval(q.operand(), row, ctx);
    Boolean in;
    if (set.values().isEmpty() && !set.hasNull()) {
      in = false;
    } else if (x == null) {
      in = null;
    } else if (set.values().contains(Values.hashKey(x))) {
      in = true;
    } else {
      in = set.hasNull() ? null : false;
    }
    return in == null ? null : in != q.not();
  }

  private static Object runSubquery(Scalar.Subquery q, ExecContext sub) {
    Operator op = Operators.build(q.plan(), sub);
    op.open();
    try {
      switch (q.kind()) {
        case EXISTS -> {
          return op.next() != null;
        }
        case SCALAR -> {
          Object[] first = op.next();
          if (first == null) {
            return null;
          }
          if (op.next() != null) {
            throw new SqlException(
                SqlState.CARDINALITY_VIOLATION,
                "more than one row returned by a subquery used as an expression");
          }
          return first[0];
        }
        case IN -> {
          Set<Object> values = new HashSet<>();
          boolean hasNull = false;
          for (Object[] r = op.next(); r != null; r = op.next()) {
            if (r[0] == null) {
              hasNull = true;
            } else {
              values.add(Values.hashKey(r[0]));
            }
          }
          return new InSet(values, hasNull);
        }
      }
      throw new IllegalStateException(q.kind().toString());
    } finally {
      op.close();
    }
  }

  /** Evaluates each of {@code list} on {@code row}. */
  static Object[] all(List<Scalar> list, Object[] row, ExecContext ctx) {
    Object[] out = new Object[list.size()];
    for (int k = 0; k < out.length; k++) {
      out[k] = eval(list.get(k), row, ctx);
    }
    return out;
  }
}

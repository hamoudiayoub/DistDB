package io.distdb.sql.exec;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.plan.Scalar;
import io.distdb.sql.types.Type;
import io.distdb.sql.types.Values;
import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;

/** The built-in scalar functions, as typed by the binder's {@code Functions}. */
final class Builtins {

  private Builtins() {}

  static Object call(Scalar.Call c, Object[] row, ExecContext ctx) {
    List<Scalar> args = c.args();
    switch (c.function()) {
      case "coalesce" -> {
        for (Scalar a : args) {
          Object v = Eval.eval(a, row, ctx);
          if (v != null) {
            return v;
          }
        }
        return null;
      }
      case "concat" -> {
        StringBuilder b = new StringBuilder();
        for (Scalar a : args) {
          Object v = Eval.eval(a, row, ctx);
          if (v != null) {
            b.append(Values.toText(v, a.type()));
          }
        }
        return b.toString();
      }
      case "greatest", "least" -> {
        Object best = null;
        for (Scalar a : args) {
          Object v = Eval.eval(a, row, ctx);
          if (v != null) {
            int cmp = best == null ? 0 : Values.compare(v, best);
            if (best == null || (c.function().equals("greatest") ? cmp > 0 : cmp < 0)) {
              best = v;
            }
          }
        }
        return best;
      }
      case "nullif" -> {
        Object a = Eval.eval(args.get(0), row, ctx);
        Object b = Eval.eval(args.get(1), row, ctx);
        return a != null && b != null && Values.compare(a, b) == 0 ? null : a;
      }
      case "unique_rowid" -> {
        return ctx.store.timestamps(1)[0];
      }
      case "version" -> {
        return "PostgreSQL 16.0 (DistDB, a distributed SQL database)";
      }
      case "current_database" -> {
        return "distdb";
      }
      case "current_user" -> {
        return "distdb";
      }
      case "current_schema" -> {
        return "public";
      }
      default -> {
        // strict functions: NULL in, NULL out
      }
    }
    Object[] v = Eval.all(args, row, ctx);
    for (Object o : v) {
      if (o == null) {
        return null;
      }
    }
    return switch (c.function()) {
      case "abs" ->
          switch (v[0]) {
            case Long l -> {
              if (l == Long.MIN_VALUE) {
                throw Values.bigintOutOfRange();
              }
              yield Math.abs(l);
            }
            case Double d -> Math.abs(d);
            case BigDecimal d -> d.abs();
            default -> throw new IllegalStateException();
          };
      case "ceil" ->
          c.type() == Type.NUMERIC ? Values.ceil((BigDecimal) v[0]) : Math.ceil((Double) v[0]);
      case "floor" ->
          c.type() == Type.NUMERIC ? Values.floor((BigDecimal) v[0]) : Math.floor((Double) v[0]);
      case "sign" ->
          c.type() == Type.NUMERIC
              ? BigDecimal.valueOf(((BigDecimal) v[0]).signum())
              : Math.signum((Double) v[0]);
      case "round" -> {
        if (v.length == 2) {
          yield Values.round((BigDecimal) v[0], places(v[1]));
        }
        yield c.type() == Type.NUMERIC
            ? Values.round((BigDecimal) v[0], 0)
            : Math.rint((Double) v[0]);
      }
      case "trunc" -> {
        if (v.length == 2) {
          yield Values.trunc((BigDecimal) v[0], places(v[1]));
        }
        yield c.type() == Type.NUMERIC
            ? Values.trunc((BigDecimal) v[0], 0)
            : (double) (long) (double) (Double) v[0];
      }
      case "sqrt" -> {
        double d = (Double) v[0];
        if (d < 0) {
          throw new SqlException("2201F", "cannot take square root of a negative number");
        }
        yield Math.sqrt(d);
      }
      case "power" -> Values.power((Double) v[0], (Double) v[1]);
      case "mod" -> Values.modulo(v[0], v[1]);
      case "length" -> (long) ((String) v[0]).codePointCount(0, ((String) v[0]).length());
      case "lower" -> ((String) v[0]).toLowerCase(Locale.ROOT);
      case "upper" -> ((String) v[0]).toUpperCase(Locale.ROOT);
      case "reverse" -> new StringBuilder((String) v[0]).reverse().toString();
      case "substr" -> substr((String) v[0], (Long) v[1], v.length > 2 ? (Long) v[2] : null);
      case "strpos" -> {
        String s = (String) v[0];
        int i = s.indexOf((String) v[1]);
        yield i < 0 ? 0L : (long) s.codePointCount(0, i) + 1;
      }
      case "replace" -> {
        String from = (String) v[1];
        yield from.isEmpty() ? v[0] : ((String) v[0]).replace(from, (String) v[2]);
      }
      case "left", "right" -> leftRight((String) v[0], (Long) v[1], c.function().equals("left"));
      case "repeat" -> {
        long n = (Long) v[1];
        yield n <= 0 ? "" : ((String) v[0]).repeat((int) Math.min(n, 1 << 20));
      }
      case "btrim", "ltrim", "rtrim" ->
          trim((String) v[0], v.length > 1 ? (String) v[1] : " ", c.function());
      default ->
          throw new SqlException(
              SqlState.UNDEFINED_FUNCTION, "function " + c.function() + " does not exist");
    };
  }

  private static int places(Object o) {
    long n = (Long) o;
    return (int) Math.max(-1000, Math.min(1000, n));
  }

  /** PostgreSQL's substr: positions from 1; a start before 1 eats into the length. */
  private static String substr(String s, long from, Long length) {
    int[] cps = s.codePoints().toArray();
    long start = from;
    long end;
    if (length == null) {
      end = Long.MAX_VALUE;
    } else {
      if (length < 0) {
        throw new SqlException("22011", "negative substring length not allowed");
      }
      end = from + length;
    }
    long lo = Math.max(start, 1);
    long hi = Math.min(end, (long) cps.length + 1);
    if (hi <= lo) {
      return "";
    }
    return new String(cps, (int) (lo - 1), (int) (hi - lo));
  }

  private static String leftRight(String s, long n, boolean left) {
    int[] cps = s.codePoints().toArray();
    int len = cps.length;
    int k = (int) (n >= 0 ? Math.min(n, len) : Math.max(len + n, 0));
    return left ? new String(cps, 0, k) : new String(cps, len - k, k);
  }

  private static String trim(String s, String chars, String which) {
    int start = 0;
    int end = s.length();
    if (!which.equals("rtrim")) {
      while (start < end && chars.indexOf(s.charAt(start)) >= 0) {
        start++;
      }
    }
    if (!which.equals("ltrim")) {
      while (end > start && chars.indexOf(s.charAt(end - 1)) >= 0) {
        end--;
      }
    }
    return s.substring(start, end);
  }
}

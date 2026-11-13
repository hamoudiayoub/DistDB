package io.distdb.sql.plan;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.plan.Scalar.AggCall;
import io.distdb.sql.plan.Scalar.Call;
import io.distdb.sql.types.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

/**
 * The built-in functions and aggregates, and their argument and result types, following
 * PostgreSQL's choices: {@code ceil(5)} is a double, {@code round(2.567, 1)} a numeric, {@code avg}
 * of integers a numeric, {@code count} a bigint.
 */
final class Functions {

  static final Set<String> AGGREGATES = Set.of("count", "sum", "avg", "min", "max");

  private Functions() {}

  static boolean isAggregate(String name) {
    return AGGREGATES.contains(name);
  }

  /** An aggregate call over {@code arg} (null for {@code count(*)}), typed. */
  static AggCall aggregate(String name, Scalar arg, boolean distinct, Binder b) {
    if (arg == null) {
      return new AggCall("count", null, false, Type.INT);
    }
    Type t = arg.type();
    return switch (name) {
      case "count" -> new AggCall(name, arg, distinct, Type.INT);
      case "sum" -> {
        Scalar a = numericArg(name, arg, b);
        yield new AggCall(name, a, distinct, a.type());
      }
      case "avg" -> {
        Scalar a = numericArg(name, arg, b);
        yield new AggCall(name, a, distinct, a.type() == Type.FLOAT ? Type.FLOAT : Type.NUMERIC);
      }
      case "min", "max" -> {
        Scalar a = t == Type.UNKNOWN ? b.coerce(arg, Type.TEXT) : arg;
        yield new AggCall(name, a, distinct, a.type());
      }
      default -> throw new IllegalArgumentException(name);
    };
  }

  private static Scalar numericArg(String name, Scalar arg, Binder b) {
    if (arg.type() == Type.UNKNOWN) {
      return b.coerce(arg, Type.NUMERIC);
    }
    if (!arg.type().isNumeric()) {
      throw undefined(name, List.of(arg));
    }
    return arg;
  }

  /** A scalar function call, with its arguments coerced. */
  static Scalar call(String name, List<Scalar> args, Binder b) {
    int n = args.size();
    switch (name) {
      case "abs" -> {
        need(name, args, 1);
        Scalar a = numeric(name, args.get(0), b, Type.FLOAT);
        return new Call(name, List.of(a), a.type());
      }
      case "ceil", "ceiling", "floor", "sign" -> {
        need(name, args, 1);
        Scalar a = numeric(name, args.get(0), b, Type.FLOAT);
        Type r = a.type() == Type.NUMERIC ? Type.NUMERIC : Type.FLOAT;
        return new Call(name.equals("ceiling") ? "ceil" : name, List.of(b.coerce(a, r)), r);
      }
      case "round", "trunc" -> {
        if (n == 2) {
          Scalar a = b.coerce(numeric(name, args.get(0), b, Type.NUMERIC), Type.NUMERIC);
          Scalar places = b.coerce(args.get(1), Type.INT);
          if (places.type() != Type.INT) {
            throw undefined(name, args);
          }
          return new Call(name, List.of(a, places), Type.NUMERIC);
        }
        need(name, args, 1);
        Scalar a = numeric(name, args.get(0), b, Type.FLOAT);
        Type r = a.type() == Type.NUMERIC ? Type.NUMERIC : Type.FLOAT;
        return new Call(name, List.of(b.coerce(a, r)), r);
      }
      case "sqrt" -> {
        need(name, args, 1);
        return new Call(
            name,
            List.of(b.coerce(numeric(name, args.get(0), b, Type.FLOAT), Type.FLOAT)),
            Type.FLOAT);
      }
      case "power", "pow" -> {
        need(name, args, 2);
        Scalar x = b.coerce(numeric(name, args.get(0), b, Type.FLOAT), Type.FLOAT);
        Scalar y = b.coerce(numeric(name, args.get(1), b, Type.FLOAT), Type.FLOAT);
        return new Call("power", List.of(x, y), Type.FLOAT);
      }
      case "mod" -> {
        need(name, args, 2);
        Type t =
            b.commonType(
                numeric(name, args.get(0), b, Type.INT),
                numeric(name, args.get(1), b, Type.INT),
                "mod");
        return new Call(name, List.of(b.coerce(args.get(0), t), b.coerce(args.get(1), t)), t);
      }
      case "length", "char_length", "character_length" -> {
        need(name, args, 1);
        return new Call("length", List.of(text(name, args.get(0), b)), Type.INT);
      }
      case "lower", "upper", "reverse" -> {
        need(name, args, 1);
        return new Call(name, List.of(text(name, args.get(0), b)), Type.TEXT);
      }
      case "substr", "substring" -> {
        if (n != 2 && n != 3) {
          throw undefined(name, args);
        }
        List<Scalar> a = new ArrayList<>();
        a.add(text(name, args.get(0), b));
        for (int k = 1; k < n; k++) {
          a.add(integer(name, args.get(k), b));
        }
        return new Call("substr", a, Type.TEXT);
      }
      case "strpos" -> {
        need(name, args, 2);
        return new Call(
            name, List.of(text(name, args.get(0), b), text(name, args.get(1), b)), Type.INT);
      }
      case "replace" -> {
        need(name, args, 3);
        return new Call(
            name,
            List.of(
                text(name, args.get(0), b), text(name, args.get(1), b), text(name, args.get(2), b)),
            Type.TEXT);
      }
      case "left", "right", "repeat" -> {
        need(name, args, 2);
        return new Call(
            name, List.of(text(name, args.get(0), b), integer(name, args.get(1), b)), Type.TEXT);
      }
      case "trim", "btrim", "ltrim", "rtrim" -> {
        if (n != 1 && n != 2) {
          throw undefined(name, args);
        }
        List<Scalar> a = new ArrayList<>();
        for (Scalar s : args) {
          a.add(text(name, s, b));
        }
        return new Call(name.equals("trim") ? "btrim" : name, a, Type.TEXT);
      }
      case "concat" -> {
        List<Scalar> a = new ArrayList<>();
        for (Scalar s : args) {
          a.add(s.type() == Type.UNKNOWN ? b.coerce(s, Type.TEXT) : s);
        }
        return new Call(name, a, Type.TEXT);
      }
      case "coalesce", "greatest", "least" -> {
        if (n == 0) {
          throw undefined(name, args);
        }
        Type t = Type.UNKNOWN;
        for (Scalar s : args) {
          t = b.commonType(t, s.type(), name.toUpperCase(java.util.Locale.ROOT));
        }
        if (t == Type.UNKNOWN) {
          t = Type.TEXT;
        }
        List<Scalar> a = new ArrayList<>();
        for (Scalar s : args) {
          a.add(b.coerce(s, t));
        }
        return new Call(name, a, t);
      }
      case "nullif" -> {
        need(name, args, 2);
        Type t = b.commonType(args.get(0).type(), args.get(1).type(), "NULLIF");
        if (t == Type.UNKNOWN) {
          t = Type.TEXT;
        }
        return new Call(name, List.of(b.coerce(args.get(0), t), b.coerce(args.get(1), t)), t);
      }
      case "unique_rowid" -> {
        need(name, args, 0);
        return new Call(name, List.of(), Type.INT);
      }
      case "version", "current_database", "current_user", "current_schema" -> {
        need(name, args, 0);
        return new Call(name, List.of(), Type.TEXT);
      }
      default -> throw undefined(name, args);
    }
  }

  private static void need(String name, List<Scalar> args, int n) {
    if (args.size() != n) {
      throw undefined(name, args);
    }
  }

  /** A numeric argument; an untyped literal becomes {@code dflt}. */
  private static Scalar numeric(String name, Scalar s, Binder b, Type dflt) {
    if (s.type() == Type.UNKNOWN) {
      return b.coerce(s, dflt);
    }
    if (!s.type().isNumeric()) {
      throw undefined(name, List.of(s));
    }
    return s;
  }

  private static Scalar text(String name, Scalar s, Binder b) {
    if (s.type() == Type.TEXT) {
      return s;
    }
    if (s.type() == Type.UNKNOWN) {
      return b.coerce(s, Type.TEXT);
    }
    throw undefined(name, List.of(s));
  }

  private static Scalar integer(String name, Scalar s, Binder b) {
    Scalar c = s.type() == Type.UNKNOWN ? b.coerce(s, Type.INT) : s;
    if (c.type() != Type.INT) {
      throw undefined(name, List.of(s));
    }
    return c;
  }

  static SqlException undefined(String name, List<Scalar> args) {
    StringJoiner types = new StringJoiner(", ");
    for (Scalar a : args) {
      types.add(a.type().sqlName());
    }
    return new SqlException(
        SqlState.UNDEFINED_FUNCTION, "function " + name + "(" + types + ") does not exist");
  }
}

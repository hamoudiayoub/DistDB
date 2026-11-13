package io.distdb.sql.exec;

import io.distdb.common.encoding.KeyReader;
import io.distdb.common.encoding.KeyWriter;
import io.distdb.common.encoding.ValueType;
import io.distdb.sql.plan.Scalar;
import io.distdb.sql.types.Type;
import io.distdb.sql.types.TypeSpec;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Expressions and values for the wire to the ranges, as tuples. Only expressions a range can
 * evaluate on its own are encodable ({@link #pushable}): no parameters, outer references or
 * subqueries (the gateway substitutes their values first), and no function whose value depends on
 * where it runs.
 */
public final class FragmentCodec {

  /** Functions whose value depends on the node or the moment: never evaluated on a range. */
  private static final Set<String> LOCAL_ONLY =
      Set.of("unique_rowid", "version", "current_database", "current_user", "current_schema");

  private FragmentCodec() {}

  /** Whether a range can evaluate {@code s} by itself. */
  public static boolean pushable(Scalar s) {
    return switch (s) {
      case Scalar.Const c -> true;
      case Scalar.ColRef c -> true;
      case Scalar.ParamRef p -> false;
      case Scalar.OuterRef o -> false;
      case Scalar.Subquery q -> false;
      case Scalar.AggCall a -> false;
      case Scalar.Call c ->
          !LOCAL_ONLY.contains(c.function()) && c.args().stream().allMatch(FragmentCodec::pushable);
      default -> io.distdb.sql.plan.Scalars.children(s).stream().allMatch(FragmentCodec::pushable);
    };
  }

  /**
   * Whether {@code s} is pushable once the gateway has replaced its parameters, outer references
   * and uncorrelated scalar subqueries by their values.
   */
  public static boolean pushableAfterBinding(Scalar s) {
    return switch (s) {
      case Scalar.ParamRef p -> true;
      case Scalar.OuterRef o -> true;
      case Scalar.Subquery q -> q.kind() == Scalar.SubqueryKind.SCALAR && !q.correlated();
      case Scalar.AggCall a -> false;
      case Scalar.Call c ->
          !LOCAL_ONLY.contains(c.function())
              && c.args().stream().allMatch(FragmentCodec::pushableAfterBinding);
      case Scalar.Const c -> true;
      case Scalar.ColRef c -> true;
      default ->
          io.distdb.sql.plan.Scalars.children(s).stream()
              .allMatch(FragmentCodec::pushableAfterBinding);
    };
  }

  /** {@code s} with parameters, outer references and scalar subqueries replaced by constants. */
  static Scalar bind(Scalar s, ExecContext ctx) {
    if (s == null) {
      return null;
    }
    return io.distdb.sql.plan.Scalars.map(
        s,
        x ->
            switch (x) {
              case Scalar.ParamRef p ->
                  new Scalar.Const(Eval.eval(p, new Object[0], ctx), p.type());
              case Scalar.OuterRef o ->
                  new Scalar.Const(Eval.eval(o, new Object[0], ctx), o.type());
              case Scalar.Subquery q ->
                  new Scalar.Const(Eval.eval(q, new Object[0], ctx), q.type());
              default -> null;
            });
  }

  // -----------------------------------------------------------------------------------------------
  // Expressions

  private enum Tag {
    CONST,
    COLREF,
    ARITH,
    NEG,
    COMPARE,
    CONCAT,
    AND,
    OR,
    NOT,
    ISNULL,
    ISBOOL,
    ISDISTINCT,
    CASE,
    CAST,
    INLIST,
    LIKE,
    CALL
  }

  static void writeScalarOrNull(KeyWriter w, Scalar s) {
    if (s == null) {
      w.writeNull();
    } else {
      writeScalar(w, s);
    }
  }

  static Scalar readScalarOrNull(KeyReader r) {
    if (r.peekType() == ValueType.NULL) {
      r.readNull();
      return null;
    }
    return readScalar(r);
  }

  static void writeScalar(KeyWriter w, Scalar s) {
    switch (s) {
      case Scalar.Const c -> {
        tag(w, Tag.CONST);
        w.writeLong(c.type().ordinal());
        writeValue(w, c.type(), c.value());
      }
      case Scalar.ColRef c -> {
        tag(w, Tag.COLREF);
        w.writeLong(c.index()).writeLong(c.type().ordinal());
      }
      case Scalar.Arith a -> {
        tag(w, Tag.ARITH);
        w.writeLong(a.op().ordinal()).writeLong(a.type().ordinal());
        writeScalar(w, a.left());
        writeScalar(w, a.right());
      }
      case Scalar.Neg n -> {
        tag(w, Tag.NEG);
        w.writeLong(n.type().ordinal());
        writeScalar(w, n.operand());
      }
      case Scalar.Compare c -> {
        tag(w, Tag.COMPARE);
        w.writeLong(c.op().ordinal());
        writeScalar(w, c.left());
        writeScalar(w, c.right());
      }
      case Scalar.Concat c -> {
        tag(w, Tag.CONCAT);
        writeScalar(w, c.left());
        writeScalar(w, c.right());
      }
      case Scalar.And a -> {
        tag(w, Tag.AND);
        writeScalar(w, a.left());
        writeScalar(w, a.right());
      }
      case Scalar.Or o -> {
        tag(w, Tag.OR);
        writeScalar(w, o.left());
        writeScalar(w, o.right());
      }
      case Scalar.Not n -> {
        tag(w, Tag.NOT);
        writeScalar(w, n.operand());
      }
      case Scalar.IsNull n -> {
        tag(w, Tag.ISNULL);
        w.writeBoolean(n.not());
        writeScalar(w, n.operand());
      }
      case Scalar.IsBool b -> {
        tag(w, Tag.ISBOOL);
        w.writeBoolean(b.value()).writeBoolean(b.not());
        writeScalar(w, b.operand());
      }
      case Scalar.IsDistinct d -> {
        tag(w, Tag.ISDISTINCT);
        w.writeBoolean(d.not());
        writeScalar(w, d.left());
        writeScalar(w, d.right());
      }
      case Scalar.Case c -> {
        tag(w, Tag.CASE);
        w.writeLong(c.type().ordinal()).writeLong(c.conditions().size());
        for (int k = 0; k < c.conditions().size(); k++) {
          writeScalar(w, c.conditions().get(k));
          writeScalar(w, c.results().get(k));
        }
        writeScalar(w, c.otherwise());
      }
      case Scalar.Cast c -> {
        tag(w, Tag.CAST);
        TypeSpec t = c.target();
        w.writeLong(t.type().ordinal())
            .writeLong(t.length())
            .writeLong(t.precision())
            .writeLong(t.scale());
        writeScalar(w, c.operand());
      }
      case Scalar.InList i -> {
        tag(w, Tag.INLIST);
        w.writeBoolean(i.not()).writeLong(i.list().size());
        writeScalar(w, i.operand());
        for (Scalar e : i.list()) {
          writeScalar(w, e);
        }
      }
      case Scalar.Like l -> {
        tag(w, Tag.LIKE);
        w.writeBoolean(l.not()).writeBoolean(l.caseInsensitive());
        writeScalar(w, l.operand());
        writeScalar(w, l.pattern());
        writeScalarOrNull(w, l.escape());
      }
      case Scalar.Call c -> {
        tag(w, Tag.CALL);
        w.writeString(c.function()).writeLong(c.type().ordinal()).writeLong(c.args().size());
        for (Scalar a : c.args()) {
          writeScalar(w, a);
        }
      }
      default -> throw new IllegalArgumentException("not evaluable on a range: " + s.getClass());
    }
  }

  private static void tag(KeyWriter w, Tag t) {
    w.writeLong(t.ordinal());
  }

  private static Type type(KeyReader r) {
    return Type.values()[(int) r.readLong()];
  }

  static Scalar readScalar(KeyReader r) {
    Tag t = Tag.values()[(int) r.readLong()];
    return switch (t) {
      case CONST -> {
        Type type = type(r);
        yield new Scalar.Const(readValue(r, type), type);
      }
      case COLREF -> {
        int index = (int) r.readLong();
        yield new Scalar.ColRef(index, type(r));
      }
      case ARITH -> {
        Scalar.ArithOp op = Scalar.ArithOp.values()[(int) r.readLong()];
        Type type = type(r);
        yield new Scalar.Arith(op, readScalar(r), readScalar(r), type);
      }
      case NEG -> {
        Type type = type(r);
        yield new Scalar.Neg(readScalar(r), type);
      }
      case COMPARE -> {
        Scalar.CmpOp op = Scalar.CmpOp.values()[(int) r.readLong()];
        yield new Scalar.Compare(op, readScalar(r), readScalar(r));
      }
      case CONCAT -> new Scalar.Concat(readScalar(r), readScalar(r));
      case AND -> new Scalar.And(readScalar(r), readScalar(r));
      case OR -> new Scalar.Or(readScalar(r), readScalar(r));
      case NOT -> new Scalar.Not(readScalar(r));
      case ISNULL -> {
        boolean not = r.readBoolean();
        yield new Scalar.IsNull(readScalar(r), not);
      }
      case ISBOOL -> {
        boolean value = r.readBoolean();
        boolean not = r.readBoolean();
        yield new Scalar.IsBool(readScalar(r), value, not);
      }
      case ISDISTINCT -> {
        boolean not = r.readBoolean();
        yield new Scalar.IsDistinct(readScalar(r), readScalar(r), not);
      }
      case CASE -> {
        Type type = type(r);
        int n = (int) r.readLong();
        List<Scalar> conds = new ArrayList<>();
        List<Scalar> results = new ArrayList<>();
        for (int k = 0; k < n; k++) {
          conds.add(readScalar(r));
          results.add(readScalar(r));
        }
        yield new Scalar.Case(conds, results, readScalar(r), type);
      }
      case CAST -> {
        TypeSpec spec =
            new TypeSpec(type(r), (int) r.readLong(), (int) r.readLong(), (int) r.readLong());
        yield new Scalar.Cast(readScalar(r), spec);
      }
      case INLIST -> {
        boolean not = r.readBoolean();
        int n = (int) r.readLong();
        Scalar operand = readScalar(r);
        List<Scalar> list = new ArrayList<>();
        for (int k = 0; k < n; k++) {
          list.add(readScalar(r));
        }
        yield new Scalar.InList(operand, list, not);
      }
      case LIKE -> {
        boolean not = r.readBoolean();
        boolean ci = r.readBoolean();
        Scalar operand = readScalar(r);
        Scalar pattern = readScalar(r);
        yield new Scalar.Like(operand, pattern, readScalarOrNull(r), not, ci);
      }
      case CALL -> {
        String fn = r.readString();
        Type type = type(r);
        int n = (int) r.readLong();
        List<Scalar> args = new ArrayList<>();
        for (int k = 0; k < n; k++) {
          args.add(readScalar(r));
        }
        yield new Scalar.Call(fn, args, type);
      }
    };
  }

  // -----------------------------------------------------------------------------------------------
  // Values

  /** A value of type {@code t}; a NUMERIC keeps its scale. */
  public static void writeValue(KeyWriter w, Type t, Object v) {
    if (v == null) {
      w.writeNull();
      return;
    }
    switch (t) {
      case BOOL -> w.writeBoolean((Boolean) v);
      case INT -> w.writeLong((Long) v);
      case FLOAT -> w.writeDouble((Double) v);
      case NUMERIC -> w.writeString(((BigDecimal) v).toPlainString());
      case TEXT, UNKNOWN -> w.writeString((String) v);
    }
  }

  public static Object readValue(KeyReader r, Type t) {
    if (r.peekType() == ValueType.NULL) {
      r.readNull();
      return null;
    }
    return switch (t) {
      case BOOL -> r.readBoolean();
      case INT -> r.readLong();
      case FLOAT -> r.readDouble();
      case NUMERIC -> new BigDecimal(r.readString());
      case TEXT, UNKNOWN -> r.readString();
    };
  }
}

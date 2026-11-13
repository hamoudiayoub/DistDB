package io.distdb.sql.plan;

import io.distdb.sql.types.Type;
import io.distdb.sql.types.TypeSpec;
import java.util.List;

/**
 * A bound expression: names resolved to row positions, types decided, casts made explicit. It is
 * evaluated against one row of its input ({@code io.distdb.sql.exec.Eval}). Records, so two
 * occurrences of the same expression are {@code equals} (the binder matches {@code GROUP BY}
 * expressions this way).
 */
public sealed interface Scalar {

  Type type();

  record Const(Object value, Type type) implements Scalar {}

  /** The value at {@code index} in the input row. */
  record ColRef(int index, Type type) implements Scalar {}

  /**
   * A column of an enclosing query, from a correlated subquery: {@code inner} evaluated against the
   * row {@code depth} levels out.
   */
  record OuterRef(int depth, Scalar inner) implements Scalar {
    @Override
    public Type type() {
      return inner.type();
    }
  }

  /** Statement parameter {@code $index+1}. */
  record ParamRef(int index, Type type) implements Scalar {}

  enum ArithOp {
    ADD,
    SUB,
    MUL,
    DIV,
    MOD,
    POW
  }

  record Arith(ArithOp op, Scalar left, Scalar right, Type type) implements Scalar {}

  record Neg(Scalar operand, Type type) implements Scalar {}

  enum CmpOp {
    EQ,
    NE,
    LT,
    LE,
    GT,
    GE;

    /** The operator with its operands swapped: {@code a < b} is {@code b > a}. */
    public CmpOp flip() {
      return switch (this) {
        case LT -> GT;
        case LE -> GE;
        case GT -> LT;
        case GE -> LE;
        default -> this;
      };
    }
  }

  record Compare(CmpOp op, Scalar left, Scalar right) implements Scalar {
    @Override
    public Type type() {
      return Type.BOOL;
    }
  }

  record Concat(Scalar left, Scalar right) implements Scalar {
    @Override
    public Type type() {
      return Type.TEXT;
    }
  }

  record And(Scalar left, Scalar right) implements Scalar {
    @Override
    public Type type() {
      return Type.BOOL;
    }
  }

  record Or(Scalar left, Scalar right) implements Scalar {
    @Override
    public Type type() {
      return Type.BOOL;
    }
  }

  record Not(Scalar operand) implements Scalar {
    @Override
    public Type type() {
      return Type.BOOL;
    }
  }

  record IsNull(Scalar operand, boolean not) implements Scalar {
    @Override
    public Type type() {
      return Type.BOOL;
    }
  }

  record IsBool(Scalar operand, boolean value, boolean not) implements Scalar {
    @Override
    public Type type() {
      return Type.BOOL;
    }
  }

  record IsDistinct(Scalar left, Scalar right, boolean not) implements Scalar {
    @Override
    public Type type() {
      return Type.BOOL;
    }
  }

  record Case(List<Scalar> conditions, List<Scalar> results, Scalar otherwise, Type type)
      implements Scalar {}

  record Cast(Scalar operand, TypeSpec target) implements Scalar {
    @Override
    public Type type() {
      return target.type();
    }
  }

  record InList(Scalar operand, List<Scalar> list, boolean not) implements Scalar {
    @Override
    public Type type() {
      return Type.BOOL;
    }
  }

  record Like(Scalar operand, Scalar pattern, Scalar escape, boolean not, boolean caseInsensitive)
      implements Scalar {
    @Override
    public Type type() {
      return Type.BOOL;
    }
  }

  /** A built-in scalar function, by name; arguments already coerced to what it takes. */
  record Call(String function, List<Scalar> args, Type type) implements Scalar {}

  enum SubqueryKind {
    /** {@code (SELECT x ...)}: one value, or NULL for no row; two rows are an error. */
    SCALAR,
    EXISTS,
    /** {@code operand [NOT] IN (SELECT ...)}. */
    IN
  }

  /**
   * A subquery. {@code correlated} ones refer to the enclosing row and run again for each row; the
   * others run once per statement.
   */
  record Subquery(
      SubqueryKind kind, Plan plan, Scalar operand, boolean not, boolean correlated, Type type)
      implements Scalar {}

  /**
   * An aggregate call while binding; the binder replaces it by a reference to the aggregate's
   * output column. {@code arg} is null for {@code count(*)}.
   */
  record AggCall(String function, Scalar arg, boolean distinct, Type type) implements Scalar {}
}

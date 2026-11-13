package io.distdb.sql.plan;

import io.distdb.sql.plan.Scalar.AggCall;
import io.distdb.sql.plan.Scalar.And;
import io.distdb.sql.plan.Scalar.Arith;
import io.distdb.sql.plan.Scalar.Call;
import io.distdb.sql.plan.Scalar.Case;
import io.distdb.sql.plan.Scalar.Cast;
import io.distdb.sql.plan.Scalar.ColRef;
import io.distdb.sql.plan.Scalar.Compare;
import io.distdb.sql.plan.Scalar.Concat;
import io.distdb.sql.plan.Scalar.Const;
import io.distdb.sql.plan.Scalar.InList;
import io.distdb.sql.plan.Scalar.IsBool;
import io.distdb.sql.plan.Scalar.IsDistinct;
import io.distdb.sql.plan.Scalar.IsNull;
import io.distdb.sql.plan.Scalar.Like;
import io.distdb.sql.plan.Scalar.Neg;
import io.distdb.sql.plan.Scalar.Not;
import io.distdb.sql.plan.Scalar.Or;
import io.distdb.sql.plan.Scalar.OuterRef;
import io.distdb.sql.plan.Scalar.ParamRef;
import io.distdb.sql.plan.Scalar.Subquery;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/** Walking and rewriting expression trees. */
public final class Scalars {

  private Scalars() {}

  /**
   * {@code s} rewritten bottom-up... top-down: where {@code f} returns non-null for a node, that
   * replaces it (and its subtree); elsewhere the node is rebuilt from rewritten children. Does not
   * enter subquery plans (their column references belong to another row).
   */
  public static Scalar map(Scalar s, Function<Scalar, Scalar> f) {
    Scalar r = f.apply(s);
    if (r != null) {
      return r;
    }
    return mapChildren(s, x -> map(x, f));
  }

  /** {@code s} with each direct child replaced by {@code f(child)}. */
  public static Scalar mapChildren(Scalar s, UnaryOperator<Scalar> f) {
    return switch (s) {
      case Const c -> c;
      case ColRef c -> c;
      case OuterRef o -> o;
      case ParamRef p -> p;
      case Arith a -> new Arith(a.op(), f.apply(a.left()), f.apply(a.right()), a.type());
      case Neg n -> new Neg(f.apply(n.operand()), n.type());
      case Compare c -> new Compare(c.op(), f.apply(c.left()), f.apply(c.right()));
      case Concat c -> new Concat(f.apply(c.left()), f.apply(c.right()));
      case And a -> new And(f.apply(a.left()), f.apply(a.right()));
      case Or o -> new Or(f.apply(o.left()), f.apply(o.right()));
      case Not n -> new Not(f.apply(n.operand()));
      case IsNull n -> new IsNull(f.apply(n.operand()), n.not());
      case IsBool b -> new IsBool(f.apply(b.operand()), b.value(), b.not());
      case IsDistinct d -> new IsDistinct(f.apply(d.left()), f.apply(d.right()), d.not());
      case Case c ->
          new Case(all(c.conditions(), f), all(c.results(), f), f.apply(c.otherwise()), c.type());
      case Cast c -> new Cast(f.apply(c.operand()), c.target());
      case InList i -> new InList(f.apply(i.operand()), all(i.list(), f), i.not());
      case Like l ->
          new Like(
              f.apply(l.operand()),
              f.apply(l.pattern()),
              l.escape() == null ? null : f.apply(l.escape()),
              l.not(),
              l.caseInsensitive());
      case Call c -> new Call(c.function(), all(c.args(), f), c.type());
      case Subquery q ->
          q.operand() == null
              ? q
              : new Subquery(
                  q.kind(), q.plan(), f.apply(q.operand()), q.not(), q.correlated(), q.type());
      case AggCall a ->
          a.arg() == null ? a : new AggCall(a.function(), f.apply(a.arg()), a.distinct(), a.type());
    };
  }

  private static List<Scalar> all(List<Scalar> list, UnaryOperator<Scalar> f) {
    List<Scalar> out = new ArrayList<>(list.size());
    for (Scalar s : list) {
      out.add(f.apply(s));
    }
    return out;
  }

  /** Calls {@code v} on every node of {@code s}, without entering subquery plans. */
  public static void visit(Scalar s, Consumer<Scalar> v) {
    v.accept(s);
    mapChildren(
        s,
        x -> {
          visit(x, v);
          return x;
        });
  }

  /** The direct children of {@code s}. */
  public static List<Scalar> children(Scalar s) {
    List<Scalar> out = new ArrayList<>();
    mapChildren(
        s,
        x -> {
          out.add(x);
          return x;
        });
    return out;
  }

  /** Whether {@code s} reads its input row (column references outside subqueries). */
  public static boolean readsRow(Scalar s) {
    boolean[] found = {false};
    visit(s, x -> found[0] |= x instanceof ColRef || (x instanceof Subquery q && q.correlated()));
    return found[0];
  }

  /** Whether {@code s} contains a subquery. */
  public static boolean hasSubquery(Scalar s) {
    boolean[] found = {false};
    visit(s, x -> found[0] |= x instanceof Subquery);
    return found[0];
  }

  /** The positions of the input row {@code s} reads. */
  public static List<Integer> columnsRead(Scalar s) {
    List<Integer> out = new ArrayList<>();
    visit(
        s,
        x -> {
          if (x instanceof ColRef c && !out.contains(c.index())) {
            out.add(c.index());
          }
        });
    return out;
  }

  /** {@code a AND b AND ...} as a list of its conjuncts. */
  public static List<Scalar> conjuncts(Scalar s) {
    List<Scalar> out = new ArrayList<>();
    collect(s, out);
    return out;
  }

  private static void collect(Scalar s, List<Scalar> out) {
    if (s instanceof And a) {
      collect(a.left(), out);
      collect(a.right(), out);
    } else if (!(s instanceof Const c && Boolean.TRUE.equals(c.value()))) {
      out.add(s);
    }
  }

  /** The conjunction of {@code list}, or null when it is empty. */
  public static Scalar and(List<Scalar> list) {
    Scalar out = null;
    for (Scalar s : list) {
      out = out == null ? s : new And(out, s);
    }
    return out;
  }
}

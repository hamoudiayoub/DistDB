package io.distdb.sql.plan;

import io.distdb.sql.catalog.Catalog;
import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.plan.Scalar.CmpOp;
import io.distdb.sql.stats.TableStats;
import java.util.List;

/**
 * Row counts and costs of physical plans, from the statistics {@code ANALYZE} collected (or
 * PostgreSQL's defaults without them). Costs are in units of one network round trip: a request
 * costs 1, moving a row over the network 0.01, reading a row on a range 0.002, processing a row on
 * the gateway 0.001.
 */
final class Estimator {

  static final double RPC = 1.0;
  static final double TRANSFER = 0.01;
  static final double READ = 0.002;
  static final double CPU = 0.001;

  static final double DEFAULT_ROWS = 1000;
  static final double EQ_SEL = 0.005;
  static final double RANGE_SEL = 1.0 / 3;
  static final double OTHER_SEL = 1.0 / 3;
  static final double NULL_SEL = 0.01;

  /** An estimate: rows out, and the total cost to produce them. */
  record Est(double rows, double cost) {}

  private final Catalog.Snapshot catalog;
  private final boolean pushdown;

  Estimator(Catalog.Snapshot catalog, boolean pushdown) {
    this.catalog = catalog;
    this.pushdown = pushdown;
  }

  TableStats stats(TableDesc t) {
    return catalog == null ? null : catalog.statsOf(t);
  }

  double rows(TableDesc t) {
    TableStats s = stats(t);
    return s == null ? DEFAULT_ROWS : Math.max(1, s.rows());
  }

  int ranges(TableDesc t) {
    TableStats s = stats(t);
    return s == null ? 1 : Math.max(1, s.ranges());
  }

  /** Distinct values of column {@code pos} of {@code t}, or -1 without statistics. */
  double distinct(TableDesc t, int pos) {
    TableStats s = stats(t);
    TableStats.ColumnStats c = s == null ? null : s.column(t.columns().get(pos).id());
    return c == null ? -1 : Math.max(1, c.distinct());
  }

  // -----------------------------------------------------------------------------------------------
  // Selectivity

  /** The fraction of {@code t}'s rows (columns at their table positions) that pass {@code p}. */
  double selectivity(Scalar p, TableDesc t) {
    return Math.max(0, Math.min(1, sel(p, t)));
  }

  private double sel(Scalar p, TableDesc t) {
    return switch (p) {
      case Scalar.And a -> sel(a.left(), t) * sel(a.right(), t);
      case Scalar.Or o -> {
        double l = sel(o.left(), t);
        double r = sel(o.right(), t);
        yield l + r - l * r;
      }
      case Scalar.Not n -> 1 - sel(n.operand(), t);
      case Scalar.Const c -> Boolean.TRUE.equals(c.value()) ? 1 : 0;
      case Scalar.IsNull n -> {
        double f = nullFraction(n.operand(), t);
        yield n.not() ? 1 - f : f;
      }
      case Scalar.InList i -> {
        double one = i.operand() instanceof Scalar.ColRef c ? eq(t, c.index()) : EQ_SEL;
        double in = Math.min(1, one * i.list().size());
        yield i.not() ? 1 - in : in;
      }
      case Scalar.Like l -> l.not() ? 0.9 : 0.1;
      case Scalar.Compare c -> compare(c, t);
      default -> OTHER_SEL;
    };
  }

  private double nullFraction(Scalar s, TableDesc t) {
    if (t != null && s instanceof Scalar.ColRef c) {
      TableStats st = stats(t);
      TableStats.ColumnStats cs = st == null ? null : st.column(t.columns().get(c.index()).id());
      if (cs != null) {
        return cs.nullFraction(st.rows());
      }
    }
    return NULL_SEL;
  }

  private double eq(TableDesc t, int pos) {
    if (t == null) {
      return EQ_SEL;
    }
    double d = distinct(t, pos);
    if (d < 0) {
      return EQ_SEL;
    }
    return (1 - nullFraction(new Scalar.ColRef(pos, null), t)) / d;
  }

  private double compare(Scalar.Compare c, TableDesc t) {
    Scalar.ColRef col = c.left() instanceof Scalar.ColRef l ? l : null;
    Scalar other = c.right();
    CmpOp op = c.op();
    if (col == null && c.right() instanceof Scalar.ColRef r) {
      col = r;
      other = c.left();
      op = op.flip();
    }
    if (col == null) {
      return op == CmpOp.EQ ? EQ_SEL : OTHER_SEL;
    }
    if (other instanceof Scalar.ColRef) {
      return op == CmpOp.EQ ? EQ_SEL : OTHER_SEL; // two columns of one row
    }
    return switch (op) {
      case EQ -> eq(t, col.index());
      case NE -> 1 - eq(t, col.index());
      default -> range(t, col.index(), op, other);
    };
  }

  private double range(TableDesc t, int pos, CmpOp op, Scalar v) {
    TableStats st = t == null ? null : stats(t);
    TableStats.ColumnStats cs = st == null ? null : st.column(t.columns().get(pos).id());
    if (cs == null
        || !(v instanceof Scalar.Const k)
        || k.value() == null
        || cs.bounds().isEmpty()
        || k.value().getClass() != cs.bounds().get(0).getClass()) {
      return RANGE_SEL;
    }
    double nonNull = 1 - cs.nullFraction(st.rows());
    double below = cs.fractionBelow(k.value(), op == CmpOp.LE || op == CmpOp.GT);
    double f = op == CmpOp.LT || op == CmpOp.LE ? below : 1 - below;
    return Math.max(1.0 / Math.max(1, st.rows()), f * nonNull);
  }

  // -----------------------------------------------------------------------------------------------
  // Plans

  /** The table a plan scans directly, if it is a scan of one table. */
  static TableDesc baseTable(Plan p) {
    return switch (p) {
      case Plan.FullScan s -> s.table();
      case Plan.IndexScan s -> s.table();
      case Plan.PointGet g -> g.table();
      case Plan.RemoteScan s -> s.aggregating() ? null : s.table();
      case Plan.Scan s -> s.table();
      default -> null;
    };
  }

  Est est(Plan p) {
    return switch (p) {
      case Plan.FullScan s -> {
        double rows = rows(s.table());
        yield new Est(rows, ranges(s.table()) * RPC + rows * (READ + TRANSFER));
      }
      case Plan.PointGet g -> new Est(1, RPC);
      case Plan.IndexScan s -> {
        double rows = rows(s.table()) * prefixSelectivity(s);
        if (s.index().unique() && s.prefix().size() == s.index().columnIds().size()) {
          rows = Math.min(rows, 1); // a unique key fixed: one row at most
        }
        boolean primary = s.index().id() == TableDesc.PRIMARY_INDEX_ID;
        yield new Est(rows, RPC + rows * (READ + TRANSFER + (primary ? 0 : RPC)));
      }
      case Plan.Filter f -> {
        TableDesc t = baseTable(f.input());
        Est in = est(f.input());
        double sel = selectivity(f.predicate(), t);
        double cost = in.cost() + in.rows() * CPU;
        if (pushdown && (f.input() instanceof Plan.FullScan || primaryRange(f.input()))) {
          // On the ranges the filter runs next to the data (its work is in READ), and only the
          // rows passing it travel.
          cost = in.cost() - in.rows() * TRANSFER * (1 - sel);
        }
        yield new Est(Math.max(1, in.rows() * sel), cost);
      }
      case Plan.Project pr -> {
        Est in = est(pr.input());
        yield new Est(in.rows(), in.cost() + in.rows() * CPU);
      }
      case Plan.HashJoin j -> {
        Est l = est(j.left());
        Est r = est(j.right());
        double rows = joinRows(l, r, j.leftKeys(), j.rightKeys(), j.left(), j.right());
        yield new Est(rows, l.cost() + r.cost() + (2 * r.rows() + l.rows() + rows) * CPU);
      }
      case Plan.LookupJoin j -> {
        Est l = est(j.left());
        yield new Est(Math.max(1, l.rows()), l.cost() + l.rows() * (RPC + TRANSFER));
      }
      case Plan.NestedLoopJoin j -> {
        Est l = est(j.left());
        Est r = est(j.right());
        double rows = Math.max(1, l.rows() * r.rows() * OTHER_SEL);
        yield new Est(rows, l.cost() + r.cost() + l.rows() * r.rows() * CPU);
      }
      case Plan.Aggregate a -> {
        Est in = est(a.input());
        double rows = a.groups().isEmpty() ? 1 : Math.max(1, in.rows() / 10);
        yield new Est(rows, in.cost() + in.rows() * CPU);
      }
      case Plan.Sort s -> {
        Est in = est(s.input());
        double n = Math.max(2, in.rows());
        yield new Est(in.rows(), in.cost() + n * Math.log(n) * CPU);
      }
      case Plan.Limit l -> {
        Est in = est(l.input());
        double rows =
            l.limit() instanceof Scalar.Const c && c.value() instanceof Long n
                ? Math.min(in.rows(), n)
                : in.rows();
        yield new Est(rows, in.cost());
      }
      case Plan.Distinct d -> {
        Est in = est(d.input());
        yield new Est(Math.max(1, in.rows() / 2), in.cost() + in.rows() * CPU);
      }
      case Plan.Values v -> new Est(v.rows().size(), 0);
      case Plan.OneRow o -> new Est(1, 0);
      case Plan.SetOp s -> {
        Est l = est(s.left());
        Est r = est(s.right());
        yield new Est(l.rows() + r.rows(), l.cost() + r.cost() + (l.rows() + r.rows()) * CPU);
      }
      case Plan.RemoteScan s -> new Est(rows(s.table()), ranges(s.table()) * RPC);
      case Plan.MergeAggregate m -> new Est(1, est(m.input()).cost());
      case Plan.Scan s -> est(new Plan.FullScan(s.table(), s.alias(), s.columns()));
      case Plan.Join j -> {
        Est l = est(j.left());
        Est r = est(j.right());
        yield new Est(Math.max(1, l.rows() * r.rows() * OTHER_SEL), l.cost() + r.cost());
      }
    };
  }

  private static boolean primaryRange(Plan p) {
    return p instanceof Plan.IndexScan s && s.index().id() == TableDesc.PRIMARY_INDEX_ID;
  }

  /** The fraction of a table an index scan's fixed prefix and bounds select. */
  double prefixSelectivity(Plan.IndexScan s) {
    TableDesc t = s.table();
    int[] cols = t.positions(s.index());
    double sel = 1;
    for (int k = 0; k < s.prefix().size(); k++) {
      sel *= eq(t, cols[k]);
    }
    if (s.low() != null || s.high() != null) {
      int next = cols[s.prefix().size()];
      if (s.low() != null) {
        sel *= range(t, next, s.low().inclusive() ? CmpOp.GE : CmpOp.GT, s.low().value());
      }
      if (s.high() != null) {
        sel *= range(t, next, s.high().inclusive() ? CmpOp.LE : CmpOp.LT, s.high().value());
      }
    }
    return sel;
  }

  /** An equi-join's rows: |L| × |R| / max(distinct keys), with distinct counts when known. */
  double joinRows(
      Est l, Est r, List<Scalar> leftKeys, List<Scalar> rightKeys, Plan left, Plan right) {
    double rows = l.rows() * r.rows();
    TableDesc lt = baseTable(left);
    TableDesc rt = baseTable(right);
    for (int k = 0; k < leftKeys.size(); k++) {
      double dl =
          leftKeys.get(k) instanceof Scalar.ColRef c && lt != null ? distinct(lt, c.index()) : -1;
      double dr =
          rightKeys.get(k) instanceof Scalar.ColRef c && rt != null ? distinct(rt, c.index()) : -1;
      double d = Math.max(dl, dr);
      if (d < 0) {
        // No statistics: assume a key on the smaller side.
        d = Math.max(1, Math.min(l.rows(), r.rows()));
      }
      rows /= d;
    }
    return Math.max(1, rows);
  }
}

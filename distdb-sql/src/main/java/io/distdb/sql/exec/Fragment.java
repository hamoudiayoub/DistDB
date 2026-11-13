package io.distdb.sql.exec;

import io.distdb.common.encoding.KeyReader;
import io.distdb.common.encoding.KeyWriter;
import io.distdb.sql.SqlException;
import io.distdb.sql.catalog.Catalog;
import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.encoding.RowCodec;
import io.distdb.sql.plan.Scalar;
import io.distdb.sql.types.Type;
import io.distdb.sql.types.TypeSpec;
import io.distdb.sql.types.Values;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A query fragment pushed down to the ranges: over the rows of one table's primary index, keep
 * those that pass {@code filter}, then either send them back (only the {@code keep} columns; the
 * others as NULL) or aggregate them into partial states per group. {@code limit} caps the rows a
 * range sends (-1: none). The same fragment runs on the gateway when it cannot run remotely (the
 * transaction has its own writes in the span), so both paths compute the same thing.
 *
 * <p>Partial states, per aggregate: {@code count} → a count; {@code sum} → a sum (NULL when no
 * row); {@code min}/{@code max} → the extreme; {@code avg} → a sum and a count (two columns),
 * because an average of averages is wrong when ranges hold different numbers of rows.
 */
public record Fragment(
    TableDesc table,
    Scalar filter,
    boolean[] keep,
    List<Scalar> groups,
    List<Scalar.AggCall> aggs,
    long limit) {

  /** Rows a range sends per response before it stops and lets the gateway resume. */
  static final int ROW_BUDGET = 2048;

  private static final int FORMAT = 1;

  public boolean aggregating() {
    return !groups.isEmpty() || !aggs.isEmpty();
  }

  /** The types of the rows this fragment produces (partial states when aggregating). */
  public List<Type> outputTypes() {
    List<Type> out = new ArrayList<>();
    if (!aggregating()) {
      for (TableDesc.Column c : table.columns()) {
        out.add(c.type().type());
      }
      return out;
    }
    for (Scalar g : groups) {
      out.add(g.type());
    }
    for (Scalar.AggCall a : aggs) {
      switch (a.function()) {
        case "count" -> out.add(Type.INT);
        case "avg" -> {
          out.add(a.arg().type() == Type.FLOAT ? Type.FLOAT : Type.NUMERIC);
          out.add(Type.INT);
        }
        case "sum" -> out.add(a.arg().type() == Type.INT ? Type.NUMERIC : a.arg().type());
        default -> out.add(a.arg().type());
      }
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Running

  /** One run over a range's (or the gateway's) entries of the table. */
  public static final class Run {
    private final Fragment f;
    private final ExecContext ctx = new ExecContext(null, null, new Object[0]);
    private final List<Object[]> rows = new ArrayList<>();
    private final Map<List<Object>, Object[]> groups = new LinkedHashMap<>();
    private SqlException error;

    public Run(Fragment f) {
      this.f = f;
    }

    /** Takes one row entry; false when the run should stop here. */
    public boolean accept(byte[] key, byte[] value) {
      if (error != null) {
        return false;
      }
      try {
        Object[] row = RowCodec.decodeRow(f.table, key, value);
        if (f.filter != null && !Eval.holds(f.filter, row, ctx)) {
          return true;
        }
        if (!f.aggregating()) {
          Object[] kept = new Object[row.length];
          for (int p = 0; p < row.length; p++) {
            kept[p] = f.keep[p] ? row[p] : null;
          }
          rows.add(kept);
          if (f.limit >= 0 && rows.size() >= f.limit) {
            return false;
          }
          return rows.size() < ROW_BUDGET;
        }
        Object[] g = Eval.all(f.groups, row, ctx);
        List<Object> k = new ArrayList<>(g.length);
        for (Object v : g) {
          k.add(Values.hashKey(v));
        }
        Object[] state = groups.get(k);
        if (state == null) {
          state = initial(f, g);
          groups.put(k, state);
        }
        add(f, state, row, ctx);
        return true;
      } catch (SqlException e) {
        error = e; // reported to the gateway, which raises it as the query's error
        return false;
      }
    }

    /** Whether a limit is reached: the span need not be read further. */
    public boolean satisfied() {
      return error == null && !f.aggregating() && f.limit >= 0 && rows.size() >= f.limit;
    }

    /** The rows (or partial states) so far. */
    public List<Object[]> rows() {
      if (error != null) {
        throw error;
      }
      if (!f.aggregating()) {
        return rows;
      }
      List<Object[]> out = new ArrayList<>(groups.values());
      if (out.isEmpty() && f.groups.isEmpty()) {
        out.add(initial(f, new Object[0])); // no GROUP BY: a row even for no input
      }
      return out;
    }

    /** The encoded result: rows, or the error that stopped the run. */
    public byte[] finish() {
      KeyWriter w = new KeyWriter(256);
      if (error != null) {
        w.writeLong(-1).writeString(error.state()).writeString(String.valueOf(error.getMessage()));
        return w.toByteArray();
      }
      List<Object[]> out = rows();
      List<Type> types = f.outputTypes();
      w.writeLong(out.size());
      for (Object[] r : out) {
        for (int c = 0; c < r.length; c++) {
          FragmentCodec.writeValue(w, types.get(c), r[c]);
        }
      }
      return w.toByteArray();
    }
  }

  /** Decodes a result produced by {@link Run#finish}; rethrows a run's error. */
  public List<Object[]> decodeResult(byte[] result) {
    KeyReader r = new KeyReader(result);
    long n = r.readLong();
    if (n < 0) {
      String state = r.readString();
      throw new SqlException(state, r.readString());
    }
    List<Type> types = outputTypes();
    List<Object[]> out = new ArrayList<>((int) n);
    for (long k = 0; k < n; k++) {
      Object[] row = new Object[types.size()];
      for (int c = 0; c < row.length; c++) {
        row[c] = FragmentCodec.readValue(r, types.get(c));
      }
      out.add(row);
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Partial and final aggregation

  private static Object[] initial(Fragment f, Object[] groupValues) {
    Object[] s = Arrays.copyOf(groupValues, f.outputTypes().size());
    int c = groupValues.length;
    for (Scalar.AggCall a : f.aggs) {
      switch (a.function()) {
        case "count" -> s[c++] = 0L;
        case "avg" -> {
          s[c++] = null;
          s[c++] = 0L;
        }
        default -> s[c++] = null;
      }
    }
    return s;
  }

  private static void add(Fragment f, Object[] s, Object[] row, ExecContext ctx) {
    int c = f.groups.size();
    for (Scalar.AggCall a : f.aggs) {
      Object v = a.arg() == null ? Boolean.TRUE : Eval.eval(a.arg(), row, ctx);
      switch (a.function()) {
        case "count" -> {
          if (v != null) {
            s[c] = (Long) s[c] + 1;
          }
          c++;
        }
        case "sum" -> {
          if (v != null) {
            Object x = v instanceof Long l ? BigDecimal.valueOf(l) : v;
            s[c] = s[c] == null ? x : Values.add(s[c], x);
          }
          c++;
        }
        case "avg" -> {
          if (v != null) {
            Object x = v instanceof Long l ? BigDecimal.valueOf(l) : v;
            s[c] = s[c] == null ? x : Values.add(s[c], x);
            s[c + 1] = (Long) s[c + 1] + 1;
          }
          c += 2;
        }
        default -> {
          if (v != null) {
            int cmp = s[c] == null ? 0 : Values.compare(v, s[c]);
            if (s[c] == null || (a.function().equals("max") ? cmp > 0 : cmp < 0)) {
              s[c] = v;
            }
          }
          c++;
        }
      }
    }
  }

  /** Merges partial states (rows of {@link #outputTypes()}) into the aggregates' final values. */
  public static List<Object[]> merge(
      int groupCount, List<Scalar.AggCall> aggs, List<Object[]> partials) {
    Map<List<Object>, Object[]> merged = new LinkedHashMap<>();
    for (Object[] p : partials) {
      List<Object> k = new ArrayList<>(groupCount);
      for (int g = 0; g < groupCount; g++) {
        k.add(Values.hashKey(p[g]));
      }
      Object[] m = merged.get(k);
      if (m == null) {
        merged.put(k, p.clone());
        continue;
      }
      int c = groupCount;
      for (Scalar.AggCall a : aggs) {
        switch (a.function()) {
          case "count" -> {
            m[c] = (Long) m[c] + (Long) p[c];
            c++;
          }
          case "avg" -> {
            m[c] = m[c] == null ? p[c] : (p[c] == null ? m[c] : Values.add(m[c], p[c]));
            m[c + 1] = (Long) m[c + 1] + (Long) p[c + 1];
            c += 2;
          }
          case "sum" -> {
            m[c] = m[c] == null ? p[c] : (p[c] == null ? m[c] : Values.add(m[c], p[c]));
            c++;
          }
          default -> {
            if (p[c] != null) {
              int cmp = m[c] == null ? 0 : Values.compare(p[c], m[c]);
              if (m[c] == null || (a.function().equals("max") ? cmp > 0 : cmp < 0)) {
                m[c] = p[c];
              }
            }
            c++;
          }
        }
      }
    }
    if (merged.isEmpty() && groupCount == 0) {
      Object[] none = new Object[groupCount + aggs.size() * 2];
      int c = 0;
      for (Scalar.AggCall a : aggs) {
        none[c++] = a.function().equals("count") ? 0L : null;
        if (a.function().equals("avg")) {
          none[c++] = 0L;
        }
      }
      merged.put(List.of(), none);
    }
    // Final values: avg divides, sum of integers goes back to an integer.
    List<Object[]> out = new ArrayList<>();
    for (Object[] m : merged.values()) {
      Object[] row = new Object[groupCount + aggs.size()];
      System.arraycopy(m, 0, row, 0, groupCount);
      int c = groupCount;
      for (int i = 0; i < aggs.size(); i++) {
        Scalar.AggCall a = aggs.get(i);
        Object v = m[c];
        switch (a.function()) {
          case "avg" -> {
            long n = (Long) m[c + 1];
            if (n == 0 || v == null) {
              v = null;
            } else if (v instanceof Double d) {
              v = d / n;
            } else {
              v = Values.divideNumeric((BigDecimal) v, BigDecimal.valueOf(n));
            }
            c += 2;
          }
          case "sum" -> {
            if (v != null && a.type() == Type.INT) {
              v = Values.cast(v, Type.NUMERIC, TypeSpec.of(Type.INT));
            }
            c++;
          }
          default -> c++;
        }
        row[groupCount + i] = v;
      }
      out.add(row);
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Encoding

  public byte[] encode() {
    KeyWriter w = new KeyWriter(512);
    w.writeLong(FORMAT);
    w.writeBytes(Catalog.encode(table));
    FragmentCodec.writeScalarOrNull(w, filter);
    w.writeLong(keep.length);
    for (boolean k : keep) {
      w.writeBoolean(k);
    }
    w.writeLong(groups.size());
    for (Scalar g : groups) {
      FragmentCodec.writeScalar(w, g);
    }
    w.writeLong(aggs.size());
    for (Scalar.AggCall a : aggs) {
      w.writeString(a.function()).writeLong(a.type().ordinal());
      FragmentCodec.writeScalarOrNull(w, a.arg());
    }
    w.writeLong(limit);
    return w.toByteArray();
  }

  public static Fragment decode(byte[] bytes) {
    KeyReader r = new KeyReader(bytes);
    if (r.readLong() != FORMAT) {
      throw new IllegalArgumentException("unknown fragment format");
    }
    TableDesc table = Catalog.decode(r.readBytes());
    Scalar filter = FragmentCodec.readScalarOrNull(r);
    boolean[] keep = new boolean[(int) r.readLong()];
    for (int k = 0; k < keep.length; k++) {
      keep[k] = r.readBoolean();
    }
    List<Scalar> groups = new ArrayList<>();
    for (long n = r.readLong(); n > 0; n--) {
      groups.add(FragmentCodec.readScalar(r));
    }
    List<Scalar.AggCall> aggs = new ArrayList<>();
    for (long n = r.readLong(); n > 0; n--) {
      String fn = r.readString();
      Type type = Type.values()[(int) r.readLong()];
      Scalar arg = FragmentCodec.readScalarOrNull(r);
      aggs.add(new Scalar.AggCall(fn, arg, false, type));
    }
    long limit = r.readLong();
    return new Fragment(table, filter, keep, groups, aggs, limit);
  }
}

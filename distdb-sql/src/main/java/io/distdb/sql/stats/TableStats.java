package io.distdb.sql.stats;

import io.distdb.common.encoding.KeyReader;
import io.distdb.common.encoding.KeyWriter;
import io.distdb.sql.exec.FragmentCodec;
import io.distdb.sql.types.Type;
import io.distdb.sql.types.Values;
import java.util.ArrayList;
import java.util.List;

/**
 * What {@code ANALYZE} learned about a table: its row count, how many ranges hold it, and per
 * column the NULLs, the distinct values (HyperLogLog) and an equi-depth histogram (bounds such that
 * each bucket holds the same number of rows).
 */
public record TableStats(long rows, int ranges, List<ColumnStats> columns) {

  private static final int FORMAT = 1;

  /** A column's statistics; {@code bounds} sorted, the first the minimum, the last the maximum. */
  public record ColumnStats(
      int columnId, Type type, long nulls, long distinct, List<Object> bounds) {

    public double nullFraction(long rows) {
      return rows == 0 ? 0 : (double) nulls / rows;
    }

    /** The fraction of non-NULL values below {@code v} (or at most {@code v}), from the bounds. */
    public double fractionBelow(Object v, boolean inclusive) {
      if (bounds.isEmpty()) {
        return 1.0 / 3;
      }
      int n = bounds.size();
      if (Values.compare(v, bounds.get(0)) < 0) {
        return 0;
      }
      if (Values.compare(v, bounds.get(n - 1)) > 0) {
        return 1;
      }
      if (n == 1) {
        return inclusive ? 1 : 0;
      }
      // The bucket v falls in, and a linear guess inside it.
      int below = 0;
      for (int i = 0; i < n; i++) {
        int c = Values.compare(bounds.get(i), v);
        if (c < 0 || (inclusive && c == 0)) {
          below = i;
        }
      }
      return Math.min(1, (below + 0.5) / (n - 1));
    }
  }

  public ColumnStats column(int columnId) {
    for (ColumnStats c : columns) {
      if (c.columnId() == columnId) {
        return c;
      }
    }
    return null;
  }

  public byte[] encode() {
    KeyWriter w = new KeyWriter(256);
    w.writeLong(FORMAT).writeLong(rows).writeLong(ranges).writeLong(columns.size());
    for (ColumnStats c : columns) {
      w.writeLong(c.columnId())
          .writeLong(c.type().ordinal())
          .writeLong(c.nulls())
          .writeLong(c.distinct())
          .writeLong(c.bounds().size());
      for (Object b : c.bounds()) {
        FragmentCodec.writeValue(w, c.type(), b);
      }
    }
    return w.toByteArray();
  }

  public static TableStats decode(byte[] bytes) {
    KeyReader r = new KeyReader(bytes);
    if (r.readLong() != FORMAT) {
      throw new IllegalArgumentException("unknown statistics format");
    }
    long rows = r.readLong();
    int ranges = (int) r.readLong();
    int n = (int) r.readLong();
    List<ColumnStats> cols = new ArrayList<>(n);
    for (int k = 0; k < n; k++) {
      int id = (int) r.readLong();
      Type type = Type.values()[(int) r.readLong()];
      long nulls = r.readLong();
      long distinct = r.readLong();
      int nb = (int) r.readLong();
      List<Object> bounds = new ArrayList<>(nb);
      for (int b = 0; b < nb; b++) {
        bounds.add(FragmentCodec.readValue(r, type));
      }
      cols.add(new ColumnStats(id, type, nulls, distinct, bounds));
    }
    return new TableStats(rows, ranges, cols);
  }
}

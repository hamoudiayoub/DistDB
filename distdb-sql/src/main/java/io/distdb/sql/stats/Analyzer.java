package io.distdb.sql.stats;

import io.distdb.common.Bytes;
import io.distdb.common.encoding.KeyWriter;
import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.encoding.RowCodec;
import io.distdb.sql.kv.KvTxn;
import io.distdb.sql.types.Values;
import java.util.ArrayList;
import java.util.List;

/** Computes a table's statistics by reading it once (in one transaction's snapshot). */
public final class Analyzer {

  /** Histogram buckets per column. */
  static final int BUCKETS = 32;

  private Analyzer() {}

  public static TableStats analyze(KvTxn txn, TableDesc t, int ranges) {
    int width = t.columns().size();
    long rows = 0;
    long[] nulls = new long[width];
    HyperLogLog[] sketches = new HyperLogLog[width];
    List<List<Object>> values = new ArrayList<>();
    for (int c = 0; c < width; c++) {
      sketches[c] = new HyperLogLog();
      values.add(new ArrayList<>());
    }
    byte[] start = RowCodec.indexPrefix(t.id(), TableDesc.PRIMARY_INDEX_ID);
    byte[] end = Bytes.prefixEnd(start).orElseThrow();
    while (true) {
      List<KvTxn.Entry> page = txn.scan(start, end, 1024);
      for (KvTxn.Entry e : page) {
        Object[] row = RowCodec.decodeRow(t, e.key(), e.value());
        rows++;
        for (int c = 0; c < width; c++) {
          if (row[c] == null) {
            nulls[c]++;
            continue;
          }
          KeyWriter w = new KeyWriter(16);
          RowCodec.writeKey(w, t.columns().get(c).type().type(), Values.hashKey(row[c]));
          sketches[c].add(HyperLogLog.hash(w.toByteArray()));
          values.get(c).add(row[c]);
        }
      }
      if (page.size() < 1024) {
        break;
      }
      start = Bytes.successor(page.get(page.size() - 1).key());
    }
    List<TableStats.ColumnStats> cols = new ArrayList<>();
    for (int c = 0; c < width; c++) {
      List<Object> vs = values.get(c);
      vs.sort(Values::compare);
      List<Object> bounds = new ArrayList<>();
      int n = vs.size();
      int b = Math.min(BUCKETS + 1, n);
      for (int i = 0; i < b; i++) {
        bounds.add(vs.get(b == 1 ? 0 : (int) ((long) i * (n - 1) / (b - 1))));
      }
      long distinct = Math.min(Math.max(sketches[c].estimate(), n == 0 ? 0 : 1), n);
      cols.add(
          new TableStats.ColumnStats(
              t.columns().get(c).id(),
              t.columns().get(c).type().type(),
              nulls[c],
              distinct,
              bounds));
    }
    return new TableStats(rows, ranges, cols);
  }
}

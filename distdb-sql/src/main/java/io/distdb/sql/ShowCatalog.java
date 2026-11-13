package io.distdb.sql;

import io.distdb.common.Bytes;
import io.distdb.common.encoding.KeyReader;
import io.distdb.sql.catalog.Catalog;
import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.encoding.RowCodec;
import io.distdb.sql.kv.KvStore;
import io.distdb.sql.parse.Ast;
import io.distdb.sql.stats.TableStats;
import io.distdb.sql.types.Type;
import io.distdb.sql.types.Values;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@code SHOW TABLES}, {@code COLUMNS}, {@code INDEXES}, {@code STATISTICS}, {@code RANGES} and
 * {@code NODES}: the catalog and the cluster's layout as ordinary result rows, for the shell's meta
 * commands and for any PostgreSQL client.
 */
final class ShowCatalog {

  private ShowCatalog() {}

  private static Result.Column col(String name, Type type) {
    return new Result.Column(name, type);
  }

  static List<Result.Column> columns(Ast.ShowCatalog.What what) {
    return switch (what) {
      case TABLES ->
          List.of(
              col("table_name", Type.TEXT),
              col("columns", Type.INT),
              col("indexes", Type.INT),
              col("estimated_rows", Type.INT),
              col("ranges", Type.INT));
      case COLUMNS ->
          List.of(
              col("column_name", Type.TEXT),
              col("data_type", Type.TEXT),
              col("nullable", Type.BOOL),
              col("default", Type.TEXT),
              col("primary_key", Type.BOOL));
      case INDEXES ->
          List.of(
              col("index_name", Type.TEXT),
              col("columns", Type.TEXT),
              col("unique", Type.BOOL),
              col("primary", Type.BOOL),
              col("state", Type.TEXT));
      case STATISTICS ->
          List.of(
              col("column_name", Type.TEXT),
              col("rows", Type.INT),
              col("distinct", Type.INT),
              col("null_fraction", Type.FLOAT),
              col("min", Type.TEXT),
              col("max", Type.TEXT),
              col("histogram_buckets", Type.INT));
      case RANGES ->
          List.of(
              col("range_id", Type.INT),
              col("start_key", Type.TEXT),
              col("end_key", Type.TEXT),
              col("leader", Type.INT),
              col("voters", Type.TEXT),
              col("learners", Type.TEXT));
      case NODES ->
          List.of(
              col("node_id", Type.INT),
              col("address", Type.TEXT),
              col("alive", Type.BOOL),
              col("replicas", Type.INT),
              col("leaders", Type.INT));
    };
  }

  static Result run(Catalog.Snapshot snapshot, KvStore store, Ast.ShowCatalog s) {
    List<Object[]> rows = new ArrayList<>();
    List<String> notices = new ArrayList<>();
    switch (s.what()) {
      case TABLES -> {
        List<TableDesc> tables = new ArrayList<>(snapshot.tables().values());
        tables.sort(Comparator.comparing(TableDesc::name));
        for (TableDesc t : tables) {
          TableStats stats = snapshot.statsOf(t);
          byte[] start = RowCodec.tablePrefix(t.id());
          rows.add(
              new Object[] {
                t.name(),
                (long) visible(t).size(),
                (long) t.indexes().size() + 1,
                stats == null ? null : stats.rows(),
                (long) store.rangeCount(start, Bytes.prefixEnd(start).orElseThrow())
              });
        }
      }
      case COLUMNS -> {
        TableDesc t = snapshot.table(s.table());
        for (TableDesc.Column c : visible(t)) {
          rows.add(
              new Object[] {
                c.name(),
                c.type().sqlName(),
                !c.notNull(),
                c.defaultSql(),
                t.primary().columnIds().contains(c.id())
              });
        }
      }
      case INDEXES -> {
        TableDesc t = snapshot.table(s.table());
        List<TableDesc.Index> all = new ArrayList<>();
        all.add(t.primary());
        all.addAll(t.indexes());
        for (TableDesc.Index i : all) {
          List<String> names = new ArrayList<>();
          for (int id : i.columnIds()) {
            names.add(t.columns().get(t.position(id)).name());
          }
          rows.add(
              new Object[] {
                i.name(),
                String.join(", ", names),
                i.unique(),
                i.id() == TableDesc.PRIMARY_INDEX_ID,
                i.state() == TableDesc.IndexState.PUBLIC ? "public" : "write only (building)"
              });
        }
      }
      case STATISTICS -> {
        TableDesc t = snapshot.table(s.table());
        TableStats stats = snapshot.statsOf(t);
        if (stats == null) {
          notices.add("no statistics for \"" + t.name() + "\": run ANALYZE " + t.name());
        } else {
          for (TableDesc.Column c : visible(t)) {
            TableStats.ColumnStats cs = stats.column(c.id());
            if (cs == null) {
              continue;
            }
            List<Object> b = cs.bounds();
            rows.add(
                new Object[] {
                  c.name(),
                  stats.rows(),
                  cs.distinct(),
                  cs.nullFraction(stats.rows()),
                  b.isEmpty() ? null : Values.toText(b.getFirst(), cs.type()),
                  b.isEmpty() ? null : Values.toText(b.getLast(), cs.type()),
                  (long) Math.max(0, b.size() - 1)
                });
          }
        }
      }
      case RANGES -> {
        KvStore.ClusterView view = store.cluster();
        if (view.ranges().isEmpty()) {
          notices.add("this node is not part of a cluster: everything is in one local store");
        }
        byte[] lo = null;
        byte[] hi = null;
        if (s.table() != null) {
          lo = RowCodec.tablePrefix(snapshot.table(s.table()).id());
          hi = Bytes.prefixEnd(lo).orElseThrow();
        }
        Map<Long, String> names = names(snapshot);
        List<KvStore.RangeRow> ranges = new ArrayList<>(view.ranges());
        ranges.sort((a, b) -> Arrays.compareUnsigned(a.start(), b.start()));
        for (KvStore.RangeRow r : ranges) {
          if (lo != null
              && ((r.end() != null && Arrays.compareUnsigned(r.end(), lo) <= 0)
                  || Arrays.compareUnsigned(r.start(), hi) >= 0)) {
            continue;
          }
          rows.add(
              new Object[] {
                r.id(),
                r.start().length == 0 ? "/min" : key(r.start(), names),
                r.end() == null ? "/max" : key(r.end(), names),
                r.leader() == 0 ? null : (long) r.leader(),
                ids(r.voters()),
                ids(r.learners())
              });
        }
      }
      case NODES -> {
        KvStore.ClusterView view = store.cluster();
        if (view.nodes().isEmpty()) {
          notices.add("this node is not part of a cluster");
        }
        Map<Integer, long[]> counts = new HashMap<>();
        for (KvStore.RangeRow r : view.ranges()) {
          for (int v : r.voters()) {
            counts.computeIfAbsent(v, k -> new long[2])[0]++;
          }
          for (int l : r.learners()) {
            counts.computeIfAbsent(l, k -> new long[2])[0]++;
          }
          if (r.leader() != 0) {
            counts.computeIfAbsent(r.leader(), k -> new long[2])[1]++;
          }
        }
        Map<Integer, KvStore.NodeRow> byId = new TreeMap<>();
        view.nodes().forEach(n -> byId.put(n.id(), n));
        for (KvStore.NodeRow n : byId.values()) {
          long[] c = counts.getOrDefault(n.id(), new long[2]);
          rows.add(new Object[] {(long) n.id(), n.address(), n.alive(), c[0], c[1]});
        }
      }
    }
    return new Result("SHOW", columns(s.what()), rows, notices);
  }

  private static List<TableDesc.Column> visible(TableDesc t) {
    return t.columns().stream().filter(c -> !c.hidden()).toList();
  }

  private static String ids(List<Integer> ids) {
    return ids.isEmpty() ? "" : String.join(", ", ids.stream().map(i -> "n" + i).toList());
  }

  private static Map<Long, String> names(Catalog.Snapshot snapshot) {
    Map<Long, String> names = new HashMap<>();
    snapshot.tables().values().forEach(t -> names.put(t.id(), t.name()));
    return names;
  }

  /**
   * A key as text, its leading table id replaced by the table's name ({@code /1} is the system).
   */
  static String key(byte[] key, Map<Long, String> names) {
    String text = KeyReader.toDebugString(key);
    int end = text.indexOf('/', 1);
    String head = end < 0 ? text.substring(1) : text.substring(1, end);
    String rest = end < 0 ? "" : text.substring(end);
    try {
      long id = Long.parseLong(head);
      if (id == 1) {
        return "/system" + rest;
      }
      String name = names.get(id);
      return name == null ? text : "/" + name + rest;
    } catch (NumberFormatException e) {
      return text;
    }
  }
}

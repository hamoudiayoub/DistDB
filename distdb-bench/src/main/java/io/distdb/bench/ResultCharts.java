package io.distdb.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

/**
 * Draws every chart from the JSON results in a directory, so that the charts can always be redrawn
 * from the data ({@code distdb-bench charts --out DIR}); the suites call it after each run.
 */
final class ResultCharts {

  private ResultCharts() {}

  static void render(Path dir) throws IOException {
    Map<String, Object> engine = load(dir.resolve("engine.json"));
    if (engine != null) {
      write(dir, "engine-group-commit", engine(engine));
    }
    Map<String, Object> kv = load(dir.resolve("kv.json"));
    if (kv != null) {
      write(dir, "kv-throughput", kvThroughput(kv));
      write(dir, "kv-latency", kvLatency(kv));
    }
    Map<String, Object> recovery = load(dir.resolve("recovery.json"));
    if (recovery != null) {
      write(dir, "recovery", recovery(recovery));
    }
    Map<String, Object> ablation = load(dir.resolve("ablation.json"));
    if (ablation != null && ablation.get("gain") != null) {
      write(dir, "ablation", ablation(ablation));
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> load(Path p) throws IOException {
    if (!Files.exists(p)) {
      return null;
    }
    return (Map<String, Object>) Json.parse(Files.readString(p, StandardCharsets.UTF_8));
  }

  private static void write(Path dir, String name, String svg) throws IOException {
    Files.writeString(dir.resolve(name + ".svg"), svg, StandardCharsets.UTF_8);
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> runs(Map<String, Object> r) {
    return (List<Map<String, Object>>) r.get("runs");
  }

  private static double num(Object o) {
    return o == null ? Double.NaN : ((Number) o).doubleValue();
  }

  static String engine(Map<String, Object> r) {
    TreeSet<Integer> threads = new TreeSet<>();
    for (Map<String, Object> run : runs(r)) {
      threads.add((int) num(run.get("threads")));
    }
    double[] xs = threads.stream().mapToDouble(Integer::doubleValue).toArray();
    double[][] ys = new double[2][xs.length];
    for (Map<String, Object> run : runs(r)) {
      int s = Boolean.TRUE.equals(run.get("group_commit")) ? 1 : 0;
      int i = threads.headSet((int) num(run.get("threads"))).size();
      ys[s][i] = num(run.get("throughput_per_s"));
    }
    return Charts.lines(
        "Storage engine: YCSB-A with fsynced writes",
        "Throughput by writer threads, group commit off and on (50% reads, 50% fsynced updates)",
        xs,
        List.of("group commit off", "group commit on"),
        ys,
        "writer threads",
        "ops/s",
        Double.NaN,
        null,
        (String) r.get("hardware"));
  }

  private static List<Integer> nodes(Map<String, Object> r) {
    TreeSet<Integer> n = new TreeSet<>();
    for (Map<String, Object> run : runs(r)) {
      n.add((int) num(run.get("nodes")));
    }
    return new ArrayList<>(n);
  }

  private static List<String> groups(List<Integer> nodes) {
    List<String> g = new ArrayList<>();
    for (int n : nodes) {
      g.add(n + (n == 1 ? " data node" : " data nodes"));
    }
    return g;
  }

  static String kvThroughput(Map<String, Object> r) {
    List<Integer> nodes = nodes(r);
    List<String> names = List.of("A (50/50)", "B (95/5)", "C (reads)");
    double[][] v = new double[3][nodes.size()];
    for (Map<String, Object> run : runs(r)) {
      if (!String.valueOf(run.get("loop")).startsWith("closed")) {
        continue;
      }
      int w = names.indexOf((String) run.get("workload"));
      v[w][nodes.indexOf((int) num(run.get("nodes")))] = num(run.get("throughput_per_s"));
    }
    return Charts.columns(
        "KV layer: YCSB throughput by cluster size",
        "Closed loop, 32 clients, Zipfian keys, 3 replicas per range (1 with one node); all on one"
            + " machine",
        groups(nodes),
        names,
        v,
        "ops/s",
        (String) r.get("hardware"));
  }

  static String kvLatency(Map<String, Object> r) {
    List<Integer> nodes = nodes(r);
    double[][] v = new double[2][nodes.size()];
    String rate = "";
    for (Map<String, Object> run : runs(r)) {
      String loop = String.valueOf(run.get("loop"));
      if (!loop.startsWith("open")) {
        continue;
      }
      rate = loop.substring(loop.indexOf(',') + 1).trim();
      int i = nodes.indexOf((int) num(run.get("nodes")));
      v[0][i] = num(run.get("p50_ms"));
      v[1][i] = num(run.get("p99_ms"));
    }
    return Charts.columns(
        "KV layer: latency at a fixed " + rate.replace(".0 ", " ") + " (YCSB-A)",
        "Open loop: each request's latency counts from its scheduled start (no coordinated"
            + " omission)",
        groups(nodes),
        List.of("p50", "p99"),
        v,
        "ms",
        (String) r.get("hardware"));
  }

  @SuppressWarnings("unchecked")
  static String recovery(Map<String, Object> r) {
    List<Object> ticks = (List<Object>) r.get("writes_per_100ms");
    // Writes complete in bursts; a 500 ms moving average shows the dip the metric measures.
    int window = 5;
    int n = ticks.size() - window;
    double[] xs = new double[n];
    double[][] ys = new double[1][n];
    for (int i = 0; i < n; i++) {
      double sum = 0;
      for (int k = i; k < i + window; k++) {
        sum += num(ticks.get(k));
      }
      xs[i] = (i + window) * 0.1;
      ys[0][i] = sum * 10 / window;
    }
    double killAt = r.get("kill_at_s") == null ? 5 : num(r.get("kill_at_s"));
    return Charts.lines(
        "Recovery: kill -9 of the node leading the most ranges",
        String.format(
            Locale.ROOT,
            "Writes completed per second (500 ms moving average), open loop at %.0f/s:"
                + " below half for %.0f ms, 90%% after %.0f ms",
            num(r.get("rate_per_s")),
            num(r.get("unavailable_ms")),
            num(r.get("back_to_90pct_ms"))),
        xs,
        List.of("writes completed (500 ms average)"),
        ys,
        "seconds",
        "writes/s",
        killAt,
        "kill -9 n" + (int) num(r.get("killed_node")),
        (String) r.get("hardware"));
  }

  @SuppressWarnings("unchecked")
  static String ablation(Map<String, Object> r) {
    Map<String, Object> gain = (Map<String, Object>) r.get("gain");
    List<String> labels =
        List.of(
            "TSO batching",
            "Coprocessor pushdown",
            "WAL group commit",
            "Raft pipelining",
            "Raft batching");
    List<String> keys =
        List.of("tso_batching", "pushdown", "group_commit", "raft_pipelining", "raft_batching");
    double[] v = new double[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      v[i] = num(gain.get(keys.get(i)));
    }
    return Charts.bars(
        "What each optimization buys, on 3 data nodes",
        "Speed with it on ÷ speed with it off: SQL updates/s (TSO), aggregation latency (pushdown),"
            + " KV writes/s (the rest)",
        labels,
        v,
        "×",
        "one machine · " + r.get("hardware"));
  }
}

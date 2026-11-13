package io.distdb.bench;

import io.distdb.client.KvClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@code distdb-bench SUITE... [--out DIR] [--quick] [--hardware TEXT]}: runs benchmark suites
 * against real processes on this machine and writes JSON results and SVG charts to DIR (default
 * {@code docs/bench}).
 *
 * <p>Suites: {@code engine} (YCSB-A on the storage engine, group commit off and on), {@code kv}
 * (YCSB A/B/C at 1, 3 and 5 data nodes), {@code tpcc} (new-order and payment over JDBC), {@code
 * recovery} (kill -9 a leader under load), {@code ablation} (each optimization off, then on),
 * {@code all}; and {@code chaos} (the Docker chaos suite, see {@link
 * io.distdb.bench.chaos.ChaosSuite}).
 */
public final class BenchMain {

  /** gRPC's own log lines (orphaned channels, Netty frame notes): noise in benchmark output. */
  private static final List<java.util.logging.Logger> QUIET =
      List.of(
          java.util.logging.Logger.getLogger("io.grpc"),
          java.util.logging.Logger.getLogger("io.netty"));

  private final Path out;
  private final boolean quick;
  private final Workloads.Timing timing;
  private final String hardware;
  private final Path work;

  private BenchMain(Path out, boolean quick, String hardware) throws IOException {
    this.out = out;
    this.quick = quick;
    this.timing = quick ? Workloads.Timing.QUICK : Workloads.Timing.FULL;
    this.hardware = hardware != null ? hardware : Env.describe();
    this.work = Files.createTempDirectory("distdb-bench-");
    Files.createDirectories(out);
  }

  public static void main(String[] args) throws Exception {
    List<String> suites = new ArrayList<>();
    Path out = Path.of("docs", "bench");
    boolean quick = false;
    String hardware = null;
    List<String> rest = new ArrayList<>();
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--out" -> out = Path.of(args[++i]);
        case "--quick" -> quick = true;
        case "--hardware" -> hardware = args[++i];
        default -> {
          if (suites.contains("chaos")) {
            rest.add(args[i]);
          } else {
            suites.add(args[i]);
          }
        }
      }
    }
    if (suites.isEmpty()) {
      System.err.println(
          "usage: distdb-bench engine|kv|tpcc|recovery|ablation|all|chaos [--out DIR] [--quick]"
              + " [--hardware TEXT]");
      System.exit(2);
    }
    if (suites.contains("chaos")) {
      System.exit(io.distdb.bench.chaos.ChaosSuite.run(rest));
    }
    if (suites.getFirst().equals("charts")) {
      ResultCharts.render(out);
      System.exit(0);
    }
    if (suites.getFirst().equals("screenshot")) {
      // screenshot IN.ansi OUT.svg [TITLE]: a terminal transcript as an SVG window
      AnsiSvg.main(
          Path.of(suites.get(1)),
          Path.of(suites.get(2)),
          suites.size() > 3 ? suites.get(3) : "distdb sql");
      System.exit(0);
    }
    QUIET.forEach(l -> l.setLevel(java.util.logging.Level.OFF));
    BenchMain b = new BenchMain(out, quick, hardware);
    log("hardware: " + b.hardware);
    boolean all = suites.contains("all");
    if (suites.contains("probe")) {
      b.probe();
    }
    if (all || suites.contains("engine")) {
      b.engine();
    }
    if (all || suites.contains("kv")) {
      b.kv();
    }
    if (all || suites.contains("tpcc")) {
      b.tpcc();
    }
    if (all || suites.contains("recovery")) {
      b.recovery();
    }
    if (all || suites.contains("ablation")) {
      b.ablation();
    }
    log("done; results in " + out.toAbsolutePath());
    System.exit(0);
  }

  static void log(String s) {
    System.out.println("[" + Instant.now().toString().substring(11, 19) + "] " + s);
    System.out.flush();
  }

  private Map<String, Object> header(String suite) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("suite", suite);
    m.put("when", Instant.now().toString());
    m.put("hardware", hardware);
    m.put("quick", quick);
    return m;
  }

  private void write(String name, Map<String, Object> results) throws IOException {
    Files.writeString(
        out.resolve(name + ".json"), Json.write(results) + "\n", StandardCharsets.UTF_8);
    ResultCharts.render(out);
  }

  private void svg(String name, String svg) throws IOException {
    Files.writeString(out.resolve(name + ".svg"), svg, StandardCharsets.UTF_8);
  }

  static Map<String, Object> summary(Runner.Result r) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ops", r.ops());
    m.put("errors", r.errors());
    m.put("seconds", r.seconds());
    m.put("throughput_per_s", round(r.throughput()));
    m.put("p50_ms", round(r.ms(0.5)));
    m.put("p99_ms", round(r.ms(0.99)));
    m.put("p999_ms", round(r.ms(0.999)));
    m.put("max_ms", round(r.latency().maxNanos() / 1e6));
    return m;
  }

  static double round(double v) {
    return Math.round(v * 100) / 100.0;
  }

  private LocalCluster cluster(int nodes, String tune, long splitKeys) throws Exception {
    Path dir = Files.createTempDirectory(work, "cluster-");
    log(
        "starting a cluster: 3 PD + "
            + nodes
            + " data nodes"
            + (tune == null ? "" : " (" + tune + ")"));
    return new LocalCluster(dir, nodes, tune, splitKeys).start();
  }

  // -----------------------------------------------------------------------------------------------

  /** Sequential single-operation latency on a one-node cluster: what one request costs. */
  void probe() throws Exception {
    try (LocalCluster c = cluster(1, null, 100_000);
        KvClient kv = c.kv()) {
      Workloads.loadKv(kv, 1000, 16);
      for (String what : List.of("get", "put")) {
        Histogram h = new Histogram();
        for (int i = 0; i < 300; i++) {
          long t0 = System.nanoTime();
          if (what.equals("get")) {
            kv.get(Workloads.key(i)).join();
          } else {
            kv.put(Workloads.key(i), Workloads.value()).join();
          }
          h.record(System.nanoTime() - t0);
        }
        log(
            String.format(
                Locale.ROOT,
                "probe %s: p50 %.2f ms, p90 %.2f ms, p99 %.2f ms",
                what,
                h.quantile(0.5) / 1e6,
                h.quantile(0.9) / 1e6,
                h.quantile(0.99) / 1e6));
      }
      double sum = c.metric(1, "distdb_commit_latency_seconds_sum");
      double count = c.metric(1, "distdb_commit_latency_seconds_count");
      log(
          String.format(
              Locale.ROOT,
              "probe server-side commit latency: mean %.2f ms over %.0f writes; %s fsyncs for %s"
                  + " engine writes",
              sum / Math.max(1, count) * 1000,
              count,
              c.metric(1, "distdb_engine_wal_syncs_total"),
              c.metric(1, "distdb_engine_writes_total")));
      Runner.Result r =
          Runner.closedAsync(
              32,
              Duration.ofSeconds(1),
              Duration.ofSeconds(4),
              Workloads.kvOp(kv, new Zipf(1000), 1.0));
      log(
          String.format(
              Locale.ROOT,
              "probe 32 clients get: %.0f ops/s p50 %.2f p99 %.2f ms",
              r.throughput(),
              r.ms(0.5),
              r.ms(0.99)));
    }
  }

  void engine() throws Exception {
    Map<String, Object> r = header("engine");
    int[] threads = quick ? new int[] {1, 4} : new int[] {1, 4, 16, 32};
    double[][] tput = new double[2][threads.length];
    List<Object> rows = new ArrayList<>();
    for (int g = 0; g < 2; g++) {
      for (int t = 0; t < threads.length; t++) {
        boolean on = g == 1;
        Runner.Result res =
            Workloads.engine(Files.createTempDirectory(work, "engine-"), threads[t], on, timing);
        tput[g][t] = res.throughput();
        Map<String, Object> row = summary(res);
        row.put("threads", threads[t]);
        row.put("group_commit", on);
        rows.add(row);
        log(
            String.format(
                Locale.ROOT,
                "engine YCSB-A threads=%d group-commit=%s: %.0f ops/s, p99 %.2f ms",
                threads[t],
                on ? "on" : "off",
                res.throughput(),
                res.ms(0.99)));
      }
    }
    r.put("runs", rows);
    write("engine", r);
    double[] xs = new double[threads.length];
    for (int i = 0; i < xs.length; i++) {
      xs[i] = threads[i];
    }
    svg(
        "engine-group-commit",
        Charts.lines(
            "Storage engine: YCSB-A with synced writes",
            "Throughput by writer threads, group commit off and on (50% reads, 50% fsynced updates)",
            xs,
            List.of("group commit off", "group commit on"),
            tput,
            "writer threads",
            "ops/s",
            Double.NaN,
            null,
            hardware));
  }

  void kv() throws Exception {
    Map<String, Object> r = header("kv");
    int[] counts = quick ? new int[] {1, 3} : new int[] {1, 3, 5};
    String[] names = {"A (50/50)", "B (95/5)", "C (reads)"};
    double[] reads = {0.5, 0.95, 1.0};
    double[][] tput = new double[3][counts.length];
    double[][] lat = new double[2][counts.length];
    List<Object> rows = new ArrayList<>();
    for (int n = 0; n < counts.length; n++) {
      try (LocalCluster c = cluster(counts[n], null, quick ? 500 : 6000);
          KvClient kv = c.kv()) {
        Workloads.loadKv(kv, timing.records(), 64);
        Zipf zipf = new Zipf(timing.records());
        for (int w = 0; w < 3; w++) {
          Runner.Result res =
              Runner.closedAsync(
                  32, timing.warmup(), timing.measure(), Workloads.kvOp(kv, zipf, reads[w]));
          tput[w][n] = res.throughput();
          Map<String, Object> row = summary(res);
          row.put("nodes", counts[n]);
          row.put("workload", names[w]);
          row.put("loop", "closed, 32 clients");
          rows.add(row);
          log(
              String.format(
                  Locale.ROOT,
                  "kv %d nodes, YCSB-%s: %.0f ops/s, p99 %.2f ms",
                  counts[n],
                  names[w],
                  res.throughput(),
                  res.ms(0.99)));
        }
        double rate = quick ? 100 : 200;
        Runner.Result open =
            Runner.open(
                rate, timing.warmup(), timing.measure(), 5000, Workloads.kvOp(kv, zipf, 0.5));
        lat[0][n] = open.ms(0.5);
        lat[1][n] = open.ms(0.99);
        Map<String, Object> row = summary(open);
        row.put("nodes", counts[n]);
        row.put("workload", "A (50/50)");
        row.put("loop", "open, " + rate + " ops/s");
        row.put("ranges", c.ranges());
        rows.add(row);
        log(
            String.format(
                Locale.ROOT,
                "kv %d nodes, open loop %.0f/s: p50 %.2f ms, p99 %.2f ms (%d ranges)",
                counts[n],
                rate,
                open.ms(0.5),
                open.ms(0.99),
                c.ranges()));
      }
    }
    r.put("runs", rows);
    write("kv", r);
    List<String> groups = new ArrayList<>();
    for (int n : counts) {
      groups.add(n + (n == 1 ? " data node" : " data nodes"));
    }
    svg(
        "kv-throughput",
        Charts.columns(
            "KV layer: YCSB throughput by cluster size",
            "Closed loop, 32 clients, Zipfian keys, 3 replicas per range (1 with one node); one machine",
            groups,
            List.of(names),
            tput,
            "ops/s",
            hardware));
    svg(
        "kv-latency",
        Charts.columns(
            "KV layer: latency at a fixed 200 ops/s (YCSB-A)",
            "Open loop: latency counts from each request's scheduled start (no coordinated omission)",
            groups,
            List.of("p50", "p99"),
            lat,
            "ms",
            hardware));
  }

  void tpcc() throws Exception {
    Map<String, Object> r = header("tpcc");
    Workloads.TpccScale scale = quick ? Workloads.TpccScale.QUICK : Workloads.TpccScale.FULL;
    try (LocalCluster c = cluster(3, null, 4000)) {
      Workloads.TpccResult t =
          Workloads.tpcc(
              c,
              scale,
              quick ? 4 : 10 * scale.warehouses(),
              quick
                  ? timing
                  : new Workloads.Timing(Duration.ofSeconds(5), Duration.ofSeconds(30), 0));
      r.put("warehouses", scale.warehouses());
      r.put("customers_per_district", scale.customersPerDistrict());
      r.put("items", scale.items());
      r.put("terminals", quick ? 4 : 10 * scale.warehouses());
      r.put("new_order", summary(t.newOrder()));
      r.put("payment", summary(t.payment()));
      r.put("tpmC", round(t.tpmC()));
      r.put("retries_after_40001", t.retries());
      r.put("gave_up", t.gaveUp());
      r.put("consistency", t.consistency());
      log(
          String.format(
              Locale.ROOT,
              "tpcc: %.0f new-orders/min (tpmC), new-order p50 %.1f ms p99 %.1f ms,"
                  + " payment p50 %.1f ms p99 %.1f ms, %d retries; %s",
              t.tpmC(),
              t.newOrder().ms(0.5),
              t.newOrder().ms(0.99),
              t.payment().ms(0.5),
              t.payment().ms(0.99),
              t.retries(),
              t.consistency()));
    }
    write("tpcc", r);
  }

  void recovery() throws Exception {
    Map<String, Object> r = header("recovery");
    try (LocalCluster c = cluster(3, null, 1000)) {
      Workloads.RecoveryResult res =
          Workloads.recovery(
              c, quick ? 100 : 150, Duration.ofSeconds(5), Duration.ofSeconds(quick ? 6 : 15));
      r.put("killed_node", res.killedNode());
      r.put("kill_at_s", 5);
      r.put("rate_per_s", quick ? 100 : 150);
      r.put("unavailable_ms", res.unavailableMs());
      r.put("back_to_90pct_ms", res.backTo90Ms());
      r.put("max_latency_ms", res.maxLatencyMs());
      r.put("run", summary(res.run()));
      long[] ticks = res.run().perTick();
      List<Long> perTick = new ArrayList<>();
      for (long t : ticks) {
        perTick.add(t);
      }
      r.put("writes_per_100ms", perTick);
      log(
          String.format(
              Locale.ROOT,
              "recovery: killed n%d; below half throughput for %.0f ms, back to 90%% after"
                  + " %.0f ms, slowest write %d ms",
              res.killedNode(),
              res.unavailableMs(),
              res.backTo90Ms(),
              res.maxLatencyMs()));
      int n = ticks.length - 1;
      double[] xs = new double[n];
      double[][] ys = new double[1][n];
      for (int i = 0; i < n; i++) {
        xs[i] = i * 0.1;
        ys[0][i] = ticks[i] * 10.0;
      }
      svg(
          "recovery",
          Charts.lines(
              "Recovery: kill -9 of the node leading the most ranges",
              String.format(
                  Locale.ROOT,
                  "Writes completed per second (100 ms buckets), open loop at %d/s;"
                      + " below half for %.0f ms, back to 90%% after %.0f ms",
                  quick ? 100 : 150,
                  res.unavailableMs(),
                  res.backTo90Ms()),
              xs,
              List.of("writes completed"),
              ys,
              "seconds",
              "writes/s",
              res.killTick() * 0.1,
              "kill -9 n" + res.killedNode(),
              hardware));
    }
    write("recovery", r);
  }

  void ablation() throws Exception {
    Map<String, Object> r = header("ablation");
    int writers = 64;
    Map<String, Runner.Result> kvWrites = new LinkedHashMap<>();
    Map<String, Runner.Result> sqlWrites = new LinkedHashMap<>();
    List<Workloads.PushdownResult> push;
    int counters = 2000;
    String[] configs = {
      null, "raft.batch=off", "raft.pipeline=off", "wal.group-commit=off", "tso.batch=off"
    };
    push = List.of();
    for (String tune : configs) {
      try (LocalCluster c = cluster(3, tune, quick ? 500 : 3000)) {
        boolean kvRun = tune == null || !tune.startsWith("tso");
        boolean sqlRun = tune == null || tune.startsWith("tso");
        if (kvRun) {
          try (KvClient kv = c.kv()) {
            Workloads.loadKv(kv, timing.records() / 2, 64);
            Zipf zipf = new Zipf(timing.records() / 2);
            Runner.Result res =
                Runner.closedAsync(
                    writers, timing.warmup(), timing.measure(), Workloads.kvOp(kv, zipf, 0.0));
            kvWrites.put(tune == null ? "all on" : tune, res);
            log(
                String.format(
                    Locale.ROOT,
                    "ablation %s: KV writes %.0f ops/s, p99 %.2f ms",
                    tune == null ? "all on" : tune,
                    res.throughput(),
                    res.ms(0.99)));
          }
        }
        if (sqlRun) {
          try (java.sql.Connection conn = c.jdbc(1)) {
            Workloads.loadSqlCounters(conn, counters);
          }
          Runner.Result res = Workloads.sqlUpdates(c, 32, counters, timing);
          sqlWrites.put(tune == null ? "all on" : tune, res);
          log(
              String.format(
                  Locale.ROOT,
                  "ablation %s: SQL updates %.0f/s, p99 %.2f ms",
                  tune == null ? "all on" : tune,
                  res.throughput(),
                  res.ms(0.99)));
        }
        if (tune == null) {
          try (java.sql.Connection conn = c.jdbc(1)) {
            Workloads.loadSales(conn, quick ? 2000 : 20000);
          }
          push = Workloads.pushdown(c, quick ? 3 : 7);
          for (Workloads.PushdownResult p : push) {
            log(
                String.format(
                    Locale.ROOT,
                    "ablation pushdown %s: %.1f ms -> %.1f ms, %.0f -> %.0f bytes",
                    p.query(),
                    p.msOff(),
                    p.msOn(),
                    p.bytesOff(),
                    p.bytesOn()));
          }
        }
      }
    }
    Map<String, Object> kvMap = new LinkedHashMap<>();
    kvWrites.forEach((k, v) -> kvMap.put(k, summary(v)));
    Map<String, Object> sqlMap = new LinkedHashMap<>();
    sqlWrites.forEach((k, v) -> sqlMap.put(k, summary(v)));
    List<Object> pushRows = new ArrayList<>();
    for (Workloads.PushdownResult p : push) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("query", p.query());
      m.put("ms_off", round(p.msOff()));
      m.put("ms_on", round(p.msOn()));
      m.put("gateway_bytes_off", round(p.bytesOff()));
      m.put("gateway_bytes_on", round(p.bytesOn()));
      pushRows.add(m);
    }
    r.put("kv_writes_64_clients", kvMap);
    r.put("sql_point_updates_32_connections", sqlMap);
    r.put("pushdown", pushRows);
    write("ablation", r);
    double base = kvWrites.get("all on").throughput();
    double sqlBase = sqlWrites.get("all on").throughput();
    double pushOff = 0;
    double pushOn = 0;
    for (Workloads.PushdownResult p : push) {
      pushOff += p.msOff();
      pushOn += p.msOn();
    }
    List<String> labels =
        List.of(
            "Raft batching",
            "Raft pipelining",
            "WAL group commit",
            "TSO batching",
            "Coprocessor pushdown");
    double[] gain = {
      base / kvWrites.get("raft.batch=off").throughput(),
      base / kvWrites.get("raft.pipeline=off").throughput(),
      base / kvWrites.get("wal.group-commit=off").throughput(),
      sqlBase / sqlWrites.get("tso.batch=off").throughput(),
      pushOn == 0 ? 1 : pushOff / pushOn
    };
    r.put(
        "gain",
        Map.of(
            "raft_batching",
            round(gain[0]),
            "raft_pipelining",
            round(gain[1]),
            "group_commit",
            round(gain[2]),
            "tso_batching",
            round(gain[3]),
            "pushdown",
            round(gain[4])));
    write("ablation", r);
    svg(
        "ablation",
        Charts.bars(
            "What each optimization buys",
            "Speed with it on ÷ speed with it off: KV write throughput (64 clients), SQL update"
                + " throughput (TSO), aggregation latency (pushdown)",
            labels,
            gain,
            "×",
            "3 data nodes on one machine · " + hardware));
  }
}

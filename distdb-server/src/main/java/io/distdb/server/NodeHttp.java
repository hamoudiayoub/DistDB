package io.distdb.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.distdb.common.metrics.LatencyHistogram;
import io.distdb.kv.Store;
import io.distdb.pd.PlacementDriver;
import io.distdb.storage.lsm.LsmEngine;
import io.distdb.txn.TxnClient;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A node's HTTP port ({@code --http HOST:PORT}):
 *
 * <ul>
 *   <li>{@code /metrics}: Prometheus' text format;
 *   <li>{@code /debug}: a page for people: the node, its ranges and their Raft state;
 *   <li>{@code /debug/clock}: the wall-clock offset; with {@code --chaos on}, {@code ?offset_ms=N}
 *       sets it (the chaos suite's clock skew);
 *   <li>{@code /health}: {@code ok}.
 * </ul>
 */
final class NodeHttp implements AutoCloseable {

  /** What the pages report; the parts a process does not have are null. */
  record Sources(
      String role,
      int id,
      String address,
      String sql,
      SkewClock clock,
      boolean chaos,
      Tuning tuning,
      LsmEngine engine,
      Store store,
      PlacementDriver pd,
      TxnClient txn,
      io.distdb.sql.Engine sqlEngine) {}

  private final HttpServer server;
  private final Sources s;
  private final long started = System.nanoTime();

  private NodeHttp(HttpServer server, Sources s) {
    this.server = server;
    this.s = s;
  }

  static NodeHttp start(Args.HostPort at, Sources s) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(at.host(), at.port()), 64);
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    NodeHttp h = new NodeHttp(server, s);
    server.createContext("/metrics", ex -> h.answer(ex, "text/plain; version=0.0.4", h.metrics()));
    server.createContext("/debug/clock", h::clock);
    server.createContext("/debug", ex -> h.answer(ex, "text/html; charset=utf-8", h.debug()));
    server.createContext("/health", ex -> h.answer(ex, "text/plain", "ok\n"));
    server.start();
    return h;
  }

  int port() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    server.stop(0);
  }

  private void answer(HttpExchange ex, String type, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().add("Content-Type", type);
    ex.sendResponseHeaders(200, bytes.length);
    try (OutputStream o = ex.getResponseBody()) {
      o.write(bytes);
    }
  }

  private void clock(HttpExchange ex) throws IOException {
    URI uri = ex.getRequestURI();
    String q = uri.getQuery();
    if (q != null && q.startsWith("offset_ms=")) {
      if (!s.chaos()) {
        byte[] no = "clock skew needs --chaos on\n".getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(403, no.length);
        try (OutputStream o = ex.getResponseBody()) {
          o.write(no);
        }
        return;
      }
      long offset = Long.parseLong(q.substring("offset_ms=".length()));
      s.clock().offsetMillis(offset);
      io.distdb.common.Log.warn(
          System.getLogger(NodeHttp.class.getName()), "clock skewed", "offset_ms", offset);
    }
    answer(ex, "text/plain", "offset_ms=" + s.clock().offsetMillis() + "\n");
  }

  // -----------------------------------------------------------------------------------------------
  // /metrics

  private static final class Prom {
    final StringBuilder b = new StringBuilder(8192);

    void metric(String name, String type, String help, double value) {
      b.append("# HELP ").append(name).append(' ').append(help).append('\n');
      b.append("# TYPE ").append(name).append(' ').append(type).append('\n');
      b.append(name).append(' ').append(number(value)).append('\n');
    }

    void counter(String name, String help, double value) {
      metric(name, "counter", help, value);
    }

    void gauge(String name, String help, double value) {
      metric(name, "gauge", help, value);
    }

    void histogram(String name, String help, LatencyHistogram.Snapshot h) {
      b.append("# HELP ").append(name).append(' ').append(help).append('\n');
      b.append("# TYPE ").append(name).append(" histogram\n");
      for (int i = 0; i < LatencyHistogram.BOUNDS.length; i++) {
        b.append(name)
            .append("_bucket{le=\"")
            .append(number(LatencyHistogram.BOUNDS[i]))
            .append("\"} ")
            .append(h.cumulative(i))
            .append('\n');
      }
      b.append(name).append("_bucket{le=\"+Inf\"} ").append(h.count()).append('\n');
      b.append(name).append("_sum ").append(number(h.sumSeconds())).append('\n');
      b.append(name).append("_count ").append(h.count()).append('\n');
    }

    static String number(double v) {
      return v == Math.rint(v) && Math.abs(v) < 1e15
          ? Long.toString((long) v)
          : String.format(Locale.ROOT, "%.6g", v);
    }
  }

  String metrics() {
    Prom p = new Prom();
    p.gauge("distdb_up", "1 while this process serves", 1);
    p.gauge(
        "distdb_clock_skew_milliseconds",
        "the wall-clock offset injected by chaos",
        s.clock().offsetMillis());
    if (s.store() != null) {
      Store.Stats st = s.store().stats().orTimeout(5, TimeUnit.SECONDS).join();
      p.gauge("distdb_replicas", "replicas this node holds", st.ranges().size());
      p.gauge("distdb_leaders", "ranges this node leads", st.leaders());
      p.counter(
          "distdb_raft_proposals_total", "entries proposed by leaders here", st.raft().proposals());
      p.counter("distdb_raft_elections_total", "elections started here", st.raft().elections());
      p.counter(
          "distdb_raft_leaderships_total",
          "times a replica here became leader",
          st.raft().leaderships());
      p.counter(
          "distdb_raft_appends_sent_total", "AppendEntries messages sent", st.raft().appendsSent());
      p.counter(
          "distdb_raft_entries_sent_total", "log entries carried by them", st.raft().entriesSent());
      p.histogram(
          "distdb_commit_latency_seconds",
          "from a write's proposal to its applied answer",
          st.commitLatency());
    }
    if (s.pd() != null) {
      PlacementDriver.ServiceStats ps = s.pd().serviceStats();
      p.gauge("distdb_pd_leader", "1 on the placement driver's leader", ps.leader() ? 1 : 0);
      p.gauge("distdb_pd_ranges", "ranges in the directory", ps.ranges());
      p.gauge("distdb_pd_stores", "data nodes known", ps.stores());
      p.counter(
          "distdb_pd_operators_issued_total",
          "operators sent to range leaders",
          ps.operatorsIssued());
      p.counter("distdb_pd_tso_requests_total", "timestamp requests answered", ps.tsoRequests());
      p.counter("distdb_pd_tso_timestamps_total", "timestamps handed out", ps.tsoTimestamps());
    }
    if (s.engine() != null) {
      LsmEngine.Stats e = s.engine().stats();
      p.counter("distdb_engine_writes_total", "write batches", e.writes());
      p.counter("distdb_engine_wal_syncs_total", "write-ahead log fsyncs", e.walSyncs());
      p.counter(
          "distdb_engine_group_commits_total",
          "log writes by a group-commit leader",
          e.groupCommits());
      p.counter(
          "distdb_engine_grouped_writes_total",
          "batches carried by those log writes",
          e.groupedWrites());
      p.counter("distdb_engine_flushes_total", "memtables written to level 0", e.flushes());
      p.counter("distdb_engine_flush_bytes_total", "bytes written by flushes", e.flushBytes());
      p.counter("distdb_engine_compactions_total", "compactions", e.compactions());
      p.counter(
          "distdb_engine_compaction_read_bytes_total",
          "bytes compactions read",
          e.compactionBytesRead());
      p.counter(
          "distdb_engine_compaction_written_bytes_total",
          "bytes compactions wrote",
          e.compactionBytesWritten());
    }
    if (s.txn() != null) {
      TxnClient.Stats t = s.txn().stats();
      p.counter("distdb_txn_begun_total", "transactions begun by this gateway", t.begun());
      p.counter("distdb_txn_committed_total", "transactions committed", t.committed());
      p.counter(
          "distdb_txn_conflicts_total",
          "commits refused: a write conflict or an abort",
          t.conflicts());
      p.counter(
          "distdb_txn_unknown_total",
          "commits whose outcome the client could not learn",
          t.unknownOutcomes());
      p.counter(
          "distdb_txn_lock_resolutions_total",
          "locks of other transactions resolved",
          t.lockResolutions());
      io.distdb.client.KvClient.Traffic traffic = s.txn().traffic();
      p.counter(
          "distdb_gateway_kv_requests_total",
          "KV requests this SQL gateway sent",
          traffic.requests());
      p.counter(
          "distdb_gateway_kv_bytes_sent_total", "bytes of those requests", traffic.bytesSent());
      p.counter(
          "distdb_gateway_kv_bytes_received_total",
          "bytes of their responses",
          traffic.bytesReceived());
    }
    if (s.sqlEngine() != null) {
      io.distdb.sql.Engine.Stats q = s.sqlEngine().stats();
      p.counter("distdb_sql_statements_total", "SQL statements run", q.statements());
      p.counter("distdb_sql_failures_total", "SQL statements that failed", q.failures());
      p.counter(
          "distdb_sql_autocommit_retries_total",
          "autocommit statements run again after a conflict",
          q.autocommitRetries());
      p.histogram("distdb_sql_statement_latency_seconds", "SQL statement latency", q.latency());
    }
    return p.b.toString();
  }

  // -----------------------------------------------------------------------------------------------
  // /debug

  private static String esc(Object o) {
    return String.valueOf(o).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
  }

  private static String ms(double seconds) {
    return String.format(Locale.ROOT, "%.2f ms", seconds * 1000);
  }

  String debug() {
    StringBuilder b = new StringBuilder(16384);
    b.append(
        """
        <!doctype html><html><head><meta charset="utf-8"><meta http-equiv="refresh" content="2">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>DistDB %s %d</title><style>
        :root { --bg:#fbfbfd; --fg:#1d1f24; --dim:#6b7280; --line:#e5e7eb; --accent:#2563eb; --ok:#15803d; --warn:#b45309; }
        @media (prefers-color-scheme: dark) { :root { --bg:#0f1115; --fg:#e6e8ee; --dim:#9aa3b2; --line:#272b33; --accent:#7aa2ff; --ok:#5fd08a; --warn:#f2b25c; } }
        body { margin:0; padding:24px 16px; background:var(--bg); color:var(--fg); font:14px/1.5 ui-sans-serif,system-ui,sans-serif; }
        main { max-width:1100px; margin:auto; } h1 { font-size:20px; margin:0 0 4px; } h2 { font-size:15px; margin:28px 0 8px; }
        .dim { color:var(--dim); } .tiles { display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:10px; }
        .tile { border:1px solid var(--line); border-radius:10px; padding:10px 12px; } .tile b { display:block; font-size:20px; }
        table { width:100%%; border-collapse:collapse; font-variant-numeric:tabular-nums; } td,th { padding:4px 8px; border-bottom:1px solid var(--line); text-align:left; }
        th { color:var(--dim); font-weight:500; } .num { text-align:right; } .leader { color:var(--ok); font-weight:600; }
        .wrap { overflow-x:auto; } code { font-size:12px; }
        </style></head><body><main>
        """
            .formatted(s.role(), s.id()));
    double up = (System.nanoTime() - started) / 1e9;
    b.append("<h1>DistDB ")
        .append(esc(s.role()))
        .append(' ')
        .append(s.id())
        .append("</h1><div class=dim>")
        .append(esc(s.address()))
        .append(s.sql() == null ? "" : " · SQL on " + esc(s.sql()))
        .append(" · up ")
        .append(Duration.ofSeconds((long) up).toString().substring(2).toLowerCase(Locale.ROOT))
        .append(" · ")
        .append(esc(s.tuning().describe()))
        .append(
            s.clock().offsetMillis() == 0
                ? ""
                : " · <b>clock skewed " + s.clock().offsetMillis() + " ms</b>")
        .append(" · <a href=/metrics>metrics</a></div>");
    b.append("<h2>Now</h2><div class=tiles>");
    if (s.store() != null) {
      Store.Stats st = s.store().stats().orTimeout(5, TimeUnit.SECONDS).join();
      tile(b, "replicas", st.ranges().size());
      tile(b, "leading", st.leaders());
      tile(b, "proposals", st.raft().proposals());
      tile(b, "elections", st.raft().elections());
      tile(b, "commit p50", ms(st.commitLatency().quantile(0.5)));
      tile(b, "commit p99", ms(st.commitLatency().quantile(0.99)));
    }
    if (s.pd() != null) {
      PlacementDriver.ServiceStats ps = s.pd().serviceStats();
      tile(b, "PD role", ps.leader() ? "leader" : "follower");
      tile(b, "ranges", ps.ranges());
      tile(b, "data nodes", ps.stores());
      tile(b, "operators", ps.operatorsIssued());
      tile(b, "timestamps", ps.tsoTimestamps());
    }
    if (s.sqlEngine() != null) {
      io.distdb.sql.Engine.Stats q = s.sqlEngine().stats();
      tile(b, "SQL statements", q.statements());
      tile(b, "SQL p99", ms(q.latency().quantile(0.99)));
    }
    if (s.txn() != null) {
      TxnClient.Stats t = s.txn().stats();
      tile(b, "commits", t.committed());
      tile(b, "conflicts", t.conflicts());
    }
    if (s.engine() != null) {
      LsmEngine.Stats e = s.engine().stats();
      tile(b, "WAL fsyncs", e.walSyncs());
      tile(
          b,
          "writes per fsync",
          e.groupCommits() == 0
              ? "-"
              : String.format(Locale.ROOT, "%.1f", (double) e.groupedWrites() / e.groupCommits()));
      tile(b, "compacted", (e.compactionBytesWritten() >> 20) + " MiB");
    }
    b.append("</div>");
    if (s.store() != null) {
      Store.Stats st = s.store().stats().orTimeout(5, TimeUnit.SECONDS).join();
      b.append(
          "<h2>Ranges on this node</h2><div class=wrap><table><tr><th>range</th><th>from</th>"
              + "<th>to</th><th>role</th><th class=num>term</th><th class=num>leader</th>"
              + "<th class=num>commit</th><th class=num>applied</th><th class=num>last</th>"
              + "<th>voters</th></tr>");
      for (Store.RangeStatus r : st.ranges()) {
        b.append("<tr><td>r")
            .append(r.id())
            .append("</td><td><code>")
            .append(esc(r.start().isEmpty() ? "/min" : r.start()))
            .append("</code></td><td><code>")
            .append(esc(r.end().isEmpty() ? "/max" : r.end()))
            .append("</code></td><td")
            .append(r.role().equals("LEADER") ? " class=leader" : "")
            .append('>')
            .append(r.role().toLowerCase(Locale.ROOT))
            .append("</td><td class=num>")
            .append(r.term())
            .append("</td><td class=num>")
            .append(r.leader() == 0 ? "-" : "n" + r.leader())
            .append("</td><td class=num>")
            .append(r.commit())
            .append("</td><td class=num>")
            .append(r.applied())
            .append("</td><td class=num>")
            .append(r.lastIndex())
            .append("</td><td>")
            .append(esc(r.voters()))
            .append(r.learners().isEmpty() ? "" : " + learners " + esc(r.learners()))
            .append("</td></tr>");
      }
      b.append("</table></div>");
    }
    b.append("<p class=dim>refreshes every 2 s</p></main></body></html>");
    return b.toString();
  }

  private static void tile(StringBuilder b, String label, Object value) {
    b.append("<div class=tile><span class=dim>")
        .append(esc(label))
        .append("</span><b>")
        .append(esc(value))
        .append("</b></div>");
  }
}

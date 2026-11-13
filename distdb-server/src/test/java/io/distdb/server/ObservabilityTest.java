package io.distdb.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.common.NodeId;
import io.distdb.raft.RaftConfig;
import io.distdb.storage.lsm.EngineOptions;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.jupiter.api.Test;

/** The tuning switches, the HTTP endpoint and the log format of a node. */
class ObservabilityTest {

  @Test
  void tuningTurnsOptimizationsOffOneByOne() throws Exception {
    assertThat(Tuning.parse(null)).isEqualTo(Tuning.DEFAULT);
    assertThat(Tuning.DEFAULT.describe()).isEqualTo("all optimizations on");
    Tuning t = Tuning.parse("raft.batch=off, wal.group-commit=off");
    RaftConfig raft = t.raft(RaftConfig.defaults(new NodeId(1)));
    assertThat(raft.maxEntriesPerAppend()).isEqualTo(1);
    assertThat(raft.maxInflightAppends())
        .isEqualTo(RaftConfig.defaults(new NodeId(1)).maxInflightAppends());
    assertThat(t.engine(EngineOptions.defaults()).groupCommit()).isFalse();
    assertThat(t.describe()).isEqualTo("off: raft.batch, wal.group-commit");
    assertThat(
            Tuning.parse("raft.pipeline=off")
                .raft(RaftConfig.defaults(new NodeId(1)))
                .maxInflightAppends())
        .isEqualTo(1);
    assertThat(
            Tuning.parse("tso.batch=off")
                .txn(io.distdb.txn.TxnClient.Options.defaults())
                .batchTimestamps())
        .isFalse();
    assertThatThrownBy(() -> Tuning.parse("raft.speed=off"))
        .isInstanceOf(Args.UsageException.class);
    assertThatThrownBy(() -> Tuning.parse("raft.batch=maybe"))
        .isInstanceOf(Args.UsageException.class);
  }

  private static HttpResponse<String> get(int port, String path) throws Exception {
    return HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).build(),
            HttpResponse.BodyHandlers.ofString());
  }

  private static NodeHttp start(SkewClock clock, boolean chaos) throws Exception {
    return NodeHttp.start(
        Args.hostPort("127.0.0.1:0"),
        new NodeHttp.Sources(
            "data node",
            7,
            "127.0.0.1:7707",
            null,
            clock,
            chaos,
            Tuning.DEFAULT,
            null,
            null,
            null,
            null,
            null));
  }

  @Test
  void theHttpPortServesMetricsADebugPageAndHealth() throws Exception {
    SkewClock clock = new SkewClock();
    try (NodeHttp h = start(clock, false)) {
      HttpResponse<String> metrics = get(h.port(), "/metrics");
      assertThat(metrics.statusCode()).isEqualTo(200);
      assertThat(metrics.body())
          .contains("# TYPE distdb_up gauge\ndistdb_up 1\n")
          .contains("distdb_clock_skew_milliseconds 0");
      assertThat(get(h.port(), "/debug").body()).contains("<h1>DistDB data node 7</h1>");
      assertThat(get(h.port(), "/health").body()).isEqualTo("ok\n");
      // Without --chaos on, the clock cannot be skewed.
      assertThat(get(h.port(), "/debug/clock?offset_ms=5000").statusCode()).isEqualTo(403);
      assertThat(clock.offsetMillis()).isZero();
    }
  }

  @Test
  void chaosModeSkewsTheWallClockButNotTheMonotonicOne() throws Exception {
    SkewClock clock = new SkewClock();
    try (NodeHttp h = start(clock, true)) {
      assertThat(get(h.port(), "/debug/clock?offset_ms=-60000").body())
          .isEqualTo("offset_ms=-60000\n");
      // Exactly a minute behind the real clock, however long the request took.
      long now = System.currentTimeMillis();
      assertThat(clock.wallTimeMillis()).isBetween(now - 61_000, now - 59_000);
      long n1 = clock.nanoTime();
      assertThat(clock.nanoTime()).isGreaterThanOrEqualTo(n1);
      assertThat(get(h.port(), "/metrics").body())
          .contains("distdb_clock_skew_milliseconds -60000");
    }
  }

  @Test
  void logsAreOneKeyValueEventPerLine() {
    LogSetup.KeyValueFormatter f = new LogSetup.KeyValueFormatter("node-3");
    LogRecord r =
        new LogRecord(
            Level.INFO,
            io.distdb.common.Log.format(
                "raft role changed", "node", 3, "range", 14, "role", "leader", "note", "a b"));
    r.setLoggerName("io.distdb.kv.Store");
    String line = f.format(r);
    assertThat(line)
        .startsWith("ts=")
        .contains(" level=info proc=node-3 component=kv.Store ")
        .contains("event=\"raft role changed\" node=3 range=14 role=leader note=\"a b\"");
  }
}

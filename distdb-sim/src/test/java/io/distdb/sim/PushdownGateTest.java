package io.distdb.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.client.KvClient;
import io.distdb.sql.Engine;
import io.distdb.sql.Session;
import io.distdb.sql.kv.Awaiter;
import io.distdb.sql.kv.TxnKvStore;
import io.distdb.txn.TxnClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * The Phase 9 gate: on a table spread over many ranges and nodes, aggregation queries move far
 * fewer bytes and answer sooner with pushdown than without, and answer the same.
 *
 * <p>The cluster is the deterministic simulator's, with a wire model ({@link
 * ShardedKvSimulation.Wire}): 1–5 ms of latency per message, a 1 Gbit/s inbound link per node that
 * carries one message at a time, and 1 µs per key a range reads. Bytes are what the gateway's KV
 * client put on and took off the wire (the serialized requests and responses); latency is simulated
 * time from the statement's start to its answer, the median of five runs.
 */
class PushdownGateTest {

  private static final int ROWS = 20_000;
  private static final int RUNS = 5;
  private static final String[] REGIONS = {
    "north", "south", "east", "west", "centre", "coast", "hills", "plains"
  };

  /** A query of the gate; {@code aggregation} ones must show the reductions. */
  private record Query(String name, String sql, boolean aggregation) {}

  private static final List<Query> QUERIES =
      List.of(
          new Query("count", "SELECT count(*) FROM sales", true),
          new Query(
              "group by region",
              "SELECT region, count(*), sum(qty), avg(price) FROM sales GROUP BY region"
                  + " ORDER BY region",
              true),
          new Query(
              "filter, group by product",
              "SELECT product, sum(qty * price), max(price) FROM sales WHERE qty > 6"
                  + " GROUP BY product ORDER BY product",
              true),
          new Query(
              "min and max",
              "SELECT min(price), max(price), min(note) FROM sales WHERE region <> 'north'",
              true),
          new Query(
              "narrow key range",
              "SELECT count(*), sum(qty) FROM sales WHERE id BETWEEN 5000 AND 5999",
              true),
          new Query("filter only", "SELECT id, qty FROM sales WHERE note LIKE '%77%'", false));

  /** One mode's measurement of one query. */
  private record Measure(long bytes, long requests, double millis, List<List<Object>> rows) {}

  @Test
  void pushdownMovesFewerBytesAndAnswersSoonerOnAggregations() {
    ShardedKvSimulation.Cluster cluster =
        ShardedKvSimulation.Cluster.start(
            9, 5, 1_000, new ShardedKvSimulation.Wire(125_000_000, Duration.ofNanos(1_000), true));
    TxnClient client = cluster.txnClient(t -> TxnClient.CrashPoint.NONE);
    Awaiter awaiter =
        new Awaiter() {
          @Override
          public <T> T await(CompletableFuture<T> f) {
            return cluster.await(f);
          }
        };
    Session s = new Engine(new TxnKvStore(client, awaiter), cluster.clock()).session();
    load(s);
    // Let the PD finish splitting and spreading the table.
    cluster.run(Duration.ofSeconds(60));
    s.execute("ANALYZE sales");
    int ranges = cluster.rangeCount();
    int nodes = cluster.leaderNodes();
    assertThat(ranges).as("ranges").isGreaterThanOrEqualTo(16);
    assertThat(nodes).as("nodes leading ranges").isGreaterThanOrEqualTo(3);

    StringBuilder table = new StringBuilder();
    table.append(
        String.format(
            "%n%d rows, %d ranges, led by %d nodes%n%-26s %12s %12s %9s %9s %10s %10s %8s%n",
            ROWS,
            ranges,
            nodes,
            "query",
            "bytes off",
            "bytes on",
            "req off",
            "req on",
            "ms off",
            "ms on",
            "faster"));
    for (Query q : QUERIES) {
      String plan = plan(s, q.sql());
      assertThat(plan).as(q.name()).contains("remote scan");
      // Warm both modes (range cache, catalog), then measure them alternately.
      measure(cluster, client, s, q.sql(), false);
      measure(cluster, client, s, q.sql(), true);
      List<Measure> off = new ArrayList<>();
      List<Measure> on = new ArrayList<>();
      for (int i = 0; i < RUNS; i++) {
        off.add(measure(cluster, client, s, q.sql(), false));
        on.add(measure(cluster, client, s, q.sql(), true));
      }
      Measure a = median(off);
      Measure b = median(on);
      assertThat(b.rows()).as(q.name() + ": same answer").isEqualTo(a.rows());
      table.append(
          String.format(
              "%-26s %12d %12d %9d %9d %10.1f %10.1f %7.1fx%n",
              q.name(),
              a.bytes(),
              b.bytes(),
              a.requests(),
              b.requests(),
              a.millis(),
              b.millis(),
              a.millis() / b.millis()));
      if (q.aggregation()) {
        assertThat(b.bytes() * 10).as(q.name() + ": bytes cut tenfold").isLessThan(a.bytes());
        assertThat(b.millis() * 2).as(q.name() + ": latency halved").isLessThan(a.millis());
      }
    }
    System.out.println("Phase 9 gate, pushdown off vs on:" + table);
  }

  private static void load(Session s) {
    s.execute(
        "CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, product INT, qty INT,"
            + " price NUMERIC(10,2), note TEXT)");
    StringBuilder b = new StringBuilder();
    for (int id = 1; id <= ROWS; id++) {
      if (b.isEmpty()) {
        b.append("INSERT INTO sales VALUES ");
      } else {
        b.append(", ");
      }
      int cents = 100 + (id * 37) % 10_000;
      b.append('(')
          .append(id)
          .append(", '")
          .append(REGIONS[(id * 7) % REGIONS.length])
          .append("', ")
          .append(id % 97)
          .append(", ")
          .append(1 + (id * 13) % 9)
          .append(", ")
          .append(cents / 100)
          .append('.')
          .append(String.format("%02d", cents % 100))
          .append(", 'order ")
          .append(id)
          .append(" shipped in a plain box')");
      if (id % 500 == 0) {
        s.execute(b.toString());
        b.setLength(0);
      }
    }
  }

  private static String plan(Session s, String sql) {
    StringBuilder b = new StringBuilder();
    for (Object[] r : s.executeOne("EXPLAIN " + sql).rows()) {
      b.append(r[0]).append('\n');
    }
    return b.toString();
  }

  private static Measure measure(
      ShardedKvSimulation.Cluster cluster,
      TxnClient client,
      Session s,
      String sql,
      boolean pushdown) {
    s.execute("SET distdb_pushdown = " + (pushdown ? "on" : "off"));
    KvClient.Traffic before = client.traffic();
    Duration start = cluster.elapsed();
    List<List<Object>> rows = new ArrayList<>();
    for (Object[] r : s.executeOne(sql).rows()) {
      rows.add(Arrays.asList(r));
    }
    double millis = (cluster.elapsed().toNanos() - start.toNanos()) / 1e6;
    KvClient.Traffic t = client.traffic().since(before);
    return new Measure(t.bytesSent() + t.bytesReceived(), t.requests(), millis, rows);
  }

  private static Measure median(List<Measure> ms) {
    List<Measure> sorted = new ArrayList<>(ms);
    sorted.sort((x, y) -> Double.compare(x.millis(), y.millis()));
    Measure m = sorted.get(sorted.size() / 2);
    long bytes = ms.stream().mapToLong(Measure::bytes).sorted().toArray()[ms.size() / 2];
    long requests = ms.stream().mapToLong(Measure::requests).sorted().toArray()[ms.size() / 2];
    return new Measure(bytes, requests, m.millis(), m.rows());
  }
}

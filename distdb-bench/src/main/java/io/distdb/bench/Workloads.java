package io.distdb.bench;

import io.distdb.client.KvClient;
import io.distdb.env.ExecutorScheduler;
import io.distdb.env.LocalFileSystem;
import io.distdb.storage.WriteBatch;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/** The benchmark workloads. Each returns what it measured; {@link BenchMain} reports it. */
final class Workloads {

  private Workloads() {}

  /** How long things run: the real numbers, or a quick pass to check the harness. */
  record Timing(Duration warmup, Duration measure, int records) {
    static final Timing FULL = new Timing(Duration.ofSeconds(3), Duration.ofSeconds(12), 20_000);
    static final Timing QUICK = new Timing(Duration.ofSeconds(1), Duration.ofSeconds(3), 2_000);
  }

  static byte[] key(long i) {
    return ("user%010d".formatted(i)).getBytes(StandardCharsets.UTF_8);
  }

  static byte[] value() {
    byte[] v = new byte[100];
    ThreadLocalRandom.current().nextBytes(v);
    return v;
  }

  // -----------------------------------------------------------------------------------------------
  // YCSB on the storage engine, in this process

  /** Workload A (50% reads, 50% synced updates) with {@code threads} clients. */
  static Runner.Result engine(Path dir, int threads, boolean groupCommit, Timing t)
      throws Exception {
    try (ExecutorScheduler s = new ExecutorScheduler();
        LsmEngine db =
            LsmEngine.open(
                LocalFileSystem.INSTANCE,
                s,
                dir,
                EngineOptions.defaults().withGroupCommit(groupCommit))) {
      WriteBatch b = new WriteBatch();
      for (long i = 0; i < t.records(); i++) {
        b.put(key(i), value());
        if (b.count() == 1000) {
          db.write(b, false);
          b = new WriteBatch();
        }
      }
      db.write(b, true);
      Zipf zipf = new Zipf(t.records());
      return Runner.closed(
          threads,
          t.warmup(),
          t.measure(),
          seq -> {
            ThreadLocalRandom r = ThreadLocalRandom.current();
            byte[] k = key(zipf.next(r.nextDouble()));
            if (r.nextBoolean()) {
              db.get(k);
            } else {
              db.write(new WriteBatch().put(k, value()), true);
            }
          });
    }
  }

  // -----------------------------------------------------------------------------------------------
  // YCSB on the KV layer, against a real cluster

  /**
   * Loads {@code records} keys with {@code concurrency} puts in flight; a put that fails (a fresh
   * cluster is still splitting and moving replicas) is tried again, up to five times.
   */
  static void loadKv(KvClient kv, int records, int concurrency) throws InterruptedException {
    Semaphore permits = new Semaphore(concurrency);
    AtomicLong failed = new AtomicLong();
    for (long i = 0; i < records; i++) {
      permits.acquire();
      put(kv, i, 5, permits, failed);
    }
    permits.acquire(concurrency);
    if (failed.get() > 0) {
      throw new IllegalStateException(failed.get() + " of " + records + " loads failed");
    }
  }

  private static void put(KvClient kv, long k, int tries, Semaphore permits, AtomicLong failed) {
    kv.put(key(k), value())
        .whenComplete((r, e) -> {
          if (e == null) {
            permits.release();
          } else if (tries > 1) {
            put(kv, k, tries - 1, permits, failed);
          } else {
            failed.incrementAndGet();
            permits.release();
          }
        });
  }

  /** A KV operation: a read with probability {@code readFraction}, else an update. */
  static Runner.Op kvOp(KvClient kv, Zipf zipf, double readFraction) {
    return seq -> {
      ThreadLocalRandom r = ThreadLocalRandom.current();
      byte[] k = key(zipf.next(r.nextDouble()));
      return r.nextDouble() < readFraction ? kv.get(k) : kv.put(k, value());
    };
  }

  // -----------------------------------------------------------------------------------------------
  // TPC-C-style SQL: new-order and payment

  /** The scale: warehouses, and per warehouse 10 districts; cut down to fit a laptop. */
  record TpccScale(int warehouses, int customersPerDistrict, int items) {
    static final TpccScale FULL = new TpccScale(2, 100, 2000);
    static final TpccScale QUICK = new TpccScale(1, 20, 200);
  }

  record TpccResult(
      Runner.Result newOrder,
      Runner.Result payment,
      long retries,
      long gaveUp,
      double tpmC,
      List<String> consistency) {}

  static void loadTpcc(Connection c, TpccScale s) throws SQLException {
    try (Statement st = c.createStatement()) {
      st.execute(
          "CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_name TEXT, w_tax NUMERIC(4,4),"
              + " w_ytd NUMERIC(12,2))");
      st.execute(
          "CREATE TABLE district (d_w_id INT, d_id INT, d_name TEXT, d_tax NUMERIC(4,4),"
              + " d_ytd NUMERIC(12,2), d_next_o_id INT, PRIMARY KEY (d_w_id, d_id))");
      st.execute(
          "CREATE TABLE customer (c_w_id INT, c_d_id INT, c_id INT, c_last TEXT,"
              + " c_discount NUMERIC(4,4), c_balance NUMERIC(12,2), c_ytd_payment NUMERIC(12,2),"
              + " c_payment_cnt INT, PRIMARY KEY (c_w_id, c_d_id, c_id))");
      st.execute("CREATE TABLE item (i_id INT PRIMARY KEY, i_name TEXT, i_price NUMERIC(5,2))");
      st.execute(
          "CREATE TABLE stock (s_w_id INT, s_i_id INT, s_quantity INT, s_ytd INT,"
              + " s_order_cnt INT, PRIMARY KEY (s_w_id, s_i_id))");
      st.execute(
          "CREATE TABLE orders (o_w_id INT, o_d_id INT, o_id INT, o_c_id INT, o_ol_cnt INT,"
              + " PRIMARY KEY (o_w_id, o_d_id, o_id))");
      st.execute(
          "CREATE TABLE new_order (no_w_id INT, no_d_id INT, no_o_id INT,"
              + " PRIMARY KEY (no_w_id, no_d_id, no_o_id))");
      st.execute(
          "CREATE TABLE order_line (ol_w_id INT, ol_d_id INT, ol_o_id INT, ol_number INT,"
              + " ol_i_id INT, ol_quantity INT, ol_amount NUMERIC(8,2),"
              + " PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))");
      st.execute(
          "CREATE TABLE history (h_c_id INT, h_c_d_id INT, h_c_w_id INT, h_d_id INT, h_w_id INT,"
              + " h_amount NUMERIC(6,2))");
      batched(
          st,
          "INSERT INTO item VALUES ",
          s.items(),
          i -> "(" + (i + 1) + ", 'item-" + (i + 1) + "', " + price(i) + ")");
      for (int w = 1; w <= s.warehouses(); w++) {
        int wid = w;
        st.execute("INSERT INTO warehouse VALUES (" + w + ", 'w" + w + "', 0.0700, 300000.00)");
        batched(
            st,
            "INSERT INTO district VALUES ",
            10,
            d -> "(" + wid + ", " + (d + 1) + ", 'd" + (d + 1) + "', 0.0500, 30000.00, 1)");
        for (int d = 1; d <= 10; d++) {
          int did = d;
          batched(
              st,
              "INSERT INTO customer VALUES ",
              s.customersPerDistrict(),
              n ->
                  "("
                      + wid
                      + ", "
                      + did
                      + ", "
                      + (n + 1)
                      + ", 'last"
                      + (n % 100)
                      + "', 0.0"
                      + (n % 10)
                      + "00, -10.00, 10.00, 1)");
        }
        batched(
            st,
            "INSERT INTO stock VALUES ",
            s.items(),
            i -> "(" + wid + ", " + (i + 1) + ", " + (10 + i % 90) + ", 0, 0)");
      }
      st.execute("ANALYZE");
    }
  }

  private static String price(int i) {
    int cents = 100 + (i * 7919) % 9900;
    return cents / 100 + "." + String.format("%02d", cents % 100);
  }

  private static void batched(
      Statement st, String head, int n, java.util.function.IntFunction<String> row)
      throws SQLException {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < n; i++) {
      b.append(b.isEmpty() ? head : ", ").append(row.apply(i));
      if ((i + 1) % 250 == 0 || i == n - 1) {
        st.execute(b.toString());
        b.setLength(0);
      }
    }
  }

  /** A transaction body; returns normally to commit. */
  @FunctionalInterface
  interface TxnBody {
    void run(Connection c) throws SQLException;
  }

  /** Runs {@code body} in a transaction, again after a serialization failure (up to 20 times). */
  static void transaction(Connection c, TxnBody body, AtomicLong retries, AtomicLong gaveUp)
      throws SQLException {
    for (int attempt = 1; ; attempt++) {
      try {
        body.run(c);
        c.commit();
        return;
      } catch (SQLException e) {
        try {
          c.rollback();
        } catch (SQLException ignored) {
          // the connection may already be in a failed state
        }
        boolean retryable = "40001".equals(e.getSQLState()) || "XX000".equals(e.getSQLState());
        if (!retryable || attempt >= 20) {
          gaveUp.incrementAndGet();
          throw e;
        }
        retries.incrementAndGet();
      }
    }
  }

  /**
   * {@code terminals} terminals (connections spread over the gateways), each bound to one district
   * as TPC-C prescribes, running new-order and payment half and half.
   */
  static TpccResult tpcc(LocalCluster cluster, TpccScale s, int terminals, Timing t)
      throws Exception {
    try (Connection c = cluster.jdbc(1)) {
      loadTpcc(c, s);
    }
    List<Connection> conns = new ArrayList<>();
    for (int i = 0; i < terminals; i++) {
      Connection c = cluster.jdbc(1 + i % cluster.nodes());
      c.setAutoCommit(false);
      conns.add(c);
    }
    AtomicLong retries = new AtomicLong();
    AtomicLong gaveUp = new AtomicLong();
    Histogram newOrderLatency = new Histogram();
    Histogram paymentLatency = new Histogram();
    AtomicLong newOrders = new AtomicLong();
    AtomicLong payments = new AtomicLong();
    long measureFrom = System.nanoTime() + t.warmup().toNanos();
    ThreadLocal<Integer> terminal = new ThreadLocal<>();
    AtomicLong nextTerminal = new AtomicLong();
    Runner.Result all =
        Runner.closed(
            terminals,
            t.warmup(),
            t.measure(),
            seq -> {
              if (terminal.get() == null) {
                terminal.set((int) nextTerminal.getAndIncrement());
              }
              int term = terminal.get();
              Connection c = conns.get(term);
              int w = 1 + term % s.warehouses();
              int d = 1 + (term / s.warehouses()) % 10;
              ThreadLocalRandom r = ThreadLocalRandom.current();
              long t0 = System.nanoTime();
              if (r.nextBoolean()) {
                transaction(c, x -> newOrder(x, s, w, d, r), retries, gaveUp);
                if (t0 >= measureFrom) {
                  newOrderLatency.record(System.nanoTime() - t0);
                  newOrders.incrementAndGet();
                }
              } else {
                transaction(c, x -> payment(x, s, w, d, r), retries, gaveUp);
                if (t0 >= measureFrom) {
                  paymentLatency.record(System.nanoTime() - t0);
                  payments.incrementAndGet();
                }
              }
            });
    for (Connection c : conns) {
      c.close();
    }
    List<String> consistency;
    try (Connection c = cluster.jdbc(1)) {
      consistency = tpccConsistency(c, s);
    }
    double minutes = all.seconds() / 60;
    return new TpccResult(
        new Runner.Result(
            newOrders.get(), 0, all.seconds(), newOrderLatency, new long[0], Runner.TICK),
        new Runner.Result(
            payments.get(), 0, all.seconds(), paymentLatency, new long[0], Runner.TICK),
        retries.get(),
        gaveUp.get(),
        newOrders.get() / minutes,
        consistency);
  }

  private static void newOrder(Connection c, TpccScale s, int w, int d, ThreadLocalRandom r)
      throws SQLException {
    int cid = 1 + r.nextInt(s.customersPerDistrict());
    int lines = 5 + r.nextInt(11);
    try (PreparedStatement wq = c.prepareStatement("SELECT w_tax FROM warehouse WHERE w_id = ?");
        PreparedStatement dq =
            c.prepareStatement(
                "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?");
        PreparedStatement du =
            c.prepareStatement(
                "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = ? AND d_id = ?");
        PreparedStatement cq =
            c.prepareStatement(
                "SELECT c_discount, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id"
                    + " = ?");
        PreparedStatement oi = c.prepareStatement("INSERT INTO orders VALUES (?, ?, ?, ?, ?)");
        PreparedStatement ni = c.prepareStatement("INSERT INTO new_order VALUES (?, ?, ?)");
        PreparedStatement iq = c.prepareStatement("SELECT i_price FROM item WHERE i_id = ?");
        PreparedStatement sq =
            c.prepareStatement("SELECT s_quantity FROM stock WHERE s_w_id = ? AND s_i_id = ?");
        PreparedStatement su =
            c.prepareStatement(
                "UPDATE stock SET s_quantity = ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1"
                    + " WHERE s_w_id = ? AND s_i_id = ?");
        PreparedStatement li =
            c.prepareStatement("INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, ?)")) {
      wq.setInt(1, w);
      BigDecimal wTax = one(wq).getBigDecimal(1);
      dq.setInt(1, w);
      dq.setInt(2, d);
      ResultSet dr = one(dq);
      BigDecimal dTax = dr.getBigDecimal(1);
      int oid = dr.getInt(2);
      du.setInt(1, w);
      du.setInt(2, d);
      du.executeUpdate();
      cq.setInt(1, w);
      cq.setInt(2, d);
      cq.setInt(3, cid);
      BigDecimal discount = one(cq).getBigDecimal(1);
      oi.setInt(1, w);
      oi.setInt(2, d);
      oi.setInt(3, oid);
      oi.setInt(4, cid);
      oi.setInt(5, lines);
      oi.executeUpdate();
      ni.setInt(1, w);
      ni.setInt(2, d);
      ni.setInt(3, oid);
      ni.executeUpdate();
      java.util.Set<Integer> chosen = new java.util.TreeSet<>();
      while (chosen.size() < lines) {
        chosen.add(1 + r.nextInt(s.items()));
      }
      int n = 0;
      for (int item : chosen) {
        n++;
        int qty = 1 + r.nextInt(10);
        iq.setInt(1, item);
        BigDecimal price = one(iq).getBigDecimal(1);
        sq.setInt(1, w);
        sq.setInt(2, item);
        int have = one(sq).getInt(1);
        su.setInt(1, have - qty >= 10 ? have - qty : have - qty + 91);
        su.setInt(2, qty);
        su.setInt(3, w);
        su.setInt(4, item);
        su.executeUpdate();
        BigDecimal amount =
            price
                .multiply(BigDecimal.valueOf(qty))
                .multiply(BigDecimal.ONE.add(wTax).add(dTax))
                .multiply(BigDecimal.ONE.subtract(discount))
                .setScale(2, java.math.RoundingMode.HALF_UP);
        li.setInt(1, w);
        li.setInt(2, d);
        li.setInt(3, oid);
        li.setInt(4, n);
        li.setInt(5, item);
        li.setInt(6, qty);
        li.setBigDecimal(7, amount);
        li.executeUpdate();
      }
    }
  }

  private static void payment(Connection c, TpccScale s, int w, int d, ThreadLocalRandom r)
      throws SQLException {
    int cid = 1 + r.nextInt(s.customersPerDistrict());
    BigDecimal amount = BigDecimal.valueOf(100 + r.nextInt(500000), 2);
    try (PreparedStatement wu =
            c.prepareStatement("UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?");
        PreparedStatement du =
            c.prepareStatement(
                "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?");
        PreparedStatement cu =
            c.prepareStatement(
                "UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?,"
                    + " c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND"
                    + " c_id = ?");
        PreparedStatement hi =
            c.prepareStatement("INSERT INTO history VALUES (?, ?, ?, ?, ?, ?)")) {
      wu.setBigDecimal(1, amount);
      wu.setInt(2, w);
      wu.executeUpdate();
      du.setBigDecimal(1, amount);
      du.setInt(2, w);
      du.setInt(3, d);
      du.executeUpdate();
      cu.setBigDecimal(1, amount);
      cu.setBigDecimal(2, amount);
      cu.setInt(3, w);
      cu.setInt(4, d);
      cu.setInt(5, cid);
      cu.executeUpdate();
      hi.setInt(1, cid);
      hi.setInt(2, d);
      hi.setInt(3, w);
      hi.setInt(4, d);
      hi.setInt(5, w);
      hi.setBigDecimal(6, amount);
      hi.executeUpdate();
    }
  }

  private static ResultSet one(PreparedStatement p) throws SQLException {
    ResultSet r = p.executeQuery();
    if (!r.next()) {
      throw new SQLException("no row", "02000");
    }
    return r;
  }

  /**
   * Three of TPC-C's consistency conditions: W_YTD = sum(D_YTD) per warehouse; D_NEXT_O_ID - 1 =
   * max(O_ID) per district; and every order has exactly its O_OL_CNT order lines. Returns a line
   * per condition, "ok" or what was wrong.
   */
  static List<String> tpccConsistency(Connection c, TpccScale s) throws SQLException {
    List<String> out = new ArrayList<>();
    try (Statement st = c.createStatement()) {
      boolean ok = true;
      for (int w = 1; w <= s.warehouses(); w++) {
        ResultSet a = st.executeQuery("SELECT w_ytd FROM warehouse WHERE w_id = " + w);
        a.next();
        BigDecimal wytd = a.getBigDecimal(1);
        ResultSet b = st.executeQuery("SELECT sum(d_ytd) FROM district WHERE d_w_id = " + w);
        b.next();
        BigDecimal dytd = b.getBigDecimal(1);
        ok &=
            wytd.subtract(new BigDecimal("300000.00"))
                    .compareTo(dytd.subtract(new BigDecimal("300000.00")))
                == 0;
      }
      out.add("W_YTD = sum(D_YTD): " + (ok ? "ok" : "VIOLATED"));
      ok = true;
      ResultSet r =
          st.executeQuery(
              "SELECT d.d_w_id, d.d_id, d.d_next_o_id, max(o.o_id) FROM district d LEFT JOIN orders"
                  + " o ON o.o_w_id = d.d_w_id AND o.o_d_id = d.d_id GROUP BY d.d_w_id, d.d_id,"
                  + " d.d_next_o_id");
      while (r.next()) {
        int max = r.getInt(4);
        ok &= r.getInt(3) - 1 == (r.wasNull() ? 0 : max);
      }
      out.add("D_NEXT_O_ID - 1 = max(O_ID): " + (ok ? "ok" : "VIOLATED"));
      ResultSet lines =
          st.executeQuery(
              "SELECT count(*) FROM orders o WHERE o.o_ol_cnt <> (SELECT count(*) FROM order_line"
                  + " l WHERE l.ol_w_id = o.o_w_id AND l.ol_d_id = o.o_d_id AND l.ol_o_id ="
                  + " o.o_id)");
      lines.next();
      out.add("O_OL_CNT = count(order lines): " + (lines.getLong(1) == 0 ? "ok" : "VIOLATED"));
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // SQL point updates (the TSO batching ablation) and aggregations (the pushdown one)

  static void loadSqlCounters(Connection c, int rows) throws SQLException {
    try (Statement st = c.createStatement()) {
      st.execute("CREATE TABLE counters (k INT PRIMARY KEY, v INT)");
      batched(st, "INSERT INTO counters VALUES ", rows, i -> "(" + i + ", 0)");
    }
  }

  /** Autocommit point updates through every gateway, {@code connections} at a time. */
  static Runner.Result sqlUpdates(LocalCluster cluster, int connections, int rows, Timing t)
      throws Exception {
    List<Connection> conns = new ArrayList<>();
    for (int i = 0; i < connections; i++) {
      conns.add(cluster.jdbc(1 + i % cluster.nodes()));
    }
    ThreadLocal<PreparedStatement> stmt = new ThreadLocal<>();
    AtomicLong next = new AtomicLong();
    try {
      return Runner.closed(
          connections,
          t.warmup(),
          t.measure(),
          seq -> {
            if (stmt.get() == null) {
              stmt.set(
                  conns
                      .get((int) next.getAndIncrement())
                      .prepareStatement("UPDATE counters SET v = v + 1 WHERE k = ?"));
            }
            PreparedStatement p = stmt.get();
            p.setInt(1, ThreadLocalRandom.current().nextInt(rows));
            p.executeUpdate();
          });
    } finally {
      for (Connection c : conns) {
        c.close();
      }
    }
  }

  static void loadSales(Connection c, int rows) throws SQLException {
    String[] regions = {"north", "south", "east", "west", "centre", "coast", "hills", "plains"};
    try (Statement st = c.createStatement()) {
      st.execute(
          "CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, product INT, qty INT,"
              + " price NUMERIC(10,2), note TEXT)");
      batched(
          st,
          "INSERT INTO sales VALUES ",
          rows,
          i ->
              "("
                  + i
                  + ", '"
                  + regions[(i * 7) % regions.length]
                  + "', "
                  + i % 97
                  + ", "
                  + (1 + (i * 13) % 9)
                  + ", "
                  + price(i)
                  + ", 'order "
                  + i
                  + " shipped in a plain box')");
      st.execute("ANALYZE");
    }
  }

  /** One query's latency (median of {@code runs}) and the gateway's KV bytes, pushdown off/on. */
  record PushdownResult(String query, double msOff, double msOn, double bytesOff, double bytesOn) {}

  static List<PushdownResult> pushdown(LocalCluster cluster, int runs) throws Exception {
    Map<String, String> queries = new LinkedHashMap<>();
    queries.put("count(*)", "SELECT count(*) FROM sales");
    queries.put(
        "group by region",
        "SELECT region, count(*), sum(qty), avg(price) FROM sales GROUP BY region");
    queries.put(
        "filter + group by",
        "SELECT product, sum(qty * price) FROM sales WHERE qty > 6 GROUP BY product");
    List<PushdownResult> out = new ArrayList<>();
    try (Connection c = cluster.jdbc(1);
        Statement st = c.createStatement()) {
      for (Map.Entry<String, String> q : queries.entrySet()) {
        double[] ms = new double[2];
        double[] bytes = new double[2];
        for (int mode = 0; mode < 2; mode++) {
          st.execute("SET distdb_pushdown = " + (mode == 0 ? "off" : "on"));
          st.executeQuery(q.getValue()).close(); // warm
          List<Double> times = new ArrayList<>();
          double before = cluster.metric(1, "distdb_gateway_kv_bytes_received_total");
          for (int i = 0; i < runs; i++) {
            long t0 = System.nanoTime();
            try (ResultSet r = st.executeQuery(q.getValue())) {
              while (r.next()) {
                // drain
              }
            }
            times.add((System.nanoTime() - t0) / 1e6);
          }
          double after = cluster.metric(1, "distdb_gateway_kv_bytes_received_total");
          times.sort(null);
          ms[mode] = times.get(times.size() / 2);
          bytes[mode] = (after - before) / runs;
        }
        out.add(new PushdownResult(q.getKey(), ms[0], ms[1], bytes[0], bytes[1]));
      }
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Recovery after a leader crash

  /**
   * Writes at a fixed rate, kills the node leading the most ranges after {@code before}, and
   * reports the timeline: successful writes per 100 ms, and when the kill happened.
   */
  record RecoveryResult(
      Runner.Result run,
      int killedNode,
      int killTick,
      double unavailableMs,
      double backTo90Ms,
      long maxLatencyMs) {}

  static RecoveryResult recovery(LocalCluster cluster, double rate, Duration before, Duration after)
      throws Exception {
    try (KvClient kv = cluster.kv()) {
      loadKv(kv, 5_000, 64);
      Zipf zipf = new Zipf(5_000);
      int victim = cluster.busiestLeader();
      Duration total = before.plus(after);
      Thread killer =
          Thread.ofVirtual()
              .start(() -> {
                try {
                  Thread.sleep(Duration.ofSeconds(1).plus(before));
                  cluster.kill(victim);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              });
      Runner.Result r =
          Runner.open(rate, Duration.ofSeconds(1), total, 20_000, kvOp(kv, zipf, 0.0));
      killer.join();
      int killTick = (int) (before.toMillis() / Runner.TICK.toMillis());
      // Writes complete in bursts (batching), so judge throughput over a 500 ms moving window.
      long[] ticks = r.perTick();
      double baseline = 0;
      for (int i = 2; i < killTick; i++) {
        baseline += ticks[i];
      }
      baseline /= Math.max(1, killTick - 2);
      int window = 5;
      int firstOk = -1;
      int back90 = -1;
      for (int i = killTick; i + window <= ticks.length; i++) {
        double avg = 0;
        for (int k = i; k < i + window; k++) {
          avg += ticks[k];
        }
        avg /= window;
        if (firstOk < 0 && i > killTick && avg >= baseline * 0.5) {
          firstOk = i;
        }
        if (back90 < 0 && i > killTick && avg >= baseline * 0.9) {
          back90 = i;
        }
      }
      double tickMs = Runner.TICK.toMillis();
      return new RecoveryResult(
          r,
          victim,
          killTick,
          firstOk < 0 ? -1 : (firstOk - killTick) * tickMs,
          back90 < 0 ? -1 : (back90 - killTick) * tickMs,
          r.latency().maxNanos() / 1_000_000);
    }
  }

  static CompletableFuture<Void> done() {
    return CompletableFuture.completedFuture(null);
  }
}

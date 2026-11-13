package io.distdb.bench.chaos;

import io.distdb.sim.Linearizability;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Jepsen-style chaos suite: the real binaries in Docker ({@code docker compose up}), real
 * faults, and checkers on what the clients saw.
 *
 * <p><b>Faults</b> (the nemesis, one at a time, at random): kill -9 and restart of a data node or
 * of the placement driver's leader; pause and resume (SIGSTOP, a GC pause or a hung VM); a node cut
 * off by iptables; the cluster split into a majority and a minority; 150 ms ± 50 ms of latency with
 * tc netem; clock skew of a few seconds on a node or a PD node (through the node's clock seam); and
 * clock skew followed by a PD leader failover, which hands the timestamp oracle to a node whose
 * clock is behind.
 *
 * <p><b>Workloads</b>, through the three SQL gateways: bank transfers between accounts with audits;
 * and reads and writes of single-row registers, recorded with when each started and ended and what
 * it returned (or "unknown" when the answer was lost).
 *
 * <p><b>Checks</b>, after healing: every audit saw the opening total; the final total is the
 * opening total; the register history is linearizable (the Phase 5 checker, Wing and Gong with
 * Lowe's memoization).
 */
public final class ChaosSuite {

  private static final int ACCOUNTS = 20;
  private static final int OPENING = 100;
  private static final int REGISTERS = 8;
  private static final List<Integer> SQL_PORTS = List.of(5433, 5434, 5435);
  private static final List<String> DATA = List.of("n1", "n2", "n3", "n4", "n5");
  private static final List<String> PD = List.of("pd1", "pd2", "pd3");

  private final Duration duration;
  private final Random random;
  private final String only;
  private final Path report;
  private final long t0 = System.nanoTime();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final List<String> faults = Collections.synchronizedList(new ArrayList<>());
  private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
  private final List<Linearizability.Op> history = Collections.synchronizedList(new ArrayList<>());
  private final List<String> violations = Collections.synchronizedList(new ArrayList<>());
  private final HttpClient http = HttpClient.newHttpClient();

  private ChaosSuite(Duration duration, long seed, Path report, String only) {
    this.only = only;
    this.duration = duration;
    this.random = new Random(seed);
    this.report = report;
  }

  /**
   * {@code chaos [--duration 180s] [--seed N] [--report FILE] [--no-up] [--only KIND]}; KIND is one
   * of kill, kill-pd, pause, isolate, split, latency, skew, skew-failover (all, at random, by
   * default).
   */
  public static int run(List<String> args) throws Exception {
    Duration duration = Duration.ofSeconds(180);
    long seed = System.nanoTime();
    Path report = Path.of("docs", "bench", "chaos.json");
    boolean up = true;
    String only = null;
    for (int i = 0; i < args.size(); i++) {
      switch (args.get(i)) {
        case "--duration" ->
            duration = Duration.ofSeconds(Long.parseLong(args.get(++i).replace("s", "")));
        case "--seed" -> seed = Long.parseLong(args.get(++i));
        case "--report" -> report = Path.of(args.get(++i));
        case "--no-up" -> up = false;
        case "--only" -> only = args.get(++i);
        default -> throw new IllegalArgumentException("unknown chaos option " + args.get(i));
      }
    }
    ChaosSuite s = new ChaosSuite(duration, seed, report, only);
    log("seed " + seed + ", " + duration.toSeconds() + " s of faults");
    if (up) {
      docker("compose", "up", "-d");
    }
    return s.go();
  }

  static void log(String s) {
    System.out.println("[" + Instant.now().toString().substring(11, 19) + "] " + s);
    System.out.flush();
  }

  private long now() {
    return System.nanoTime() - t0;
  }

  private void count(String what) {
    counts.computeIfAbsent(what, k -> new AtomicLong()).incrementAndGet();
  }

  // -----------------------------------------------------------------------------------------------
  // The run

  int go() throws Exception {
    awaitSql(Duration.ofMinutes(3));
    setUp();
    List<Thread> workers = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      int id = i;
      workers.add(Thread.ofPlatform().name("bank-" + i).start(() -> bank(id)));
    }
    workers.add(Thread.ofPlatform().name("auditor").start(this::auditor));
    for (int i = 0; i < 3; i++) {
      int id = i;
      workers.add(Thread.ofPlatform().name("register-" + i).start(() -> registers(id)));
    }
    Thread nemesis = Thread.ofPlatform().name("nemesis").start(this::nemesis);
    nemesis.join();
    running.set(false);
    for (Thread t : workers) {
      t.join(30_000);
    }
    log("healing; then the final checks");
    healAll();
    Thread.sleep(15_000);
    awaitSql(Duration.ofMinutes(2));
    long[] last = finalAudit(Duration.ofMinutes(2));
    if (last == null) {
      violations.add("the final audit never got an answer");
    } else if (last[0] != ACCOUNTS || last[1] != (long) ACCOUNTS * OPENING) {
      violations.add("final audit: " + last[0] + " accounts holding " + last[1]);
    }
    Linearizability.Result lin;
    synchronized (history) {
      lin = Linearizability.check(new ArrayList<>(history));
    }
    if (!lin.linearizable()) {
      violations.add("register " + lin.key() + " is not linearizable: " + lin.history());
    }
    write(lin);
    boolean ok = violations.isEmpty();
    log((ok ? "PASS" : "FAIL") + ": " + counts + "; " + faults.size() + " faults");
    for (String v : violations) {
      log("violation: " + (v.length() > 2000 ? v.substring(0, 2000) + "…" : v));
    }
    return ok ? 0 : 1;
  }

  private void setUp() throws Exception {
    try (Connection c = connect(5433);
        Statement st = c.createStatement()) {
      st.execute("DROP TABLE IF EXISTS accounts");
      st.execute("DROP TABLE IF EXISTS registers");
      st.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT NOT NULL)");
      StringBuilder b = new StringBuilder("INSERT INTO accounts VALUES ");
      for (int i = 0; i < ACCOUNTS; i++) {
        b.append(i == 0 ? "" : ", ").append('(').append(i).append(", ").append(OPENING).append(')');
      }
      st.execute(b.toString());
      st.execute("CREATE TABLE registers (k INT PRIMARY KEY, v INT NOT NULL)");
      b = new StringBuilder("INSERT INTO registers VALUES ");
      for (int i = 0; i < REGISTERS; i++) {
        b.append(i == 0 ? "" : ", ").append('(').append(i).append(", 0)");
      }
      st.execute(b.toString());
    }
    for (int k = 0; k < REGISTERS; k++) {
      history.add(new Linearizability.Op(-1, "r" + k, Linearizability.Kind.PUT, "0", 0, 0));
    }
    log("schema ready: " + ACCOUNTS + " accounts of " + OPENING + ", " + REGISTERS + " registers");
  }

  // -----------------------------------------------------------------------------------------------
  // Clients

  private Connection connect(int port) throws SQLException {
    Properties p = new Properties();
    p.setProperty("user", "distdb");
    p.setProperty("connectTimeout", "5");
    p.setProperty("socketTimeout", "15");
    p.setProperty("tcpKeepAlive", "true");
    return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + port + "/distdb", p);
  }

  /** A connection to some gateway, retried until one answers or the run ends. */
  private Connection anyGateway(Random r) {
    while (running.get()) {
      try {
        return connect(SQL_PORTS.get(r.nextInt(SQL_PORTS.size())));
      } catch (SQLException e) {
        sleep(300);
      }
    }
    return null;
  }

  private static boolean broken(SQLException e) {
    String s = e.getSQLState();
    return s == null || s.startsWith("08") || s.equals("57P01");
  }

  private void bank(int id) {
    Random r = new Random(id * 7919L + 1);
    Connection c = null;
    while (running.get()) {
      try {
        if (c == null) {
          c = anyGateway(r);
          if (c == null) {
            return;
          }
          c.setAutoCommit(false);
        }
        int a = r.nextInt(ACCOUNTS);
        int b = (a + 1 + r.nextInt(ACCOUNTS - 1)) % ACCOUNTS;
        int amount = 1 + r.nextInt(5);
        try (PreparedStatement get =
                c.prepareStatement("SELECT balance FROM accounts WHERE id = ?");
            PreparedStatement set =
                c.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE id = ?")) {
          get.setInt(1, a);
          int have;
          try (ResultSet rs = get.executeQuery()) {
            rs.next();
            have = rs.getInt(1);
          }
          if (have >= amount) {
            set.setInt(1, -amount);
            set.setInt(2, a);
            set.executeUpdate();
            set.setInt(1, amount);
            set.setInt(2, b);
            set.executeUpdate();
          }
          c.commit();
          count("transfers");
        }
      } catch (SQLException e) {
        count("transfer failures");
        try {
          if (c != null) {
            c.rollback();
          }
        } catch (SQLException ignored) {
          // broken anyway
        }
        if (broken(e)) {
          close(c);
          c = null;
          sleep(200);
        }
      }
    }
    close(c);
  }

  private void auditor() {
    Random r = new Random(99);
    Connection c = null;
    while (running.get()) {
      try {
        if (c == null) {
          c = anyGateway(r);
          if (c == null) {
            return;
          }
        }
        try (Statement st = c.createStatement();
            ResultSet rs = st.executeQuery("SELECT count(*), sum(balance) FROM accounts")) {
          rs.next();
          long n = rs.getLong(1);
          long total = rs.getLong(2);
          count("audits");
          if (n != ACCOUNTS || total != (long) ACCOUNTS * OPENING) {
            violations.add(
                String.format("audit at %.1f s: %d accounts holding %d", now() / 1e9, n, total));
          }
        }
        sleep(500);
      } catch (SQLException e) {
        count("audit failures");
        if (broken(e)) {
          close(c);
          c = null;
        }
        sleep(200);
      }
    }
    close(c);
  }

  private void registers(int client) {
    Random r = new Random(client * 31L + 5);
    Connection c = null;
    long seq = 0;
    while (running.get()) {
      if (c == null) {
        c = anyGateway(r);
        if (c == null) {
          return;
        }
      }
      int k = r.nextInt(REGISTERS);
      boolean write = r.nextBoolean();
      long call = now();
      if (write) {
        int value = client * 1_000_000 + (int) ++seq;
        try (PreparedStatement p = c.prepareStatement("UPDATE registers SET v = ? WHERE k = ?")) {
          p.setInt(1, value);
          p.setInt(2, k);
          p.executeUpdate();
          history.add(op(client, k, Linearizability.Kind.PUT, "" + value, call, now()));
          count("register writes");
        } catch (SQLException e) {
          if ("40001".equals(e.getSQLState())) {
            count("register writes refused"); // a conflict: it did not happen
          } else {
            // The answer was lost: it may have happened, at any time after its call.
            history.add(op(client, k, Linearizability.Kind.PUT, "" + value, call, Long.MAX_VALUE));
            count("register writes unknown");
          }
          if (broken(e)) {
            close(c);
            c = null;
          }
        }
      } else {
        try (PreparedStatement p = c.prepareStatement("SELECT v FROM registers WHERE k = ?")) {
          p.setInt(1, k);
          String v;
          try (ResultSet rs = p.executeQuery()) {
            v = rs.next() ? "" + rs.getInt(1) : null;
          }
          history.add(op(client, k, Linearizability.Kind.GET, v, call, now()));
          count("register reads");
        } catch (SQLException e) {
          count("register reads failed");
          if (broken(e)) {
            close(c);
            c = null;
          }
        }
      }
      sleep(20 + r.nextInt(40));
    }
    close(c);
  }

  private static Linearizability.Op op(
      int client, int k, Linearizability.Kind kind, String v, long call, long ret) {
    return new Linearizability.Op(client, "r" + k, kind, v, call, ret);
  }

  // -----------------------------------------------------------------------------------------------
  // The nemesis

  private void nemesis() {
    long end = now() + duration.toNanos();
    sleep(5_000); // a calm start
    while (now() < end && violations.isEmpty()) {
      try {
        int kind =
            only == null
                ? random.nextInt(8)
                : List.of(
                        "kill",
                        "kill-pd",
                        "pause",
                        "isolate",
                        "split",
                        "latency",
                        "skew",
                        "skew-failover")
                    .indexOf(only);
        if (kind < 0) {
          throw new IllegalArgumentException("unknown fault kind " + only);
        }
        switch (kind) {
          case 0 -> killNode();
          case 1 -> killPdLeader();
          case 2 -> pause();
          case 3 -> isolate();
          case 4 -> split();
          case 5 -> latency();
          case 6 -> skew();
          default -> skewThenFailover();
        }
      } catch (Exception e) {
        log("nemesis step failed: " + e);
      }
      sleep(2_000 + random.nextInt(3_000));
    }
  }

  private void fault(String what) {
    String line = String.format("%.1f s: %s", now() / 1e9, what);
    faults.add(line);
    log("fault " + line);
  }

  private void killNode() throws Exception {
    String n = DATA.get(random.nextInt(DATA.size()));
    fault("kill -9 " + n);
    docker("kill", "--signal", "KILL", "distdb-" + n);
    sleep(4_000 + random.nextInt(5_000));
    docker("start", "distdb-" + n);
  }

  private void killPdLeader() throws Exception {
    String leader = pdLeader();
    if (leader == null) {
      return;
    }
    fault("kill -9 the PD leader " + leader);
    docker("kill", "--signal", "KILL", "distdb-" + leader);
    sleep(4_000 + random.nextInt(4_000));
    docker("start", "distdb-" + leader);
  }

  private void pause() throws Exception {
    String n = all().get(random.nextInt(all().size()));
    fault("pause " + n);
    docker("pause", "distdb-" + n);
    sleep(3_000 + random.nextInt(4_000));
    docker("unpause", "distdb-" + n);
  }

  private void isolate() throws Exception {
    String n = all().get(random.nextInt(all().size()));
    List<String> others = new ArrayList<>(all());
    others.remove(n);
    fault("partition: " + n + " cut off from everyone");
    cut(List.of(n), others);
    sleep(6_000 + random.nextInt(5_000));
    heal(List.of(n));
  }

  private void split() throws Exception {
    List<String> data = new ArrayList<>(DATA);
    Collections.shuffle(data, random);
    List<String> pds = new ArrayList<>(PD);
    Collections.shuffle(pds, random);
    List<String> minority = new ArrayList<>(data.subList(0, 2));
    minority.add(pds.getFirst());
    List<String> majority = new ArrayList<>(all());
    majority.removeAll(minority);
    fault("partition: " + minority + " | " + majority);
    cut(minority, majority);
    sleep(8_000 + random.nextInt(5_000));
    heal(minority);
  }

  private void latency() throws Exception {
    List<String> nodes = new ArrayList<>(all());
    Collections.shuffle(nodes, random);
    List<String> slow = nodes.subList(0, 2);
    fault("latency 150ms ± 50ms on " + slow);
    for (String n : slow) {
      docker(
          "exec",
          "distdb-" + n,
          "tc",
          "qdisc",
          "add",
          "dev",
          "eth0",
          "root",
          "netem",
          "delay",
          "150ms",
          "50ms");
    }
    sleep(8_000 + random.nextInt(4_000));
    for (String n : slow) {
      dockerQuiet("exec", "distdb-" + n, "tc", "qdisc", "del", "dev", "eth0", "root");
    }
  }

  private void skew() throws Exception {
    String n = all().get(random.nextInt(all().size()));
    long offset = (random.nextBoolean() ? 1 : -1) * (1_000L + random.nextInt(7_000));
    fault("clock skew " + offset + " ms on " + n);
    skew(n, offset);
    sleep(10_000);
    skew(n, 0);
  }

  /** Puts a PD follower's clock behind, then kills the leader: the oracle may move to it. */
  private void skewThenFailover() throws Exception {
    String leader = pdLeader();
    if (leader == null) {
      return;
    }
    List<String> followers = new ArrayList<>(PD);
    followers.remove(leader);
    long offset = -(3_000L + random.nextInt(5_000));
    fault(
        "clock skew " + offset + " ms on " + followers + ", then kill -9 the PD leader " + leader);
    for (String f : followers) {
      skew(f, offset);
    }
    docker("kill", "--signal", "KILL", "distdb-" + leader);
    sleep(8_000);
    docker("start", "distdb-" + leader);
    sleep(4_000);
    for (String f : followers) {
      skew(f, 0);
    }
  }

  private static List<String> all() {
    List<String> a = new ArrayList<>(PD);
    a.addAll(DATA);
    return a;
  }

  private int httpPort(String n) {
    return n.startsWith("pd") ? 8090 + (n.charAt(2) - '0') : 8080 + (n.charAt(1) - '0');
  }

  private void skew(String n, long offset) throws Exception {
    http.send(
        HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + httpPort(n) + "/debug/clock?offset_ms=" + offset))
            .timeout(Duration.ofSeconds(5))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private String pdLeader() {
    for (String p : PD) {
      try {
        String body =
            http.send(
                    HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + httpPort(p) + "/metrics"))
                        .timeout(Duration.ofSeconds(3))
                        .build(),
                    HttpResponse.BodyHandlers.ofString())
                .body();
        if (body.contains("\ndistdb_pd_leader 1")) {
          return p;
        }
      } catch (Exception ignored) {
        // that one is down
      }
    }
    return null;
  }

  private static String ip(String n) throws Exception {
    return docker(
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            "distdb-" + n)
        .strip();
  }

  /** Drops all traffic between every node of {@code a} and every node of {@code b}. */
  private void cut(List<String> a, List<String> b) throws Exception {
    Map<String, String> ips = new LinkedHashMap<>();
    for (String n : all()) {
      ips.put(n, ip(n));
    }
    for (String x : a) {
      for (String y : b) {
        String ipY = ips.get(y);
        docker("exec", "distdb-" + x, "iptables", "-A", "INPUT", "-s", ipY, "-j", "DROP");
        docker("exec", "distdb-" + x, "iptables", "-A", "OUTPUT", "-d", ipY, "-j", "DROP");
      }
    }
  }

  private void heal(List<String> nodes) throws Exception {
    for (String n : nodes) {
      dockerQuiet("exec", "distdb-" + n, "iptables", "-F");
    }
  }

  private void healAll() {
    for (String n : all()) {
      dockerQuiet("start", "distdb-" + n);
      dockerQuiet("unpause", "distdb-" + n);
      dockerQuiet("exec", "distdb-" + n, "iptables", "-F");
      dockerQuiet("exec", "distdb-" + n, "tc", "qdisc", "del", "dev", "eth0", "root");
      try {
        skew(n, 0);
      } catch (Exception ignored) {
        // retried by the wait below
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  private void awaitSql(Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    for (int port : SQL_PORTS) {
      while (true) {
        try (Connection c = connect(port);
            Statement st = c.createStatement()) {
          st.execute("SHOW TABLES");
          break;
        } catch (SQLException e) {
          if (System.nanoTime() > deadline) {
            throw new IllegalStateException("SQL port " + port + " never answered", e);
          }
          Thread.sleep(500);
        }
      }
    }
  }

  private long[] finalAudit(Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      try (Connection c = connect(5433);
          Statement st = c.createStatement();
          ResultSet rs = st.executeQuery("SELECT count(*), sum(balance) FROM accounts")) {
        rs.next();
        return new long[] {rs.getLong(1), rs.getLong(2)};
      } catch (SQLException e) {
        sleep(1_000);
      }
    }
    return null;
  }

  private void write(Linearizability.Result lin) throws IOException {
    StringBuilder b = new StringBuilder("{\n");
    b.append("  \"when\": \"").append(Instant.now()).append("\",\n");
    b.append("  \"seconds_of_faults\": ").append(duration.toSeconds()).append(",\n");
    b.append("  \"verdict\": \"").append(violations.isEmpty() ? "pass" : "fail").append("\",\n");
    b.append("  \"register_history_ops\": ").append(history.size()).append(",\n");
    b.append("  \"linearizable\": ").append(lin.linearizable()).append(",\n");
    b.append("  \"counts\": {");
    List<String> keys = new ArrayList<>(counts.keySet());
    Collections.sort(keys);
    for (int i = 0; i < keys.size(); i++) {
      b.append(i == 0 ? "\n" : ",\n")
          .append("    \"")
          .append(keys.get(i))
          .append("\": ")
          .append(counts.get(keys.get(i)).get());
    }
    b.append("\n  },\n  \"faults\": [");
    for (int i = 0; i < faults.size(); i++) {
      b.append(i == 0 ? "\n" : ",\n")
          .append("    \"")
          .append(faults.get(i).replace("\"", "'"))
          .append('"');
    }
    b.append("\n  ],\n  \"violations\": [");
    for (int i = 0; i < violations.size(); i++) {
      String v = violations.get(i);
      v = v.length() > 500 ? v.substring(0, 500) + "…" : v;
      b.append(i == 0 ? "\n" : ",\n").append("    \"").append(v.replace("\"", "'")).append('"');
    }
    b.append("\n  ]\n}\n");
    Files.createDirectories(report.toAbsolutePath().getParent());
    Files.writeString(report, b.toString(), StandardCharsets.UTF_8);
  }

  private static void close(Connection c) {
    if (c != null) {
      try {
        c.close();
      } catch (SQLException ignored) {
        // going away
      }
    }
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  static String docker(String... args) throws Exception {
    List<String> cmd = new ArrayList<>();
    cmd.add("docker");
    cmd.addAll(List.of(args));
    Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    if (!p.waitFor(120, TimeUnit.SECONDS) || p.exitValue() != 0) {
      throw new IOException("docker " + String.join(" ", args) + " failed: " + out.strip());
    }
    return out;
  }

  private static void dockerQuiet(String... args) {
    try {
      docker(args);
    } catch (Exception ignored) {
      // healing what may already be healed
    }
  }
}

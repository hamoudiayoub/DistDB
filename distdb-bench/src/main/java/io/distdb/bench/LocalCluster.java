package io.distdb.bench;

import io.distdb.client.ClientOptions;
import io.distdb.client.KvClient;
import io.distdb.env.ExecutorScheduler;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.SystemClock;
import io.distdb.rpc.PdClient;
import io.distdb.rpc.grpc.GrpcKvChannel;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A real cluster on this machine: three placement-driver processes and {@code n} data-node
 * processes (each its own JVM, talking gRPC over localhost), every data node serving SQL and HTTP.
 * Nodes can be killed ({@code kill -9}) and restarted.
 */
final class LocalCluster implements AutoCloseable {

  private final Path dir;
  private final int nodes;
  private final String tune;
  private final long splitKeys;
  private final List<String> pdPeers = new ArrayList<>();
  private final List<String> pdAddresses = new ArrayList<>();
  private final Map<Integer, Integer> nodePort = new LinkedHashMap<>();
  private final Map<Integer, Integer> sqlPort = new LinkedHashMap<>();
  private final Map<Integer, Integer> httpPort = new LinkedHashMap<>();
  private final Map<String, Process> processes = new LinkedHashMap<>();
  private final Path argFile;
  private final ExecutorScheduler scheduler = new ExecutorScheduler();
  private final HttpClient http = HttpClient.newHttpClient();
  private final List<PdClient> pdClients = new ArrayList<>();

  LocalCluster(Path dir, int nodes, String tune, long splitKeys) throws IOException {
    this.dir = dir;
    this.nodes = nodes;
    this.tune = tune;
    this.splitKeys = splitKeys;
    Files.createDirectories(dir);
    argFile = dir.resolve("classpath.args");
    Files.writeString(
        argFile, "-cp \"" + System.getProperty("java.class.path").replace('\\', '/') + "\"\n");
  }

  static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket()) {
      s.setReuseAddress(true);
      s.bind(new InetSocketAddress("127.0.0.1", 0));
      return s.getLocalPort();
    }
  }

  LocalCluster start() throws Exception {
    for (int i = 1; i <= 3; i++) {
      int port = freePort();
      pdPeers.add(i + "=127.0.0.1:" + port);
      pdAddresses.add("127.0.0.1:" + port);
    }
    for (int i = 1; i <= 3; i++) {
      List<String> args =
          new ArrayList<>(
              List.of(
                  "pd",
                  "--data-dir",
                  dir.resolve("pd" + i).toString(),
                  "--id",
                  "" + i,
                  "--peers",
                  String.join(",", pdPeers),
                  "--split-keys",
                  "" + splitKeys,
                  "--dead-after",
                  "10s"));
      tuned(args);
      launch("pd" + i, args, "256m");
    }
    for (int i = 1; i <= nodes; i++) {
      nodePort.put(i, freePort());
      sqlPort.put(i, freePort());
      httpPort.put(i, freePort());
      startNode(i);
    }
    awaitReady(Duration.ofSeconds(90));
    Thread.sleep(3_000); // let the placement driver spread the first replicas
    return this;
  }

  private void tuned(List<String> args) {
    if (tune != null && !tune.isBlank()) {
      args.add("--tune");
      args.add(tune);
    }
  }

  private void startNode(int i) throws IOException {
    String founders = nodes >= 3 ? "1,2,3" : nodes == 2 ? "1,2" : "1";
    List<String> args =
        new ArrayList<>(
            List.of(
                "node",
                "--data-dir",
                dir.resolve("n" + i).toString(),
                "--id",
                "" + i,
                "--listen",
                "127.0.0.1:" + nodePort.get(i),
                "--pd",
                String.join(",", pdAddresses),
                "--founders",
                founders,
                "--sql",
                "127.0.0.1:" + sqlPort.get(i),
                "--http",
                "127.0.0.1:" + httpPort.get(i)));
    tuned(args);
    launch("n" + i, args, "512m");
  }

  private void launch(String name, List<String> args, String heap) throws IOException {
    String java = ProcessHandle.current().info().command().orElseThrow();
    List<String> cmd = new ArrayList<>();
    cmd.add(java);
    cmd.add("-Xmx" + heap);
    cmd.add("-XX:+UseSerialGC");
    cmd.add("-XX:TieredStopAtLevel=4");
    String jfr = System.getenv("DISTDB_BENCH_JFR");
    if (jfr != null && name.equals("n1")) {
      cmd.add("-XX:StartFlightRecording=filename=" + jfr + ",settings=profile,duration=22s");
    }
    cmd.add("@" + argFile);
    cmd.add("io.distdb.server.Main");
    cmd.addAll(args);
    Process p =
        new ProcessBuilder(cmd)
            .redirectErrorStream(true)
            .redirectOutput(dir.resolve(name + ".log").toFile())
            .start();
    processes.put(name, p);
  }

  /** Waits until a key can be written and read through the PD, and every SQL port answers. */
  void awaitReady(Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    try (KvClient c = kv()) {
      while (true) {
        try {
          c.put("probe".getBytes(StandardCharsets.UTF_8), new byte[] {1}, Duration.ofSeconds(2))
              .get(3, TimeUnit.SECONDS);
          break;
        } catch (Exception e) {
          if (System.nanoTime() > deadline) {
            throw new IllegalStateException("the cluster in " + dir + " did not come up", e);
          }
          Thread.sleep(200);
        }
      }
    }
    for (int i : sqlPort.keySet()) {
      while (true) {
        try (java.sql.Connection c = jdbc(i)) {
          c.createStatement().execute("SHOW TABLES");
          break;
        } catch (Exception e) {
          if (System.nanoTime() > deadline) {
            throw new IllegalStateException("node " + i + "'s SQL port did not answer", e);
          }
          Thread.sleep(200);
        }
      }
    }
  }

  /** A KV client routed through the placement driver. */
  KvClient kv() {
    PdClient pd = new PdClient(pdAddresses, GrpcKvChannel::connect);
    synchronized (pdClients) {
      pdClients.add(pd);
    }
    return new KvClient(
        pd,
        GrpcKvChannel::connect,
        SystemClock.INSTANCE,
        scheduler,
        SplittableRandomSource.fromEntropy(),
        ClientOptions.defaults().withDefaultTimeout(Duration.ofSeconds(5)));
  }

  java.sql.Connection jdbc(int node) throws java.sql.SQLException {
    java.util.Properties p = new java.util.Properties();
    p.setProperty("user", "distdb");
    p.setProperty("connectTimeout", "5");
    p.setProperty("socketTimeout", "30");
    return java.sql.DriverManager.getConnection(
        "jdbc:postgresql://127.0.0.1:" + sqlPort.get(node) + "/distdb", p);
  }

  int nodes() {
    return nodes;
  }

  List<String> pdAddresses() {
    return pdAddresses;
  }

  /** One of the node's metrics, from its /metrics page; 0 if absent. */
  double metric(int node, String name) throws IOException, InterruptedException {
    String body =
        http.send(
                HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + httpPort.get(node) + "/metrics"))
                    .timeout(Duration.ofSeconds(5))
                    .build(),
                HttpResponse.BodyHandlers.ofString())
            .body();
    for (String line : body.split("\n")) {
      if (line.startsWith(name + " ")) {
        return Double.parseDouble(line.substring(name.length() + 1).trim());
      }
    }
    return 0;
  }

  /** Kills data node {@code i} without warning (kill -9). */
  void kill(int i) throws InterruptedException {
    Process p = processes.get("n" + i);
    p.destroyForcibly();
    p.waitFor(10, TimeUnit.SECONDS);
  }

  void restart(int i) throws IOException {
    startNode(i);
  }

  /** The node that leads the most ranges, as the placement driver last heard. */
  int busiestLeader() throws Exception {
    try (PdClient pd = new PdClient(pdAddresses, GrpcKvChannel::connect)) {
      var list =
          pd.call(
                  io.distdb.rpc.proto.PdRequest.newBuilder()
                      .setListRanges(io.distdb.rpc.proto.ListRangesRequest.getDefaultInstance())
                      .build(),
                  Duration.ofSeconds(3))
              .get(5, TimeUnit.SECONDS)
              .getListRanges();
      Map<Integer, Integer> counts = new LinkedHashMap<>();
      for (var r : list.getRangesList()) {
        counts.merge(r.getLeader(), 1, Integer::sum);
      }
      counts.remove(0);
      return counts.entrySet().stream().max(Map.Entry.comparingByValue()).orElseThrow().getKey();
    }
  }

  int ranges() throws Exception {
    try (PdClient pd = new PdClient(pdAddresses, GrpcKvChannel::connect)) {
      return pd.call(
              io.distdb.rpc.proto.PdRequest.newBuilder()
                  .setListRanges(io.distdb.rpc.proto.ListRangesRequest.getDefaultInstance())
                  .build(),
              Duration.ofSeconds(3))
          .get(5, TimeUnit.SECONDS)
          .getListRanges()
          .getRangesCount();
    }
  }

  @Override
  public void close() {
    synchronized (pdClients) {
      pdClients.forEach(PdClient::close);
    }
    List<CompletableFuture<?>> exits = new ArrayList<>();
    for (Process p : processes.values()) {
      p.destroyForcibly();
      exits.add(p.onExit());
    }
    try {
      CompletableFuture.allOf(exits.toArray(CompletableFuture[]::new)).get(20, TimeUnit.SECONDS);
    } catch (Exception ignored) {
      // best effort
    }
    scheduler.close();
  }

  /** The last lines a process printed, for error messages. */
  String tail(String name) {
    try (BufferedReader r =
        new BufferedReader(
            new InputStreamReader(
                Files.newInputStream(dir.resolve(name + ".log")), StandardCharsets.UTF_8))) {
      List<String> lines = r.lines().toList();
      return String.join("\n", lines.subList(Math.max(0, lines.size() - 20), lines.size()));
    } catch (IOException e) {
      return "(no log)";
    }
  }
}

package io.distdb.server;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * {@code distdb demo}: a whole cluster in this one process (three placement-driver nodes and three
 * data nodes on localhost, in a temporary directory), then the SQL shell connected to it. For a
 * first look with nothing to set up; the nodes are the real ones, talking gRPC to each other.
 */
final class DemoCommand {

  private DemoCommand() {}

  static int run(List<String> argv, PrintStream out, PrintStream err) throws Exception {
    int nodes = 3;
    int sqlPort = 5433;
    List<String> shellArgs = new ArrayList<>();
    for (int i = 0; i < argv.size(); i++) {
      switch (argv.get(i)) {
        case "--nodes" -> nodes = Integer.parseInt(argv.get(++i));
        case "--sql-port" -> sqlPort = Integer.parseInt(argv.get(++i));
        default -> shellArgs.add(argv.get(i));
      }
    }
    if (nodes < 1 || nodes > 9) {
      throw new Args.UsageException("--nodes must be 1 to 9");
    }
    System.setProperty("distdb.demo", "true");
    Path dir = Files.createTempDirectory("distdb-demo-");
    quietLogs(dir.resolve("demo.log"));
    PrintStream log =
        new PrintStream(
            Files.newOutputStream(dir.resolve("nodes.out")), true, StandardCharsets.UTF_8);
    boolean scripted = shellArgs.contains("-c") || shellArgs.contains("-f");
    Progress progress = new Progress(scripted ? PrintStream.nullOutputStream() : out);
    progress.say("starting 3 placement-driver nodes");
    int pdBase = freeBase(7600, 3);
    List<String> peers = new ArrayList<>();
    List<String> pdAddresses = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      peers.add(i + "=127.0.0.1:" + (pdBase + i));
      pdAddresses.add("127.0.0.1:" + (pdBase + i));
    }
    for (int i = 1; i <= 3; i++) {
      start(
          "pd" + i,
          List.of(
              "pd",
              "--data-dir",
              dir.resolve("pd" + i).toString(),
              "--id",
              "" + i,
              "--peers",
              String.join(",", peers),
              "--split-keys",
              "2000"),
          log);
    }
    progress.say("starting " + nodes + " data nodes");
    int nodeBase = freeBase(7700, nodes);
    int httpBase = freeBase(8080, nodes);
    String founders = nodes >= 3 ? "1,2,3" : nodes == 2 ? "1,2" : "1";
    for (int i = 1; i <= nodes; i++) {
      start(
          "n" + i,
          List.of(
              "node",
              "--data-dir",
              dir.resolve("n" + i).toString(),
              "--id",
              "" + i,
              "--listen",
              "127.0.0.1:" + (nodeBase + i),
              "--pd",
              String.join(",", pdAddresses),
              "--founders",
              founders,
              "--sql",
              "127.0.0.1:" + (sqlPort + i - 1),
              "--http",
              "127.0.0.1:" + (httpBase + i)),
          log);
    }
    progress.say("waiting for the first range to elect a leader");
    long deadline = System.nanoTime() + 60_000_000_000L;
    while (!answers(sqlPort)) {
      if (System.nanoTime() > deadline) {
        progress.done();
        err.println("error: the demo cluster did not come up in a minute; see " + dir);
        return KvCommand.FAILED;
      }
      progress.tick();
      Thread.sleep(150);
    }
    progress.done();
    if (!scripted) {
      out.println(
          "  a cluster of 3 placement-driver nodes and "
              + nodes
              + " data nodes is running in this process; SQL on 127.0.0.1:"
              + sqlPort
              + (nodes > 1 ? "-" + (sqlPort + nodes - 1) : "")
              + "; each node's /debug page on http://127.0.0.1:"
              + (httpBase + 1)
              + (nodes > 1 ? "-" + (httpBase + nodes) : "")
              + "\n  (data and logs in "
              + dir
              + "; it all stops when you quit)");
    }
    List<String> args = new ArrayList<>();
    args.add("127.0.0.1:" + sqlPort);
    args.addAll(shellArgs);
    return io.distdb.shell.Shell.run(args, System.in, out, err);
  }

  /** Whether the SQL port answers a statement that needs the cluster (a timestamp, a catalog). */
  private static boolean answers(int port) {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress("127.0.0.1", port), 300);
    } catch (IOException e) {
      return false;
    }
    try (io.distdb.shell.PgClient c =
        io.distdb.shell.PgClient.connect(
            "127.0.0.1", port, "distdb", "distdb", null, java.time.Duration.ofSeconds(2))) {
      return c.query("SHOW TABLES").getFirst() instanceof io.distdb.shell.PgClient.Ok;
    } catch (IOException | io.distdb.shell.PgClient.PgError | RuntimeException e) {
      return false;
    }
  }

  private static void start(String name, List<String> args, PrintStream log) {
    Thread t =
        Thread.ofPlatform()
            .name("demo-" + name)
            .daemon(true)
            .start(() -> {
              try {
                Main.run(args, log, log);
              } catch (RuntimeException e) {
                log.println(name + " stopped: " + e);
              }
            });
    t.setUncaughtExceptionHandler((th, e) -> log.println(name + " failed: " + e));
  }

  /** The first port p >= base such that p+1 .. p+count are all free. */
  private static int freeBase(int base, int count) throws IOException {
    for (int p = base; p < base + 500; p += 10) {
      boolean free = true;
      for (int i = 1; i <= count && free; i++) {
        try (ServerSocket s = new ServerSocket()) {
          s.bind(new InetSocketAddress("127.0.0.1", p + i));
        } catch (IOException e) {
          free = false;
        }
      }
      if (free) {
        return p;
      }
    }
    throw new IOException("no free ports near " + base);
  }

  /** Every log record goes to {@code file}, none to the terminal the shell is drawing on. */
  private static void quietLogs(Path file) throws IOException {
    Logger root = LogManager.getLogManager().getLogger("");
    for (Handler h : root.getHandlers()) {
      root.removeHandler(h);
    }
    FileHandler f = new FileHandler(file.toString());
    f.setFormatter(new SimpleFormatter());
    root.addHandler(f);
    root.setLevel(Level.INFO);
  }

  /** One status line that redraws itself, with a spinner. */
  private static final class Progress {
    private static final String FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";
    private final PrintStream out;
    private String message = "";
    private int frame;

    Progress(OutputStream out) {
      this.out =
          out instanceof PrintStream p ? p : new PrintStream(out, true, StandardCharsets.UTF_8);
    }

    void say(String m) {
      message = m;
      tick();
    }

    void tick() {
      out.print(
          "\r\u001b[2K  \u001b[38;5;75m"
              + FRAMES.charAt(frame++ % FRAMES.length())
              + "\u001b[0m "
              + message
              + "…");
      out.flush();
    }

    void done() {
      out.print("\r\u001b[2K");
      out.flush();
    }
  }
}

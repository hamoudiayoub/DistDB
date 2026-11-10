package io.distdb.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.client.ClientOptions;
import io.distdb.client.Entry;
import io.distdb.client.KvClient;
import io.distdb.env.ExecutorScheduler;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.SystemClock;
import io.distdb.rpc.grpc.GrpcKvChannel;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Phase 3 gate: a client in another process reads and writes over the network. The node runs in
 * its own JVM, the CLI in a fresh JVM per command, and the library client in this one; they meet
 * only over TCP. The node is then killed outright, like {@code kill -9}, and restarted.
 */
class MultiProcessGateTest {

  private static final Pattern READY = Pattern.compile(Pattern.quote(NodeCommand.READY) + "(\\S+)");

  @TempDir Path tmp;
  private final AtomicInteger processes = new AtomicInteger();
  private final Map<Long, Path> stderrFiles = new ConcurrentHashMap<>();

  @Test
  void aClientInAnotherProcessReadsAndWritesOverTheNetwork() throws Exception {
    Path data = tmp.resolve("data");
    String addr;
    try (NodeProcess node = NodeProcess.start(this, data)) {
      addr = node.address;
      // The command-line client: each command is its own process.
      assertThat(cli("--server", addr, "put", "greeting", "hello world")).isEqualTo(ok("OK"));
      assertThat(cli("--server", addr, "get", "greeting")).isEqualTo(ok("hello world"));
      assertThat(cli("--server", addr, "put", "bin\\x00key", "tab\\x09value")).isEqualTo(ok("OK"));
      assertThat(cli("--server", addr, "get", "bin\\x00key")).isEqualTo(ok("tab\\x09value"));
      assertThat(cli("--server", addr, "get", "missing"))
          .isEqualTo(new Result(KvCommand.NOT_FOUND, "(not found)"));

      // The library client in this JVM: 200 concurrent writes, then reads back.
      try (GrpcKvChannel channel = GrpcKvChannel.connect(addr);
          ExecutorScheduler scheduler = new ExecutorScheduler()) {
        KvClient client =
            new KvClient(
                channel,
                SystemClock.INSTANCE,
                scheduler,
                SplittableRandomSource.fromEntropy(),
                ClientOptions.defaults().withDefaultTimeout(Duration.ofSeconds(60)));
        List<CompletableFuture<Void>> writes = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
          writes.add(client.put(utf8(item(i)), utf8("value " + i)));
        }
        CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).join();
        List<Entry> items = KvClient.await(client.scan(utf8("item"), utf8("iten"), 1000));
        assertThat(items).hasSize(200);
        assertThat(new String(items.get(42).value(), StandardCharsets.UTF_8)).isEqualTo("value 42");
        KvClient.await(client.delete(utf8(item(7))));
        assertThat(KvClient.await(client.get(utf8(item(7))))).isEmpty();
      }
      assertThat(cli("--server", addr, "scan", "item", "iten", "--limit", "3"))
          .isEqualTo(ok("item000\tvalue 0\nitem001\tvalue 1\nitem002\tvalue 2"));

      node.kill(); // no shutdown, no final sync: like pulling the plug on the process
      assertThat(cli("--server", addr, "--timeout", "1s", "get", "greeting").exitCode())
          .isEqualTo(KvCommand.FAILED);
    }

    // Every acknowledged write was synced, so all of them survive.
    try (NodeProcess node = NodeProcess.start(this, data)) {
      addr = node.address;
      assertThat(cli("--server", addr, "get", "greeting")).isEqualTo(ok("hello world"));
      assertThat(cli("--server", addr, "get", item(7)).exitCode()).isEqualTo(KvCommand.NOT_FOUND);
      assertThat(cli("--server", addr, "get", item(199))).isEqualTo(ok("value 199"));
      assertThat(cli("--server", addr, "delete", "greeting")).isEqualTo(ok("OK"));
      assertThat(cli("--server", addr, "get", "greeting").exitCode())
          .isEqualTo(KvCommand.NOT_FOUND);
    }
  }

  @Test
  void twoNodesCannotShareADataDirectory() throws Exception {
    Path data = tmp.resolve("shared");
    try (NodeProcess first = NodeProcess.start(this, data)) {
      Process second = java("node", "--data-dir", data.toString(), "--listen", "127.0.0.1:0");
      String out = new String(second.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertThat(second.waitFor(60, TimeUnit.SECONDS)).isTrue();
      assertThat(second.exitValue()).isEqualTo(3);
      assertThat(Files.readString(stderrOf(second))).contains("already using");
      assertThat(out).isEmpty();
      assertThat(cli("--server", first.address, "put", "still", "serving")).isEqualTo(ok("OK"));
    }
  }

  // ---------------------------------------------------------------------------------------------

  record Result(int exitCode, String stdout) {}

  private static Result ok(String stdout) {
    return new Result(KvCommand.OK, stdout);
  }

  private Result cli(String... args) throws Exception {
    // A generous deadline: this test is about correctness across processes, not latency, and a
    // cold CLI JVM talking to a cold node on a busy machine (parallel test tasks) can be slow.
    // A later --timeout in args wins.
    List<String> argv = new ArrayList<>(List.of("kv", "--timeout", "60s"));
    argv.addAll(List.of(args));
    Process p = java(argv.toArray(String[]::new));
    String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(p.waitFor(90, TimeUnit.SECONDS)).as("the CLI finished").isTrue();
    if (p.exitValue() != 0) {
      System.out.println(
          "distdb " + argv + " exited " + p.exitValue() + ": " + Files.readString(stderrOf(p)));
    }
    return new Result(p.exitValue(), out.strip().replace("\r\n", "\n"));
  }

  /** Starts {@code distdb <args>} in a new JVM, with stderr going to a file. */
  Process java(String... args) throws IOException {
    String javaBin = ProcessHandle.current().info().command().orElseThrow();
    // The classpath can be longer than Windows allows on a command line, so pass it in an
    // argument file. Forward slashes avoid backslash escaping inside the file.
    Path argFile = tmp.resolve("classpath.args");
    if (!Files.exists(argFile)) {
      String cp = System.getProperty("java.class.path").replace('\\', '/');
      Files.writeString(argFile, "-cp \"" + cp + "\"\n");
    }
    List<String> command = new ArrayList<>(List.of(javaBin, "@" + argFile, Main.class.getName()));
    command.addAll(List.of(args));
    Path stderr = tmp.resolve("stderr-" + processes.incrementAndGet() + ".txt");
    Process p = new ProcessBuilder(command).redirectError(stderr.toFile()).start();
    stderrFiles.put(p.pid(), stderr);
    return p;
  }

  private Path stderrOf(Process p) {
    return stderrFiles.get(p.pid());
  }

  private static String item(int i) {
    return String.format("item%03d", i);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /** A {@code distdb node} process, ready once it has printed its address. */
  private static final class NodeProcess implements AutoCloseable {
    final Process process;
    final String address;

    private NodeProcess(Process process, String address) {
      this.process = process;
      this.address = address;
    }

    static NodeProcess start(MultiProcessGateTest test, Path data) throws Exception {
      Process p = test.java("node", "--data-dir", data.toString(), "--listen", "127.0.0.1:0");
      LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();
      Thread.ofVirtual()
          .start(() -> {
            try (BufferedReader r =
                new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
              for (String line; (line = r.readLine()) != null; ) {
                lines.add(line);
              }
            } catch (IOException e) {
              // The process went away.
            }
          });
      String line = lines.poll(60, TimeUnit.SECONDS);
      if (line == null) {
        p.destroyForcibly();
        throw new AssertionError(
            "node did not start: " + Files.readString(test.stderrOf(p), StandardCharsets.UTF_8));
      }
      Matcher m = READY.matcher(line);
      assertThat(m.find()).as("ready line: %s", line).isTrue();
      return new NodeProcess(p, m.group(1));
    }

    void kill() throws InterruptedException {
      process.destroyForcibly();
      assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
    }

    @Override
    public void close() {
      if (process.isAlive()) {
        try {
          kill();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}

package io.distdb.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Starts {@code distdb} commands as separate JVMs, for tests that need real processes. */
final class DistdbProcesses {

  private static final Pattern READY = Pattern.compile(Pattern.quote(NodeCommand.READY) + "(\\S+)");

  /** What a CLI command printed and returned. */
  record Result(int exitCode, String stdout) {}

  private final Path tmp;
  private final AtomicInteger count = new AtomicInteger();
  private final Map<Long, Path> stderrFiles = new ConcurrentHashMap<>();

  DistdbProcesses(Path tmp) {
    this.tmp = tmp;
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
    Path stderr = tmp.resolve("stderr-" + count.incrementAndGet() + ".txt");
    Process p = new ProcessBuilder(command).redirectError(stderr.toFile()).start();
    stderrFiles.put(p.pid(), stderr);
    return p;
  }

  Path stderrOf(Process p) {
    return stderrFiles.get(p.pid());
  }

  /**
   * Runs {@code distdb kv <args>} to completion. A generous default deadline: these tests are about
   * correctness across processes, and a cold CLI JVM on a busy machine can be slow. A later
   * --timeout in args wins.
   */
  Result cli(String... args) throws Exception {
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

  /** Starts {@code distdb node <args>} and waits until it prints its address. */
  NodeProcess node(String... args) throws Exception {
    List<String> argv = new ArrayList<>(List.of("node"));
    argv.addAll(List.of(args));
    Process p = java(argv.toArray(String[]::new));
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
          "node did not start: " + Files.readString(stderrOf(p), StandardCharsets.UTF_8));
    }
    Matcher m = READY.matcher(line);
    assertThat(m.find()).as("ready line: %s", line).isTrue();
    return new NodeProcess(p, m.group(1), List.copyOf(argv));
  }

  /** Ports nothing listens on right now (a small race, fine for tests). */
  static int[] freePorts(int n) throws IOException {
    ServerSocket[] sockets = new ServerSocket[n];
    int[] ports = new int[n];
    try {
      for (int i = 0; i < n; i++) {
        sockets[i] = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        ports[i] = sockets[i].getLocalPort();
      }
    } finally {
      for (ServerSocket s : sockets) {
        if (s != null) {
          s.close();
        }
      }
    }
    return ports;
  }

  /** A running {@code distdb node}. */
  static final class NodeProcess implements AutoCloseable {
    final Process process;
    final String address;
    final List<String> args;

    NodeProcess(Process process, String address, List<String> args) {
      this.process = process;
      this.address = address;
      this.args = args;
    }

    /** Kills it outright, like {@code kill -9}: no shutdown, no final sync. */
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

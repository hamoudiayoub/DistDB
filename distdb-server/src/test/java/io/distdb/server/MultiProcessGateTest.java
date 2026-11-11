package io.distdb.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.client.ClientOptions;
import io.distdb.client.Entry;
import io.distdb.client.KvClient;
import io.distdb.env.ExecutorScheduler;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.SystemClock;
import io.distdb.rpc.grpc.GrpcKvChannel;
import io.distdb.server.DistdbProcesses.NodeProcess;
import io.distdb.server.DistdbProcesses.Result;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Phase 3 gate: a client in another process reads and writes over the network. The node runs in
 * its own JVM (since Phase 5, a replicated group of one), the CLI in a fresh JVM per command, and
 * the library client in this one; they meet only over TCP. The node is then killed outright, like
 * {@code kill -9}, and restarted.
 */
class MultiProcessGateTest {

  @TempDir Path tmp;
  private DistdbProcesses procs;

  @BeforeEach
  void setUp() {
    procs = new DistdbProcesses(tmp);
  }

  private NodeProcess start(Path data) throws Exception {
    return procs.node("--data-dir", data.toString(), "--listen", "127.0.0.1:0");
  }

  @Test
  void aClientInAnotherProcessReadsAndWritesOverTheNetwork() throws Exception {
    Path data = tmp.resolve("data");
    String addr;
    try (NodeProcess node = start(data)) {
      addr = node.address;
      // The command-line client: each command is its own process.
      assertThat(procs.cli("--server", addr, "put", "greeting", "hello world")).isEqualTo(ok("OK"));
      assertThat(procs.cli("--server", addr, "get", "greeting")).isEqualTo(ok("hello world"));
      assertThat(procs.cli("--server", addr, "put", "bin\\x00key", "tab\\x09value"))
          .isEqualTo(ok("OK"));
      assertThat(procs.cli("--server", addr, "get", "bin\\x00key")).isEqualTo(ok("tab\\x09value"));
      assertThat(procs.cli("--server", addr, "get", "missing"))
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
      assertThat(procs.cli("--server", addr, "scan", "item", "iten", "--limit", "3"))
          .isEqualTo(ok("item000\tvalue 0\nitem001\tvalue 1\nitem002\tvalue 2"));

      node.kill(); // no shutdown, no final sync: like pulling the plug on the process
      assertThat(procs.cli("--server", addr, "--timeout", "1s", "get", "greeting").exitCode())
          .isEqualTo(KvCommand.FAILED);
    }

    // Every acknowledged write was synced, so all of them survive.
    try (NodeProcess node = start(data)) {
      addr = node.address;
      assertThat(procs.cli("--server", addr, "get", "greeting")).isEqualTo(ok("hello world"));
      assertThat(procs.cli("--server", addr, "get", item(7)).exitCode())
          .isEqualTo(KvCommand.NOT_FOUND);
      assertThat(procs.cli("--server", addr, "get", item(199))).isEqualTo(ok("value 199"));
      assertThat(procs.cli("--server", addr, "delete", "greeting")).isEqualTo(ok("OK"));
      assertThat(procs.cli("--server", addr, "get", "greeting").exitCode())
          .isEqualTo(KvCommand.NOT_FOUND);
    }
  }

  @Test
  void twoNodesCannotShareADataDirectory() throws Exception {
    Path data = tmp.resolve("shared");
    try (NodeProcess first = start(data)) {
      Process second = procs.java("node", "--data-dir", data.toString(), "--listen", "127.0.0.1:0");
      String out = new String(second.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertThat(second.waitFor(60, TimeUnit.SECONDS)).isTrue();
      assertThat(second.exitValue()).isEqualTo(3);
      assertThat(Files.readString(procs.stderrOf(second))).contains("already using");
      assertThat(out).isEmpty();
      assertThat(procs.cli("--server", first.address, "put", "still", "serving"))
          .isEqualTo(ok("OK"));
    }
  }

  private static Result ok(String stdout) {
    return new Result(KvCommand.OK, stdout);
  }

  private static String item(int i) {
    return String.format("item%03d", i);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}

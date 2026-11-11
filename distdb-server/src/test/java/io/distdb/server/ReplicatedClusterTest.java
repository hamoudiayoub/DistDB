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
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Phase 5 milestone on real processes: three {@code distdb node} JVMs replicate over gRPC. The
 * test kills the leader outright and measures how long writes stay unavailable (failover), checks
 * that no acknowledged write was lost, restarts the dead node, then kills another, so the cluster
 * only keeps working if the restarted node caught up.
 */
class ReplicatedClusterTest {

  @TempDir Path tmp;

  @Test
  void aThreeNodeClusterSurvivesLosingItsLeader() throws Exception {
    DistdbProcesses procs = new DistdbProcesses(tmp);
    int[] ports = DistdbProcesses.freePorts(3);
    List<String> addresses = new ArrayList<>();
    StringBuilder peers = new StringBuilder();
    for (int i = 0; i < 3; i++) {
      addresses.add("127.0.0.1:" + ports[i]);
      peers.append(i == 0 ? "" : ",").append(i + 1).append('=').append(addresses.get(i));
    }
    NodeProcess[] nodes = new NodeProcess[3];
    try (ExecutorScheduler scheduler = new ExecutorScheduler()) {
      for (int i = 0; i < 3; i++) {
        nodes[i] = start(procs, i, peers.toString());
      }
      try (KvClient client =
          new KvClient(
              addresses,
              GrpcKvChannel::connect,
              SystemClock.INSTANCE,
              scheduler,
              SplittableRandomSource.fromEntropy(),
              ClientOptions.defaults().withDefaultTimeout(Duration.ofSeconds(60)))) {
        List<CompletableFuture<Void>> writes = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
          writes.add(client.put(utf8(item(i)), utf8("value " + i)));
        }
        CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).join();

        // Kill the leader and time how long until a write succeeds again.
        int leader = addresses.indexOf(client.leader());
        assertThat(leader).as("the client found the leader").isNotNegative();
        long killed = System.nanoTime();
        nodes[leader].kill();
        KvClient.await(client.put(utf8("after-failover"), utf8("yes")));
        long failoverMillis = (System.nanoTime() - killed) / 1_000_000;
        int newLeader = addresses.indexOf(client.leader());
        System.out.printf(
            "ReplicatedClusterTest: killed leader n%d; writes succeeded again after %d ms, through"
                + " n%d%n",
            leader + 1, failoverMillis, newLeader + 1);
        assertThat(newLeader).isNotEqualTo(leader);
        assertThat(failoverMillis).as("failover, ms").isLessThan(10_000);

        // Nothing acknowledged was lost.
        List<Entry> items = KvClient.await(client.scan(utf8("item"), utf8("iten"), 1000));
        assertThat(items).hasSize(100);

        // The CLI in its own process, pointed at any node, is redirected to the leader.
        String all = String.join(",", addresses);
        assertThat(procs.cli("--server", all, "put", "via-cli", "hello"))
            .isEqualTo(new Result(KvCommand.OK, "OK"));
        assertThat(procs.cli("--server", all, "get", "via-cli"))
            .isEqualTo(new Result(KvCommand.OK, "hello"));

        // Bring the dead node back; it catches up from the log (or a snapshot).
        nodes[leader] = start(procs, leader, peers.toString());
        KvClient.await(client.put(utf8("after-restart"), utf8("yes")));
        Thread.sleep(2_000); // give it time to replicate everything

        // Kill the other survivor that isn't leading: the restarted node must now be one of the
        // two that form a majority, so this only works if it really caught up.
        int other = 3 - leader - newLeader;
        nodes[other].kill();
        KvClient.await(client.put(utf8("after-second-failure"), utf8("yes")));
        assertThat(KvClient.await(client.get(utf8("after-failover"))))
            .map(v -> new String(v, StandardCharsets.UTF_8))
            .isEqualTo(Optional.of("yes"));
        assertThat(KvClient.await(client.scan(utf8("item"), utf8("iten"), 1000))).hasSize(100);
      }
    } finally {
      for (NodeProcess n : nodes) {
        if (n != null) {
          n.close();
        }
      }
    }
  }

  private NodeProcess start(DistdbProcesses procs, int i, String peers) throws Exception {
    return procs.node(
        "--data-dir",
        tmp.resolve("n" + (i + 1)).toString(),
        "--id",
        String.valueOf(i + 1),
        "--peers",
        peers);
  }

  private static String item(int i) {
    return String.format("item%03d", i);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}

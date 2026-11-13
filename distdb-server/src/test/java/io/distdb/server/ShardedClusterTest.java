package io.distdb.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.client.ClientOptions;
import io.distdb.client.KvClient;
import io.distdb.env.ExecutorScheduler;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.SystemClock;
import io.distdb.kv.Ranges;
import io.distdb.rpc.PdClient;
import io.distdb.rpc.grpc.GrpcKvChannel;
import io.distdb.rpc.proto.ListRangesRequest;
import io.distdb.rpc.proto.ListRangesResponse;
import io.distdb.rpc.proto.PdRequest;
import io.distdb.rpc.proto.RangeInfo;
import io.distdb.server.DistdbProcesses.NodeProcess;
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
 * The Phase 6 milestone on real processes: a placement driver of three {@code distdb pd} JVMs and
 * four {@code distdb node} JVMs, talking gRPC. Range 1 starts on nodes 1 to 3; as clients write,
 * the PD splits it and spreads the ranges onto node 4. Then the test kills a data node outright and
 * keeps writing, and every acknowledged write must still read back.
 */
class ShardedClusterTest {

  private static final int KEYS = 400;

  @TempDir Path tmp;

  @Test
  void aShardedClusterSplitsSpreadsAndSurvivesLosingANode() throws Exception {
    DistdbProcesses procs = new DistdbProcesses(tmp);
    int[] ports = DistdbProcesses.freePorts(7);
    List<String> pdAddresses = new ArrayList<>();
    StringBuilder pdPeers = new StringBuilder();
    for (int i = 0; i < 3; i++) {
      pdAddresses.add("127.0.0.1:" + ports[i]);
      pdPeers.append(i == 0 ? "" : ",").append(i + 1).append('=').append(pdAddresses.get(i));
    }
    String pdList = String.join(",", pdAddresses);
    List<NodeProcess> all = new ArrayList<>();
    NodeProcess[] nodes = new NodeProcess[4];
    try (ExecutorScheduler scheduler = new ExecutorScheduler();
        PdClient pd = new PdClient(pdAddresses, GrpcKvChannel::connect)) {
      for (int i = 0; i < 3; i++) {
        all.add(
            procs.pd(
                "--data-dir",
                tmp.resolve("pd" + (i + 1)).toString(),
                "--id",
                "" + (i + 1),
                "--peers",
                pdPeers.toString(),
                "--split-keys",
                "60"));
      }
      for (int i = 0; i < 4; i++) {
        nodes[i] =
            procs.node(
                "--data-dir",
                tmp.resolve("n" + (i + 1)).toString(),
                "--id",
                "" + (i + 1),
                "--listen",
                "127.0.0.1:" + ports[3 + i],
                "--pd",
                pdList,
                "--founders",
                "1,2,3");
        all.add(nodes[i]);
      }
      try (KvClient client =
          new KvClient(
              new PdClient(pdAddresses, GrpcKvChannel::connect),
              GrpcKvChannel::connect,
              SystemClock.INSTANCE,
              scheduler,
              SplittableRandomSource.fromEntropy(),
              ClientOptions.defaults().withDefaultTimeout(Duration.ofMinutes(2)))) {
        // Cold JVMs on one machine: wait for the cluster to form before timing anything.
        awaitRanges(pd, r -> ranges(r) >= 1 && r.getRanges(0).getLeader() != 0);
        long start = System.nanoTime();
        writeAll(client, 0, KEYS / 2);
        System.out.printf(
            "ShardedClusterTest: %d writes took %d ms%n",
            KEYS / 2, (System.nanoTime() - start) / 1_000_000);

        // The PD splits the range as it grows and moves replicas onto node 4.
        ListRangesResponse spread = awaitRanges(pd, r -> ranges(r) >= 3 && onNode(r, 4) > 0);
        System.out.printf(
            "ShardedClusterTest: after %d ms, %d ranges, %d of them with a replica on node 4:%n  %s%n",
            (System.nanoTime() - start) / 1_000_000,
            ranges(spread),
            onNode(spread, 4),
            String.join("\n  ", describe(spread)));

        // Kill a data node outright and keep writing.
        nodes[0].kill();
        long killed = System.nanoTime();
        writeAll(client, KEYS / 2, KEYS);
        System.out.printf(
            "ShardedClusterTest: with node 1 dead, %d more writes took %d ms%n",
            KEYS / 2, (System.nanoTime() - killed) / 1_000_000);
        for (int i = 0; i < KEYS; i++) {
          Optional<byte[]> v = KvClient.await(client.get(utf8(key(i))));
          assertThat(v).as(key(i)).isPresent();
          assertThat(new String(v.get(), StandardCharsets.UTF_8)).isEqualTo("value " + i);
        }
        assertThat(KvClient.await(client.scan(new byte[0], null, 10 * KEYS))).hasSize(KEYS);
      }
    } catch (Throwable t) {
      // What each process said, for the failure report.
      for (NodeProcess p : all) {
        System.out.println("---- " + p.args + " (alive " + p.process.isAlive() + ")");
        System.out.println(java.nio.file.Files.readString(procs.stderrOf(p.process)));
      }
      throw t;
    } finally {
      all.forEach(NodeProcess::close);
    }
  }

  private static void writeAll(KvClient client, int from, int to) {
    List<CompletableFuture<Void>> writes = new ArrayList<>();
    for (int i = from; i < to; i++) {
      writes.add(client.put(utf8(key(i)), utf8("value " + i)));
    }
    writes.forEach(KvClient::await);
  }

  private static ListRangesResponse awaitRanges(
      PdClient pd, java.util.function.Predicate<ListRangesResponse> done) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
    ListRangesResponse last = null;
    while (System.nanoTime() < deadline) {
      try {
        last =
            pd.call(
                    PdRequest.newBuilder()
                        .setListRanges(ListRangesRequest.getDefaultInstance())
                        .build(),
                    Duration.ofSeconds(2))
                .join()
                .getListRanges();
        if (done.test(last)) {
          return last;
        }
      } catch (RuntimeException e) {
        // that PD node is not the leader, or not up yet
      }
      Thread.sleep(250);
    }
    throw new AssertionError(
        "the ranges never spread out; last seen: " + (last == null ? "nothing" : describe(last)));
  }

  private static int ranges(ListRangesResponse r) {
    return r.getRangesCount();
  }

  private static long onNode(ListRangesResponse r, int node) {
    return r.getRangesList().stream()
        .filter(info -> info.getDesc().getVotersList().contains(node))
        .count();
  }

  private static List<String> describe(ListRangesResponse r) {
    List<String> out = new ArrayList<>();
    for (RangeInfo info : r.getRangesList()) {
      out.add(Ranges.describe(info.getDesc()) + " leader " + info.getLeader());
    }
    return out;
  }

  private static String key(int i) {
    return String.format("key-%04d", i);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}

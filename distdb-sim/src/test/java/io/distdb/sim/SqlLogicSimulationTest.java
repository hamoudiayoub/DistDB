package io.distdb.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.sql.Engine;
import io.distdb.sql.Session;
import io.distdb.sql.kv.Awaiter;
import io.distdb.sql.kv.TxnKvStore;
import io.distdb.sql.testing.SqlLogic;
import io.distdb.txn.TxnClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * The sqllogictest files, whose expected results came from PostgreSQL, run on the sharded cluster
 * (three PD nodes, three data nodes, Raft, Percolator) in the deterministic simulator.
 */
class SqlLogicSimulationTest {

  private static Engine gateway(ShardedKvSimulation.Cluster cluster) {
    Awaiter awaiter =
        new Awaiter() {
          @Override
          public <T> T await(CompletableFuture<T> f) {
            return cluster.await(f);
          }
        };
    return new Engine(new TxnKvStore(cluster.txnClient(t -> TxnClient.CrashPoint.NONE), awaiter));
  }

  @Test
  void everyFileMatchesPostgresOnTheCluster() {
    int statements = 0;
    int queries = 0;
    for (String file : SqlLogic.FILES) {
      ShardedKvSimulation.Cluster cluster = ShardedKvSimulation.Cluster.start(file.hashCode());
      SqlLogic.Counts c =
          SqlLogic.run(file, SqlLogic.load(file), SqlLogic.of(gateway(cluster).session()));
      statements += c.statements();
      queries += c.queries();
    }
    System.out.println(
        "sqllogictest on the cluster: "
            + SqlLogic.FILES.size()
            + " files, "
            + statements
            + " statements, "
            + queries
            + " queries, all as PostgreSQL answered");
  }

  /**
   * The same files where every table soon spans many ranges, split while the statements run: the
   * fragments fan out, partial aggregates merge across ranges, and stale range caches make requests
   * clip at a range's end and resume in the next.
   */
  @Test
  void everyFileMatchesPostgresOnManySplittingRanges() {
    int queries = 0;
    int ranges = 0;
    for (String file : SqlLogic.FILES) {
      ShardedKvSimulation.Cluster cluster =
          ShardedKvSimulation.Cluster.start(
              file.hashCode() + 1L, 4, 32, ShardedKvSimulation.Wire.NONE);
      queries +=
          SqlLogic.run(file, SqlLogic.load(file), SqlLogic.of(gateway(cluster).session()))
              .queries();
      ranges = Math.max(ranges, cluster.rangeCount());
    }
    assertThat(ranges).as("ranges in the busiest file").isGreaterThan(20);
    System.out.println(
        "sqllogictest on splitting ranges: "
            + queries
            + " queries, all as PostgreSQL answered, up to "
            + ranges
            + " ranges");
  }

  @Test
  void everyGatewaySeesEveryTable() {
    ShardedKvSimulation.Cluster cluster = ShardedKvSimulation.Cluster.start(42);
    Session a = gateway(cluster).session();
    Session b = gateway(cluster).session();
    a.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
    a.execute("INSERT INTO t VALUES (1, 'from a')");
    b.execute("INSERT INTO t VALUES (2, 'from b')");
    b.execute("CREATE INDEX t_v ON t (v)");
    List<List<Object>> rows = new ArrayList<>();
    for (Object[] r : a.executeOne("SELECT id, v FROM t WHERE v = 'from b'").rows()) {
      rows.add(Arrays.asList(r));
    }
    assertThat(rows).containsExactly(List.of(2L, "from b"));
    a.execute("DROP TABLE t");
    assertThat(b.execute("CREATE TABLE t (x INT)")).hasSize(1);
  }
}

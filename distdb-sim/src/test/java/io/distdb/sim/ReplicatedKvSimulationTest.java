package io.distdb.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The Phase 5 gate: client histories recorded under faults are linearizable, no write takes effect
 * twice or contradicts what its client was told, and replicas converge.
 *
 * <p>{@code -Ddistdb.sim.kvSeeds=N} runs seeds 1..N (default 30); {@code -Ddistdb.sim.kvSeed=S}
 * replays seed S alone.
 */
class ReplicatedKvSimulationTest {

  @Test
  void clientHistoriesUnderFaultsAreLinearizable() {
    Long only = Long.getLong("distdb.sim.kvSeed");
    int seeds = Integer.getInteger("distdb.sim.kvSeeds", 30);
    long first = only == null ? 1 : only;
    long last = only == null ? seeds : only;
    long ops = 0;
    long acked = 0;
    long reads = 0;
    long unknown = 0;
    long failed = 0;
    long powerCuts = 0;
    long leaderCrashes = 0;
    long diskFailures = 0;
    long partitions = 0;
    List<Long> election = new ArrayList<>();
    List<Long> visible = new ArrayList<>();
    for (long seed = first; seed <= last; seed++) {
      ReplicatedKvSimulation.Report r;
      try {
        r = ReplicatedKvSimulation.run(seed, ReplicatedKvSimulation.Options.varied(seed));
      } catch (AssertionError | RuntimeException e) {
        throw new AssertionError(
            "seed " + seed + " failed; replay with -Ddistdb.sim.kvSeed=" + seed, e);
      }
      ops += r.operations();
      acked += r.writesAcknowledged();
      reads += r.readsAnswered();
      unknown += r.unknownOutcomes();
      failed += r.certainlyFailed();
      powerCuts += r.powerCuts();
      leaderCrashes += r.leaderCrashes();
      diskFailures += r.diskFailures();
      partitions += r.partitions();
      election.addAll(r.electionFailoverMillis());
      visible.addAll(r.clientFailoverMillis());
    }
    System.out.printf(
        "ReplicatedKvSimulationTest: %d seeds; %d operations checked for linearizability"
            + " (%d writes acknowledged, %d reads, %d writes of unknown outcome, %d certainly"
            + " failed); power cuts %d (leader %d), disk failures %d, partitions %d;"
            + " failover to a new leader %s, to the next successful write %s%n",
        last - first + 1,
        ops,
        acked,
        reads,
        unknown,
        failed,
        powerCuts,
        leaderCrashes,
        diskFailures,
        partitions,
        percentiles(election),
        percentiles(visible));
    assertThat(acked).isPositive();
    if (only == null) {
      assertThat(reads).isPositive();
      assertThat(unknown).isPositive();
      assertThat(leaderCrashes).isPositive();
      assertThat(diskFailures).isPositive();
      assertThat(partitions).isPositive();
    }
  }

  @Test
  void leaderFailoverIsMeasured() {
    List<Long> election = new ArrayList<>();
    List<Long> visible = new ArrayList<>();
    for (long seed = 1; seed <= 5; seed++) {
      ReplicatedKvSimulation.Report r = ReplicatedKvSimulation.measureFailover(seed, 3);
      election.addAll(r.electionFailoverMillis());
      visible.addAll(r.clientFailoverMillis());
    }
    System.out.printf(
        "ReplicatedKvSimulationTest failover (3 nodes, 10 ms ticks, elections after 100-200 ms):"
            + " new leader %s, next successful write %s%n",
        percentiles(election), percentiles(visible));
    assertThat(election).hasSizeGreaterThan(20);
    long median = sorted(election).get(election.size() / 2);
    assertThat(median).as("median election failover, ms").isBetween(100L, 1000L);
  }

  @Test
  void aSeedReplaysExactly() {
    ReplicatedKvSimulation.Report a =
        ReplicatedKvSimulation.run(3, ReplicatedKvSimulation.Options.varied(3));
    ReplicatedKvSimulation.Report b =
        ReplicatedKvSimulation.run(3, ReplicatedKvSimulation.Options.varied(3));
    assertThat(a).isEqualTo(b);
  }

  private static String percentiles(List<Long> millis) {
    if (millis.isEmpty()) {
      return "(no samples)";
    }
    List<Long> s = sorted(millis);
    return String.format(
        "p50 %d ms, p99 %d ms, max %d ms (n=%d)",
        s.get(s.size() / 2),
        s.get((int) Math.min(s.size() - 1, s.size() * 0.99)),
        s.getLast(),
        s.size());
  }

  private static List<Long> sorted(List<Long> values) {
    List<Long> s = new ArrayList<>(values);
    s.sort(null);
    return s;
  }
}

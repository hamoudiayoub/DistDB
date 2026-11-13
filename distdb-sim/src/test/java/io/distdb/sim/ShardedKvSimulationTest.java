package io.distdb.sim;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The Phase 6 gate. CI runs 20 seeds; the gate is {@code -Ddistdb.sim.shardSeeds=500}. Replay one
 * seed with {@code -Ddistdb.sim.shardSeed=S}.
 */
class ShardedKvSimulationTest {

  @Test
  void clientsStayLinearizableWhileRangesSplitAndMoveUnderFaults() {
    String only = System.getProperty("distdb.sim.shardSeed");
    long first = only != null ? Long.parseLong(only) : 1;
    long last = only != null ? first : Long.getLong("distdb.sim.shardSeeds", 20);
    long ops = 0;
    long acked = 0;
    long reads = 0;
    long unknown = 0;
    long ranges = 0;
    long splits = 0;
    long changes = 0;
    long destroyed = 0;
    long powerCuts = 0;
    long pdCrashes = 0;
    long diskFailures = 0;
    long partitions = 0;
    for (long seed = first; seed <= last; seed++) {
      ShardedKvSimulation.Report r;
      try {
        r = ShardedKvSimulation.run(seed, ShardedKvSimulation.Options.varied(seed));
      } catch (AssertionError | RuntimeException e) {
        throw new AssertionError(
            "seed " + seed + " failed; replay with -Ddistdb.sim.shardSeed=" + seed, e);
      }
      ops += r.operations();
      acked += r.writesAcknowledged();
      reads += r.readsAnswered();
      unknown += r.unknownOutcomes();
      ranges += r.ranges();
      splits += r.splits();
      changes += r.membershipChanges();
      destroyed += r.replicasDestroyed();
      powerCuts += r.powerCuts();
      pdCrashes += r.pdLeaderCrashes();
      diskFailures += r.diskFailures();
      partitions += r.partitions();
    }
    System.out.printf(
        "ShardedKvSimulationTest: %d seeds; %d operations linearizable (%d acknowledged writes,"
            + " %d reads, %d unknown outcomes); %d ranges at the end, %d splits, %d membership"
            + " changes, %d replicas destroyed; %d power cuts (%d of the PD leader), %d disk"
            + " failures, %d partitions%n",
        last - first + 1,
        ops,
        acked,
        reads,
        unknown,
        ranges,
        splits,
        changes,
        destroyed,
        powerCuts,
        pdCrashes,
        diskFailures,
        partitions);
    assertThat(acked).isPositive();
    if (only == null) {
      // The reshuffling and the faults must really have happened.
      assertThat(splits).isPositive();
      assertThat(changes).isPositive();
      assertThat(destroyed).isPositive();
      assertThat(powerCuts).isPositive();
      assertThat(pdCrashes).isPositive();
      assertThat(diskFailures).isPositive();
      assertThat(partitions).isPositive();
    }
  }

  @Test
  void aSeedReplaysExactly() {
    ShardedKvSimulation.Report a =
        ShardedKvSimulation.run(3, ShardedKvSimulation.Options.varied(3));
    ShardedKvSimulation.Report b =
        ShardedKvSimulation.run(3, ShardedKvSimulation.Options.varied(3));
    assertThat(b).isEqualTo(a);
  }
}

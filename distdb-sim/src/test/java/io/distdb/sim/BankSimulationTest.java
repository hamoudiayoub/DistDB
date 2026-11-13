package io.distdb.sim;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The Phase 7 gate: bank transfers in transactions on a sharded cluster under faults, while ranges
 * split and move, clients die mid-commit, and GC drops old versions. Every audit (a snapshot of
 * every account) must hold exactly the money the bank opened with. CI runs 20 seeds; the gate is
 * {@code -Ddistdb.sim.bankSeeds=2000}. Replay one seed with {@code -Ddistdb.sim.bankSeed=S}.
 */
class BankSimulationTest {

  @Test
  void transfersNeverCreateOrDestroyMoney() {
    String only = System.getProperty("distdb.sim.bankSeed");
    long first = only != null ? Long.parseLong(only) : 1;
    long last = only != null ? first : Long.getLong("distdb.sim.bankSeeds", 20);
    long transfers = 0;
    long conflicts = 0;
    long unknown = 0;
    long crashes = 0;
    long audits = 0;
    long gc = 0;
    long splits = 0;
    long changes = 0;
    long powerCuts = 0;
    long pdCrashes = 0;
    long diskFailures = 0;
    long partitions = 0;
    for (long seed = first; seed <= last; seed++) {
      ShardedKvSimulation.Report r;
      try {
        r = ShardedKvSimulation.run(seed, ShardedKvSimulation.Options.bank(seed));
      } catch (AssertionError | RuntimeException e) {
        throw new AssertionError(
            "seed " + seed + " failed; replay with -Ddistdb.sim.bankSeed=" + seed, e);
      }
      transfers += r.transfers();
      conflicts += r.transferConflicts();
      unknown += r.transferUnknown();
      crashes += r.clientCrashes();
      audits += r.audits();
      gc += r.gcRounds();
      splits += r.splits();
      changes += r.membershipChanges();
      powerCuts += r.powerCuts();
      pdCrashes += r.pdLeaderCrashes();
      diskFailures += r.diskFailures();
      partitions += r.partitions();
    }
    System.out.printf(
        "BankSimulationTest: %d seeds; %d transfers committed, %d audits balanced; %d transactions"
            + " gave up on conflicts, %d of unknown outcome, %d clients killed mid-commit; %d GC"
            + " rounds; %d splits, %d membership changes; %d power cuts (%d of the PD leader), %d"
            + " disk failures, %d partitions%n",
        last - first + 1,
        transfers,
        audits,
        conflicts,
        unknown,
        crashes,
        gc,
        splits,
        changes,
        powerCuts,
        pdCrashes,
        diskFailures,
        partitions);
    assertThat(transfers).isPositive();
    assertThat(audits).isPositive();
    if (only == null) {
      assertThat(crashes).as("clients died mid-commit").isPositive();
      assertThat(gc).as("GC ran").isPositive();
      assertThat(splits).isPositive();
      assertThat(powerCuts).isPositive();
      assertThat(partitions).isPositive();
    }
  }
}

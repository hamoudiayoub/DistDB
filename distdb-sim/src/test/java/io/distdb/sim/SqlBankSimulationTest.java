package io.distdb.sim;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The Phase 8 gate: a bank in SQL, through partitions, power cuts, disk failures, splits, moves and
 * GC, with a secondary index built and dropped under the load.
 *
 * <p>{@code -Ddistdb.sim.sqlSeeds=N} runs seeds 1..N (default 10); {@code -Ddistdb.sim.sqlSeed=S}
 * runs one.
 */
class SqlBankSimulationTest {

  @Test
  void moneyIsConservedAndIndexesStayExact() {
    Long one = Long.getLong("distdb.sim.sqlSeed");
    int seeds = Integer.getInteger("distdb.sim.sqlSeeds", 10);
    long transfers = 0;
    long audits = 0;
    long indexAudits = 0;
    long opened = 0;
    long closed = 0;
    long built = 0;
    long dropped = 0;
    long buildFailed = 0;
    long conflicts = 0;
    long unknown = 0;
    long failed = 0;
    long powerCuts = 0;
    long partitions = 0;
    long diskFailures = 0;
    long splits = 0;
    long moves = 0;
    long from = one == null ? 1 : one;
    long to = one == null ? seeds : one;
    for (long seed = from; seed <= to; seed++) {
      SqlBankSimulation.Report r;
      try {
        r = SqlBankSimulation.run(seed, SqlBankSimulation.Options.of(seed));
      } catch (AssertionError e) {
        throw new AssertionError("seed " + seed + " failed: " + e.getMessage(), e);
      }
      transfers += r.transfers();
      audits += r.audits();
      indexAudits += r.indexAudits();
      opened += r.opened();
      closed += r.closed();
      built += r.indexesBuilt();
      dropped += r.indexesDropped();
      buildFailed += r.indexBuildsFailed();
      conflicts += r.conflicts();
      unknown += r.unknown();
      failed += r.failed();
      powerCuts += r.cluster().powerCuts();
      partitions += r.cluster().partitions();
      diskFailures += r.cluster().diskFailures();
      splits += r.cluster().splits();
      moves += r.cluster().membershipChanges();
    }
    System.out.printf(
        "SQL bank, %d seeds: %d transfers committed, %d accounts opened and %d closed, %d audits and"
            + " %d index audits exact;"
            + " %d index builds and %d drops under load (%d builds cut short); %d serialization"
            + " failures, %d unknown outcomes, %d other failures; faults: %d power cuts, %d"
            + " partitions, %d disk failures; %d splits, %d membership changes%n",
        to - from + 1,
        transfers,
        opened,
        closed,
        audits,
        indexAudits,
        built,
        dropped,
        buildFailed,
        conflicts,
        unknown,
        failed,
        powerCuts,
        partitions,
        diskFailures,
        splits,
        moves);
    assertThat(transfers).isPositive();
    assertThat(audits).isPositive();
  }
}

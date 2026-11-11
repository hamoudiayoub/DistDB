package io.distdb.sim;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The Phase 4 gate: many seeds of {@link RaftSimulation}, each with partitions, power cuts, disk
 * failures, lossy and reordering links and membership changes, must keep every safety property and
 * recover once the faults stop.
 *
 * <p>{@code -Ddistdb.sim.raftSeeds=N} runs seeds 1..N (default 100); {@code
 * -Ddistdb.sim.raftSeed=S} replays seed S alone.
 */
class RaftSimulationTest {

  @Test
  void raftStaysSafeAndRecoversUnderFaults() {
    Long only = Long.getLong("distdb.sim.raftSeed");
    int seeds = Integer.getInteger("distdb.sim.raftSeeds", 100);
    long first = only == null ? 1 : only;
    long last = only == null ? seeds : only;
    long committed = 0;
    long leaders = 0;
    long proposals = 0;
    long reads = 0;
    long powerCuts = 0;
    long diskFailures = 0;
    long partitions = 0;
    long snapshots = 0;
    long configChanges = 0;
    long sent = 0;
    long lost = 0;
    for (long seed = first; seed <= last; seed++) {
      RaftSimulation.Report r;
      try {
        r = RaftSimulation.run(seed, RaftSimulation.Options.varied(seed));
      } catch (AssertionError | RuntimeException e) {
        throw new AssertionError(
            "seed " + seed + " failed; replay with -Ddistdb.sim.raftSeed=" + seed, e);
      }
      committed += r.committed();
      leaders += r.leaderChanges();
      proposals += r.proposalsAccepted();
      reads += r.readsChecked();
      powerCuts += r.powerCuts();
      diskFailures += r.diskFailures();
      partitions += r.partitions();
      snapshots += r.snapshotsInstalled();
      configChanges += r.configChanges();
      sent += r.messagesSent();
      lost += r.messagesLost();
    }
    System.out.printf(
        "RaftSimulationTest: %d seeds; entries committed %d, leaders elected %d, proposals %d,"
            + " linearizable reads checked %d; power cuts %d, disk failures %d, partitions %d,"
            + " snapshots installed %d, membership changes %d; messages %d, lost %d%n",
        last - first + 1,
        committed,
        leaders,
        proposals,
        reads,
        powerCuts,
        diskFailures,
        partitions,
        snapshots,
        configChanges,
        sent,
        lost);
    assertThat(committed).isPositive();
    if (only == null) {
      // The faults must really have happened, or the properties were checked against nothing.
      assertThat(powerCuts).isPositive();
      assertThat(diskFailures).isPositive();
      assertThat(partitions).isPositive();
      assertThat(snapshots).isPositive();
      assertThat(configChanges).isPositive();
      assertThat(reads).isPositive();
      assertThat(leaders).isGreaterThan(last - first + 1);
    }
  }

  @Test
  void aSeedReplaysExactly() {
    RaftSimulation.Report a = RaftSimulation.run(7, RaftSimulation.Options.varied(7));
    RaftSimulation.Report b = RaftSimulation.run(7, RaftSimulation.Options.varied(7));
    assertThat(a).isEqualTo(b);
  }
}

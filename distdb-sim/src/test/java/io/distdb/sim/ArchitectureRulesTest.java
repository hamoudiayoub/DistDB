package io.distdb.sim;

import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.EvaluationResult;
import io.distdb.kv.fixture.KvThing;
import io.distdb.raft.fixture.CleanRaftFixture;
import io.distdb.raft.fixture.NondeterministicRaftFixture;
import io.distdb.storage.fixture.StorageReachingIntoKvFixture;
import org.junit.jupiter.api.Test;

/**
 * Most core modules are still empty, so the rules pass vacuously on production code. These tests
 * run them against deliberate violations to prove they are not vacuous.
 */
class ArchitectureRulesTest {

  private static EvaluationResult evaluate(
      com.tngtech.archunit.lang.ArchRule rule, Class<?>... classes) {
    JavaClasses imported = new ClassFileImporter().importClasses(classes);
    return rule.evaluate(imported);
  }

  @Test
  void determinismRuleCatchesClockRandomnessSleepAndThreads() {
    EvaluationResult result =
        evaluate(ArchitectureRules.CORE_IS_DETERMINISTIC, NondeterministicRaftFixture.class);
    assertThat(result.hasViolation()).isTrue();
    String report = String.join("\n", result.getFailureReport().getDetails());
    assertThat(report)
        .contains("System.currentTimeMillis()")
        .contains("System.nanoTime()")
        .contains("Thread.sleep(long)")
        .contains("java.util.Random")
        .contains("Instant.now()")
        .contains("Thread.<init>");
  }

  @Test
  void determinismRuleAcceptsCodeThatUsesTheSeams() {
    assertThat(
            evaluate(ArchitectureRules.CORE_IS_DETERMINISTIC, CleanRaftFixture.class)
                .hasViolation())
        .isFalse();
  }

  @Test
  void layerRuleCatchesAnUpwardDependency() {
    EvaluationResult result =
        evaluate(ArchitectureRules.LAYERS, StorageReachingIntoKvFixture.class, KvThing.class);
    assertThat(result.hasViolation()).isTrue();
  }
}

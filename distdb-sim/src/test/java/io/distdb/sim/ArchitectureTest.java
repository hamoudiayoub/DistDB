package io.distdb.sim;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/** Checks every production class in the repository against the architecture rules. */
@AnalyzeClasses(packages = "io.distdb", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  @ArchTest static final ArchRule layers = ArchitectureRules.LAYERS;

  @ArchTest static final ArchRule coreIsDeterministic = ArchitectureRules.CORE_IS_DETERMINISTIC;
}

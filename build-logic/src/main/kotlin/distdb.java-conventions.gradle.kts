// Shared build settings for every DistDB module.
plugins {
  `java-library`
  jacoco
  id("com.diffplug.spotless")
}

val libs = the<VersionCatalogsExtension>().named("libs")

fun lib(alias: String) = libs.findLibrary(alias).get()

fun version(alias: String) = libs.findVersion(alias).get().requiredVersion

java { toolchain { languageVersion = JavaLanguageVersion.of(25) } }

tasks.withType<JavaCompile>().configureEach {
  options.encoding = "UTF-8"
  options.release = 25
  options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

dependencies {
  testImplementation(platform(lib("junit-bom")))
  testImplementation(lib("junit-jupiter"))
  testImplementation(lib("assertj-core"))
  testImplementation(lib("jqwik"))
  testRuntimeOnly(lib("junit-platform-launcher"))
}

tasks.withType<Test>().configureEach {
  useJUnitPlatform()
  // Keep jqwik's failure database out of the source tree.
  systemProperty("jqwik.database", layout.buildDirectory.file("jqwik-database").get().asFile.path)
  // Pass -Ddistdb.* through to tests, e.g. ./gradlew test -Ddistdb.storage.modelOps=2000000
  providers.systemPropertiesPrefixedBy("distdb.").get().forEach { (k, v) -> systemProperty(k, v) }
  testLogging {
    events("failed")
    exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
  }
  finalizedBy(tasks.named("jacocoTestReport"))
}

jacoco { toolVersion = version("jacoco") }

tasks.named<JacocoReport>("jacocoTestReport") { reports { xml.required = true } }

spotless {
  java {
    // Only hand-written code: generated sources (protoc's, under build/) are not ours to format.
    target("src/**/*.java")
    googleJavaFormat(version("googleJavaFormat"))
    removeUnusedImports()
    trimTrailingWhitespace()
    endWithNewline()
  }
}

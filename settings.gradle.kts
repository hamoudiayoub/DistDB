pluginManagement { includeBuild("build-logic") }

plugins { id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0" }

dependencyResolutionManagement {
  repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
  repositories { mavenCentral() }
}

rootProject.name = "distdb"

include(
  "distdb-common",
  "distdb-env",
  "distdb-storage",
  "distdb-raft",
  "distdb-rpc",
  "distdb-kv",
  "distdb-pd",
  "distdb-client",
  "distdb-txn",
  "distdb-sql",
  "distdb-pgwire",
  "distdb-shell",
  "distdb-server",
  "distdb-sim",
  "distdb-bench",
)

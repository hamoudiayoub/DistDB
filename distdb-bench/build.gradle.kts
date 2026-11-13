plugins {
  id("distdb.java-conventions")
  application
}

dependencies {
  implementation(project(":distdb-server"))
  implementation(project(":distdb-kv"))
  implementation(project(":distdb-client"))
  implementation(project(":distdb-storage"))
  // The linearizability checker, for the chaos suite's register histories.
  implementation(project(":distdb-sim"))
  // TPC-C and the chaos suite talk SQL through PostgreSQL's driver, like any application.
  implementation(libs.postgresql.jdbc)
}

application {
  mainClass = "io.distdb.bench.BenchMain"
  applicationName = "distdb-bench"
  applicationDefaultJvmArgs = listOf("-Xmx1g")
}

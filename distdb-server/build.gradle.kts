plugins {
  id("distdb.java-conventions")
  application
}

dependencies {
  implementation(project(":distdb-pgwire"))
  implementation(project(":distdb-kv"))
  implementation(project(":distdb-pd"))
  implementation(project(":distdb-client"))
  implementation(project(":distdb-txn"))
  implementation(project(":distdb-sql"))
  implementation(project(":distdb-shell"))
  // PostgreSQL's JDBC driver, for the multi-process SQL test.
  testImplementation(libs.postgresql.jdbc)
}

application {
  mainClass = "io.distdb.server.Main"
  applicationName = "distdb"
  // The shell's terminal support (JLine) calls native code: allow it without a warning.
  applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
}

plugins { id("distdb.java-conventions") }

dependencies {
  // Line editing: history, completion, highlighting as you type, on every terminal.
  implementation(libs.jline)
  // The shell talks to a real wire-protocol server in its tests.
  testImplementation(project(":distdb-pgwire"))
  testImplementation(testFixtures(project(":distdb-sql")))
}

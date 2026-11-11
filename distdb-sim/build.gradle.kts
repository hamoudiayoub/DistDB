plugins { id("distdb.java-conventions") }

dependencies {
  implementation(project(":distdb-server"))
  implementation(project(":distdb-kv"))
  implementation(project(":distdb-pd"))
  implementation(project(":distdb-pgwire"))
  // The simulated clock, scheduler and file system.
  implementation(testFixtures(project(":distdb-env")))
  testImplementation(libs.archunit.junit5)
}

plugins { id("distdb.java-conventions") }

dependencies {
  implementation(project(":distdb-server"))
  implementation(project(":distdb-kv"))
  implementation(project(":distdb-pd"))
  implementation(project(":distdb-pgwire"))
  testImplementation(libs.archunit.junit5)
  testImplementation(testFixtures(project(":distdb-env")))
}

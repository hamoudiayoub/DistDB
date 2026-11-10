plugins { id("distdb.java-conventions") }

dependencies {
  api(project(":distdb-rpc"))
  testImplementation(testFixtures(project(":distdb-env")))
}

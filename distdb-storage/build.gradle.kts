plugins { id("distdb.java-conventions") }

dependencies {
  api(project(":distdb-env"))
  testImplementation(testFixtures(project(":distdb-env")))
}

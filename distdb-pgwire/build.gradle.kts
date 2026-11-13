plugins { id("distdb.java-conventions") }

dependencies {
  api(project(":distdb-sql"))
  testImplementation(testFixtures(project(":distdb-sql")))
  testImplementation(libs.postgresql.jdbc)
}

plugins {
  id("distdb.java-conventions")
  `java-test-fixtures`
}

dependencies {
  api(project(":distdb-txn"))
  // PostgreSQL's driver, to record expected results from a real PostgreSQL (SqlLogicTest).
  testImplementation(libs.postgresql.jdbc)
}

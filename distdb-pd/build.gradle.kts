plugins { id("distdb.java-conventions") }

dependencies {
  // The PD's own group is a Phase 5 replicated KV group, and it allocates range ids through a
  // KV client of that group.
  api(project(":distdb-kv"))
  api(project(":distdb-client"))
  testImplementation(testFixtures(project(":distdb-env")))
}

plugins { id("distdb.java-conventions") }

dependencies {
  api(project(":distdb-storage"))
  api(project(":distdb-raft"))
  api(project(":distdb-rpc"))
}

plugins {
  id("distdb.java-conventions")
  application
}

dependencies {
  implementation(project(":distdb-pgwire"))
  implementation(project(":distdb-kv"))
  implementation(project(":distdb-pd"))
  implementation(project(":distdb-client"))
}

application {
  mainClass = "io.distdb.server.Main"
  applicationName = "distdb"
}

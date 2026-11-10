plugins { id("distdb.protobuf-conventions") }

dependencies {
  api(project(":distdb-raft"))
  // The generated messages are part of this module's API.
  api(libs.protobuf.java)
  implementation(libs.grpc.stub)
  implementation(libs.grpc.protobuf)
  implementation(libs.grpc.netty.shaded)
}

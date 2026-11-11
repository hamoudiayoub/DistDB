plugins { id("distdb.protobuf-conventions") }

dependencies {
  api(project(":distdb-raft"))
  // The generated messages are part of this module's API.
  api(libs.protobuf.java)
  // GrpcKvServer and GrpcTransport expose gRPC types (BindableService) to their users.
  api(libs.grpc.stub)
  implementation(libs.grpc.protobuf)
  implementation(libs.grpc.netty.shaded)
}

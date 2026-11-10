// Protocol Buffers and gRPC code generation, for modules with .proto files in src/main/proto.
plugins {
  id("distdb.java-conventions")
  id("com.google.protobuf")
}

val libs = the<VersionCatalogsExtension>().named("libs")

fun version(alias: String) = libs.findVersion(alias).get().requiredVersion

protobuf {
  protoc { artifact = "com.google.protobuf:protoc:${version("protobuf")}" }
  plugins { create("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:${version("grpc")}" } }
  generateProtoTasks { all().configureEach { plugins { create("grpc") } } }
}

plugins { `kotlin-dsl` }

// Every third-party plugin a module applies is loaded here, so all modules share one classloader
// for it. Applying one with a version in a module's own plugins block gives that module a
// separate classloader, which breaks plugins that share a build service across modules (Spotless).
dependencies {
  implementation(libs.spotless.gradle)
  implementation(libs.protobuf.gradle)
}

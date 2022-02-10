plugins {
  id("otel.library-instrumentation")
}

val grpcVersion = "1.6.0"

dependencies {
  library("io.grpc:grpc-core:$grpcVersion")

  library("cloud.filibuster:instrumentation:0.19-SNAPSHOT")

  testLibrary("io.grpc:grpc-netty:$grpcVersion")
  testLibrary("io.grpc:grpc-protobuf:$grpcVersion")
  testLibrary("io.grpc:grpc-services:$grpcVersion")
  testLibrary("io.grpc:grpc-stub:$grpcVersion")

  library("com.github.cliftonlabs:json-simple:2.1.2")
  library("org.json:json:20210307")

  testImplementation("org.assertj:assertj-core")
  testImplementation(project(":instrumentation:grpc-1.6:testing"))
}

repositories {
  mavenCentral()

  maven {
    url = uri("https://maven.pkg.github.com/filibuster-testing/filibuster-java")
    credentials {
      username = project.findProperty("gpr.user") as String? ?: System.getenv("GITHUB_USERNAME")
      password = project.findProperty("gpr.key") as String? ?: System.getenv("GITHUB_TOKEN")
    }
  }
}
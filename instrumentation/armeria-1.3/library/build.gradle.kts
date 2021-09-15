plugins {
  id("otel.library-instrumentation")
  id("otel.nullaway-conventions")
}

dependencies {
  library("com.linecorp.armeria:armeria:1.3.0")
  library("cloud.filibuster:instrumentation:0.2-SNAPSHOT")

  testImplementation(project(":instrumentation:armeria-1.3:testing"))
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
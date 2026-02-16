plugins {
  id("java")
  id("maven-publish")
}

group = "de.lukasbreuer"
version = "1.0.0-SNAPSHOT"

publishing {
  publications {
    create<MavenPublication>("library") {
      from(components["java"])
    }
  }
  repositories {
    maven {
      name = "GitHubPackages"
      url = uri("https://maven.pkg.github.com/breuerlukas/cassandra-orm")
      credentials {
        username = (project.findProperty("gpr.user") ?: System.getenv("GITHUB_USERNAME")) as String?
        password = (project.findProperty("gpr.token") ?: System.getenv("GITHUB_TOKEN")) as String?
      }
    }
  }
}

repositories {
  mavenCentral()
  maven {
    url = uri("https://maven.pkg.github.com/breuerlukas/iteron")
    credentials {
      username = project.findProperty("gpr.user")?.toString() ?: System.getenv("GITHUB_USERNAME")
      password = project.findProperty("gpr.token")?.toString() ?: System.getenv("GITHUB_TOKEN")
    }
  }
}

dependencies {
  testImplementation(platform("org.junit:junit-bom:6.0.3"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")

  implementation("com.google.inject:guice:7.0.0")

  implementation("com.google.guava:guava:33.5.0-jre")

  implementation("org.projectlombok:lombok:1.18.42")
  annotationProcessor("org.projectlombok:lombok:1.18.42")
  testImplementation("org.projectlombok:lombok:1.18.42")
  testAnnotationProcessor("org.projectlombok:lombok:1.18.42")

  implementation("com.datastax.oss:java-driver-core:4.17.0")

  implementation("org.json:json:20251224")
  implementation("commons-io:commons-io:2.21.0")

  implementation("de.lukasbreuer:iteron:1.0.0-SNAPSHOT")
}

tasks.test {
  useJUnitPlatform()
}
# Cassandra ORM

This project is my own implementation of an ORM for the Cassandra database for the Java programming language.

## Status

|      | Build Status                                                                                                |
|------|-------------------------------------------------------------------------------------------------------------|
| main | ![Java CI with Gradle](https://github.com/breuerlukas/cassandra-orm/actions/workflows/gradle.yml/badge.svg) |

## Installation

```
repositories {
  maven {
    url = uri("https://maven.pkg.github.com/breuerlukas/cassandra-orm")
    credentials {
      username = project.findProperty("gpr.user")?.toString() ?: System.getenv("GITHUB_USERNAME")
      password = project.findProperty("gpr.token")?.toString() ?: System.getenv("GITHUB_TOKEN")
    }
  }
}

dependencies {
  implementation("de.lukasbreuer:cassandra-orm:1.0.0-SNAPSHOT")
}
```

## License

[GPL](https://github.com/breuerlukas/cassandra-orm/blob/main/LICENSE.md)
package de.lukasbreuer.cassandra.orm;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(staticName = "create")
public final class DatabaseInjectionModule extends AbstractModule {
  @Provides
  @Singleton
  DatabaseConfiguration provideDatabaseConfiguration() throws Exception {
    return DatabaseConfiguration.createAndLoad();
  }

  @Provides
  @Singleton
  DatabaseConnection provideDatabaseConnection(
    DatabaseConfiguration configuration
  ) {
    var databaseConnection = DatabaseConnection.create(configuration);
    databaseConnection.connect();
    return databaseConnection;
  }

  @Provides
  @Singleton
  DatabaseKeyspace provideDatabaseKeyspace(DatabaseConnection connection) {
    var databaseKeyspace = DatabaseKeyspace.create(connection, "dulno",
      "SimpleStrategy", 2);
    databaseKeyspace.createIfNotExists().join();
    databaseKeyspace.use();
    return databaseKeyspace;
  }
}

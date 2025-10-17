package de.lukasbreuer.cassandra.orm.skeleton;

import java.util.concurrent.CompletableFuture;

public interface DroppableDatabaseTable extends AbstractDatabaseTable,
  ViewableDatabaseTable
{

  /**
   * Deletes the database table and all its content
   */
  default CompletableFuture<Void> drop() {
    return dropAllViews().thenCompose(value -> drop(""));
  }

  /**
   * Deletes the database table and all its content only if it exists
   */
  default CompletableFuture<Void> dropIfExists() {
    return dropAllViews().thenCompose(value -> drop("IF EXISTS "));
  }

  default CompletableFuture<Void> drop(String addition) {
    var query = new StringBuilder("DROP TABLE ");
    query.append(addition);
    query.append(fullName());
    query.append(";");
    unregisterTable();
    return connection().execute(query).thenApply(value -> null);
  }

  /**
   * When this function is called the table will be unregistered from the keyspace
   */
  void unregisterTable();
}

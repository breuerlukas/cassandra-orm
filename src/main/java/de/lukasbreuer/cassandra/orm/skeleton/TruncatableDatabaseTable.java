package de.lukasbreuer.cassandra.orm.skeleton;

import java.util.concurrent.CompletableFuture;

public interface TruncatableDatabaseTable extends AbstractDatabaseTable {
  /**
   * Is used to clear a database table
   * @return A future that is completed when all data is cleared
   */
  default CompletableFuture<Void> truncate() {
    var query = new StringBuilder("TRUNCATE TABLE ");
    query.append(fullName());
    query.append(";");
    return connection().execute(query).thenApply(value -> null);
  }
}

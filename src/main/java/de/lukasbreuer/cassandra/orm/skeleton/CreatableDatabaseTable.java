package de.lukasbreuer.cassandra.orm.skeleton;

import de.lukasbreuer.cassandra.orm.DatabaseColumn;

import java.util.concurrent.CompletableFuture;

public interface CreatableDatabaseTable extends AbstractDatabaseTable {
  /**
   * Creates the database table even if it already exists
   * @return A future that is completed when creation is done
   */
  default CompletableFuture<Void> createAsync() {
    return createAsync("");
  }

  /**
   * Creates the database table only if it does not already exist
   * @return A future that is completed when creation is done
   */
  default CompletableFuture<Void> createAsyncIfNotExists() {
    return createAsync("IF NOT EXISTS ");
  }

  private CompletableFuture<Void> createAsync(String addition) {
    registerTable();
    if (isCreationAuthorised()) {
      return connection().execute(creationQuery(addition)).thenApply(value -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Creates the database table even if it already exists
   */
  default void create() {
    create("");
  }

  /**
   * Creates the database table only if it does not already exist
   */
  default void createIfNotExists() {
    create("IF NOT EXISTS ");
  }

  private void create(String addition) {
    registerTable();
    if (isCreationAuthorised()) {
      connection().executesSynchronously(creationQuery(addition));
    }
  }

  private String creationQuery(String addition) {
    var query = new StringBuilder("CREATE TABLE ");
    query.append(addition);
    query.append(fullName());
    query.append(" (");
    query.append(columnCompilation());
    query.append(")");
    query.append(clusteringOrder());
    query.append(";");
    return query.toString();
  }

  default boolean isCreationAuthorised() {
    return true;
  }

  /**
   * When this function is called the table will be registered in the keyspace
   */
  void registerTable();

  private String columnCompilation() {
    var compilation = new StringBuilder();
    for (var i = 0; i < columns().size(); i++) {
      compilation.append(columns().get(i).databaseEntry());
      compilation.append(", ");
    }
    compilation.append("PRIMARY KEY (");
    compilation.append(columnNameCompilation(columns().stream()
      .filter(column -> column.type().isPartitionKey()).toList(), "(", ")"));
    compilation.append(columnNameCompilation(columns().stream()
      .filter(column -> column.type().isClusteringKey()).toList(), ",", ""));
    compilation.append(columnNameCompilation(columns().stream()
      .filter(column -> column.type().isPrimaryKey()).toList(), "", ""));
    compilation.append(")");
    return compilation.toString();
  }

  private String clusteringOrder() {
    var order = columns().stream().filter(DatabaseColumn::hasOrder).toList();
    if (order.isEmpty()) {
      return "";
    }
    var result = new StringBuilder();
    result.append(" WITH CLUSTERING ORDER BY (");
    for (var i = 0; i < order.size(); i++) {
      if (i > 0) {
        result.append(", ");
      }
      var column = order.get(i);
      result.append(column.name());
      result.append(" ");
      result.append(column.order().value());
    }
    result.append(")");
    return result.toString();
  }
}

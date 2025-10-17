package de.lukasbreuer.cassandra.orm.skeleton;

import java.util.concurrent.CompletableFuture;

public interface IndexableDatabaseTable extends AbstractDatabaseTable {
  /**
   * Creates an index for a column of the database table even if it already exists
   * @param column The column for which the index is to be created
   * @return A future that is completed when creation is done
   */
  default CompletableFuture<Void> createIndexAsync(String column) {
    return createIndexAsync(column, "", "");
  }

  /**
   * Creates an index for a column of the database table even if it already exists
   * @param column The column for which the index is to be created
   * @param customType The custom type of the index
   * @return A future that is completed when creation is done
   */
  default CompletableFuture<Void> createIndexAsync(
    String column, String customType
  ) {
    return createIndexAsync(column, "", customType);
  }

  /**
   * Creates an index for a column of the database table if it does not already exist
   * @param column The column for which the index is to be created
   * @return A future that is completed when creation is done
   */
  default CompletableFuture<Void> createIndexAsyncIfNotExists(String column) {
    return createIndexAsync(column, "IF NOT EXISTS", "");
  }

  /**
   * Creates an index for a column of the database table if it does not already exist
   * @param column The column for which the index is to be created
   * @param customType The custom type of the index
   * @return A future that is completed when creation is done
   */
  default CompletableFuture<Void> createIndexAsyncIfNotExists(
    String column, String customType
  ) {
    return createIndexAsync(column, "IF NOT EXISTS", customType);
  }

  private CompletableFuture<Void> createIndexAsync(
    String column, String addition, String customType
  ) {
    return connection().execute(indexCreationQuery(column, addition,
      customType)).thenApply(value -> null);
  }

  /**
   * Creates an index for a column of the database table even if it already exists
   * @param column The column for which the index is to be created
   */
  default void createIndex(String column) {
    createIndex(column, "", "");
  }

  /**
   * Creates an index for a column of the database table even if it already exists
   * @param column The column for which the index is to be created
   * @param customType The custom type of the index
   */
  default void createIndex(String column, String customType) {
    createIndex(column, "", customType);
  }

  /**
   * Creates an index for a column of the database table if it does not already exist
   * @param column The column for which the index is to be created
   */
  default void createIndexIfNotExists(String column) {
    createIndex(column, "IF NOT EXISTS", "");
  }

  /**
   * Creates an index for a column of the database table if it does not already exist
   * @param column The column for which the index is to be created
   * @param customType The custom type of the index
   */
  default void createIndexIfNotExists(String column, String customType) {
    createIndex(column, "IF NOT EXISTS", customType);
  }

  private void createIndex(String column, String addition, String customType) {
    connection().executesSynchronously(indexCreationQuery(column, addition,
      customType));
  }

  private String indexCreationQuery(
    String column, String addition, String customType
  ) {
    var query = new StringBuilder("CREATE");
    if (!customType.isEmpty()) {
      query.append(" CUSTOM");
    }
    query.append(" INDEX ");
    query.append(addition);
    query.append(" ON ");
    query.append(fullName());
    query.append(" (");
    query.append(column);
    query.append(")");
    if (!customType.isEmpty()) {
      query.append(" USING ");
      query.append(customType);
    }
    query.append(";");
    return query.toString();
  }

  /**
   * Deletes a certain index
   * @param column The column whose index is to be deleted
   * @return A future that is completed when deletion is done
   */
  default CompletableFuture<Void> dropIndexAsync(String column) {
    return dropIndexAsync(column, "");
  }

  /**
   * Deletes a certain index
   * @param column The column whose index is to be deleted
   * @return A future that is completed when deletion is done
   */
  default CompletableFuture<Void> dropIndexAsyncIfExists(String column) {
    return dropIndexAsync(column, "IF EXISTS ");
  }

  private CompletableFuture<Void> dropIndexAsync(String column, String addition) {
    return connection().execute(indexDropQuery(column, addition))
      .thenApply(value -> null);
  }

  /**
   * Deletes a certain index
   * @param column The column whose index is to be deleted
   */
  default void dropIndex(String column) {
    dropIndex(column, "");
  }

  /**
   * Deletes a certain index
   * @param column The column whose index is to be deleted
   */
  default void dropIndexIfExists(String column) {
    dropIndex(column, "IF EXISTS ");
  }

  private void dropIndex(String column, String addition) {
    connection().executesSynchronously(indexDropQuery(column, addition));
  }

  private String indexDropQuery(
    String column, String addition
  ) {
    var query = new StringBuilder("DROP INDEX ");
    query.append(addition);
    query.append(fullName());
    query.append("_");
    query.append(column);
    query.append("_idx");
    query.append(";");
    return query.toString();
  }
}

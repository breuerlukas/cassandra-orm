package de.lukasbreuer.cassandra.orm;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(staticName = "create")
public final class DatabaseKeyspace {
  private final DatabaseConnection connection;
  private final String name;
  private final String replicationClass;
  private final int replicationFactor;
  private final LinkedHashSet<DatabaseTable> tables = new LinkedHashSet<>();

  /**
   * Creates keyspace even if it already exists
   * @return A future result that is completed when creation is completed
   */
  public CompletableFuture<Void> create() {
    return create("");
  }

  /**
   * Creates keyspace only if it not already exists
   * @return A future result that is completed when creation is completed
   */
  public CompletableFuture<Void> createIfNotExists() {
    return create("IF NOT EXISTS ");
  }

  private CompletableFuture<Void> create(String addition) {
    var query = new StringBuilder("CREATE KEYSPACE ");
    query.append(addition);
    query.append(name);
    query.append(" WITH REPLICATION = {'class' : '");
    query.append(replicationClass);
    query.append("', 'replication_factor' : ");
    query.append(replicationFactor);
    query.append("};");
    return connection.execute(query).thenApply(value -> null);
  }

  /**
   * Deletes the keyspace and all its content
   */
  public void drop() {
    drop("");
  }

  /**
   * Deletes the keyspace and all its content only if it exists
   */
  public void dropIfExists() {
    drop("IF EXISTS ");
  }

  private void drop(String addition) {
    var query = new StringBuilder("DROP KEYSPACE ");
    query.append(addition);
    query.append(name);
    query.append(";");
    connection.execute(query);
  }

  /**
   * Switches the currently active keyspace to this one
   */
  public void use() {
    var query = new StringBuilder("USE ");
    query.append(name);
    query.append(";");
    connection.execute(query);
  }

  /**
   * Is used find the schema of a table
   * @param tableName The name of the table
   * @return The columns of the table
   */
  public List<DatabaseColumn> findTableColumns(String tableName) {
    var query = new StringBuilder("SELECT * FROM system_schema.columns WHERE ");
    query.append("keyspace_name = '");
    query.append(name);
    query.append("' AND table_name = '");
    query.append(tableName);
    query.append("';");
    var result = connection().executesSynchronously(query);
    return result.getAvailableWithoutFetching() > 0 ?
      createDatabaseColumns(result.all()) : Lists.newArrayList();
  }

  private List<DatabaseColumn> createDatabaseColumns(Iterable<Row> iterator) {
    var rows = Lists.newArrayList(iterator);
    rows.sort(Comparator.comparingInt(row -> row.getInt("position")));
    var partitionKeyColumns = Lists.<DatabaseColumn>newArrayList();
    var clusteringKeyColumns = Lists.<DatabaseColumn>newArrayList();
    var columns = Lists.<DatabaseColumn>newArrayList();
    for (var row : rows) {
      var column = createDatabaseColumnEntry(row);
      if (column.type().isPartitionKey()) {
        partitionKeyColumns.add(column);
      } else if (column.type().isClusteringKey()) {
        clusteringKeyColumns.add(column);
      } else {
        columns.add(column);
      }
    }
    if (partitionKeyColumns.size() == 1 && clusteringKeyColumns.isEmpty()) {
      var partitionKeyColumn = partitionKeyColumns.get(0);
      partitionKeyColumn.updateType(DatabaseColumn.Type.PRIMARY_KEY);
      columns.add(0, partitionKeyColumn);
      return columns;
    }
    columns.addAll(0, clusteringKeyColumns);
    columns.addAll(0, partitionKeyColumns);
    return columns;
  }

  private DatabaseColumn createDatabaseColumnEntry(Row row) {
    var columnName = row.getString("column_name");
    var kind = row.getString("kind");
    var columnType = switch (kind) {
      case "partition_key" -> DatabaseColumn.Type.PARTITION_KEY;
      case "clustering" -> DatabaseColumn.Type.CLUSTERING_KEY;
      default -> DatabaseColumn.Type.REGULAR;
    };
    var dataType = row.getString("type").toUpperCase();
    if (dataType.contains("LIST")) {
      return DatabaseListColumn.create(columnName, DatabaseDataType.valueOf(
          dataType.replace("LIST", "").replace("<", "").replace(">", "")),
        columnType);
    } else if (dataType.contains("MAP")) {
      var dataTypeSplit = dataType.replace("MAP", "").replace("<", "")
        .replace(">", "").split(", ");
      return DatabaseMapColumn.create(columnName,
        DatabaseDataType.valueOf(dataTypeSplit[0]),
        DatabaseDataType.valueOf(dataTypeSplit[1]), columnType);
    }
    return DatabaseColumn.create(columnName, DatabaseDataType.valueOf(dataType),
      columnType);
  }

  /**
   * Is used register a new table to the keyspace
   * @param table The table that is to be registered
   */
  public void registerTable(DatabaseTable table) {
    tables.add(table);
  }

  /**
   * Is used to unregister a table from the table
   * @param table The table that is to be unregister
   */
  public void unregisterTable(DatabaseTable table) {
    tables.remove(table);
  }

  /**
   * The tables that were registered
   * @return The list of tables
   */
  public List<DatabaseTable> tables() {
    return List.copyOf(tables);
  }

  /**
   * Is used to find a table by name
   * @param tableName The name of the table
   * @return The table if it could be found
   */
  public Optional<DatabaseTable> findTableByName(String tableName) {
    return tables.stream()
      .filter(table -> table.name().equals(tableName))
      .findFirst();
  }

  /**
   * Is used to find a table by the table class canonical name
   * @param tableClassName The canonical name of table
   * @return The table if it could be found
   */
  public Optional<DatabaseTable> findTableByClass(String tableClassName) {
    return tables.stream()
      .filter(table -> table.getClass().getCanonicalName().equals(tableClassName))
      .findFirst();
  }
}

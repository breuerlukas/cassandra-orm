package de.lukasbreuer.cassandra.orm;

import de.lukasbreuer.cassandra.orm.skeleton.*;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Accessors(fluent = true)
public class DatabaseTable implements CreatableDatabaseTable,
  DroppableDatabaseTable, InsertableDatabaseTable, DeletableDatabaseTable,
  ExistableDatabaseTable, SelectableDatabaseTable, UpdatableDatabaseTable,
  CountableDatabaseTable, IndexableDatabaseTable, PageableDatabaseTable,
  ViewableDatabaseTable, AbstractDatabaseTable, TruncatableDatabaseTable,
  AggregatableDatabaseTable
{
  private final DatabaseConnection connection;
  private final DatabaseKeyspace keyspace;
  private final String name;
  private List<DatabaseColumn> columns;
  private List<DatabaseColumn> transformationColumns;
  private DatabaseTable temporaryTable;

  public DatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns
  ) {
    this.connection = connection;
    this.keyspace = keyspace;
    this.name = name;
    this.columns = columns;
  }

  /**
   * The implemented database table
   * @return The table
   */
  public DatabaseTable table() {
    return this;
  }

  /**
   * Is used to find the connection of the table
   * @return The connection of the table
   */
  public DatabaseConnection connection() {
    return connection;
  }

  /**
   * Is used to find the keyspace in which the table is located
   * @return The keyspace
   */
  public DatabaseKeyspace keyspace() {
    return keyspace;
  }

  /**
   * Returns the name of the table
   * @return The name
   */
  public String name() {
    return name;
  }

  /**
   * Is used to construct the full name of the table (keyspace and name combined)
   * @return The full name of the table
   */
  public String fullName() {
    return keyspace.name() + "." + name;
  }

  /**
   * Is used to find the columns of the table
   * @return The columns
   */
  public List<DatabaseColumn> columns() {
    return List.copyOf(columns);
  }

  /**
   * Creates a compilation of the names of the columns
   * @return The name compilation
   */
  public String columnNameCompilation() {
    var compilation = new StringBuilder();
    for (var i = 0; i < columns.size(); i++) {
      compilation.append(columns.get(i).name());
      if (i < columns.size() - 1) {
        compilation.append(", ");
      }
    }
    return compilation.toString();
  }

  /**
   * Another way to create a column name compilation with more unique parameters
   * @param columns The columns from which the compilation in created
   * @param prefix The prefix of the compilation
   * @param suffix The suffix of the compilation
   * @return The column name compilation
   */
  public String columnNameCompilation(
    List<DatabaseColumn> columns, String prefix, String suffix
  ) {
    if (columns.isEmpty()) {
      return "";
    }
    var compilation = new StringBuilder(prefix);
    for (var i = 0; i < columns.size(); i++) {
      if (i > 0) {
        compilation.append(", ");
      }
      compilation.append(columns.get(i).name());
    }
    compilation.append(suffix);
    return compilation.toString();
  }

  /**
   * Is used to find a single primary key column
   * @return The single primary key column
   */
  public DatabaseColumn findPrimaryKeyColumn() {
    return columns.stream().filter(column -> column.type().isPrimaryKey())
      .findFirst().get();
  }

  /**
   * Is used to find all column with type primary key
   * @return The list of columns
   */
  public List<DatabaseColumn> findPrimaryKeyColumns() {
    return columns.stream().filter(column -> column.type().isPrimaryKey())
      .toList();
  }

  /**
   * Is used to find a single partition key column
   * @return The single partition key column
   */
  public DatabaseColumn findPartitionKeyColumn() {
    return columns.stream().filter(column -> column.type().isPartitionKey())
      .findFirst().get();
  }

  /**
   * Is used to find all column with type partition key
   * @return The list of columns
   */
  public List<DatabaseColumn> findPartitionKeyColumns() {
    return columns.stream().filter(column -> column.type().isPartitionKey())
      .toList();
  }

  /**
   * Is used to find a certain column by its name
   * @return The column
   */
  public DatabaseColumn findColumnByName(String name) {
    return columns.stream().filter(column -> column.name().equals(name))
      .findFirst().get();
  }

  /**
   * Is used to add a new column to the database table
   * @param column The new column
   * @return A future that is completed when the operation is completed
   */
  public CompletableFuture<Void> addColumn(DatabaseColumn column) {
    columns.add(column);
    var query = new StringBuilder("ALTER TABLE ");
    query.append(fullName());
    query.append(" ADD ");
    query.append(column.name());
    query.append(" ");
    query.append(column.dataType());
    query.append(";");
    return connection.execute(query).thenApply(value -> null);
  }

  /**
   * Is used to rename an existing column
   * @param oldColumnName The old name of the column
   * @param newColumnName The new name of the column
   * @return A future that is completed when the operation is completed
   */
  public CompletableFuture<Void> renameColumn(String oldColumnName, String newColumnName) {
    var query = new StringBuilder("ALTER TABLE ");
    query.append(fullName());
    query.append(" RENAME ");
    query.append(oldColumnName);
    query.append(" TO ");
    query.append(newColumnName);
    query.append(";");
    return connection.execute(query).thenApply(value -> null);
  }

  /**
   * Is used to delete a column and all its content
   * @param columnName The name of the column that is to be dropped
   * @return A future that is completed when the operation is completed
   */
  public CompletableFuture<Void> dropColumn(String columnName) {
    var newColumns = columns.stream()
      .filter(column -> !column.name().equalsIgnoreCase(columnName))
      .toList();
    columns.clear();
    columns.addAll(newColumns);
    var query = new StringBuilder("ALTER TABLE ");
    query.append(fullName());
    query.append(" DROP ");
    query.append(columnName);
    query.append(";");
    return connection.execute(query).thenApply(value -> null);
  }

  /**
   * Is used by table system (databases) to configure table asynchronous
   * @param newColumns The columns of the table
   */
  protected void fillColumns(List<DatabaseColumn> newColumns) {
    columns.clear();
    columns.addAll(newColumns);
  }

  /**
   * When this function is called the table will be registered in the keyspace
   */
  public void registerTable() {
    keyspace.registerTable(this);
  }

  /**
   * When this function is called the table will be unregistered from the keyspace
   */
  public void unregisterTable() {
    keyspace.unregisterTable(this);
  }
}

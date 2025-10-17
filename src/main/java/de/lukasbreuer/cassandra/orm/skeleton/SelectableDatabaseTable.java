package de.lukasbreuer.cassandra.orm.skeleton;

import de.lukasbreuer.cassandra.orm.DatabaseColumn;
import de.lukasbreuer.cassandra.orm.DatabaseRow;
import de.lukasbreuer.cassandra.orm.condition.DatabaseCondition;
import de.lukasbreuer.cassandra.orm.iterator.AsyncIterator;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface SelectableDatabaseTable extends AbstractDatabaseTable {
  /**
   * Finds all available rows inside the database table
   * @return List of all possible rows
   */
  default CompletableFuture<List<DatabaseRow>> selectAllRows() {
    return selectAllRows(columnNameCompilation());
  }

  /**
   * Finds all available rows inside the database table
   * @param columns The columns which should be retrieved
   * @return List of all possible rows
   */
  default CompletableFuture<List<DatabaseRow>> selectAllRowsColumns(
    List<DatabaseColumn> columns
  ) {

    return selectAllRows(selectColumnNameCompilation(columns));
  }

  /**
   * Finds all available rows inside the database table
   * @param columnNames The name of the columns that should be selected
   * @return List of all possible rows
   */
  default CompletableFuture<List<DatabaseRow>> selectAllRows(String columnNames) {
    return selectAllRowsFix(columnNames);
  }

  /**
   * Finds all available rows inside the database table ignoring
   * transformation processes
   * @param columnNames The name of the columns that should be selected
   * @return List of all possible rows
   */
  default CompletableFuture<List<DatabaseRow>> selectAllRowsFix(String columnNames) {
    var query = new StringBuilder("SELECT ");
    query.append(columnNames);
    query.append(" FROM ");
    query.append(fullName());
    query.append(";");
    var columnCount = columnNames.length() - columnNames.replace(",", "").length() + 1;
    return connection().execute(query).thenApply(result ->
      DatabaseRow.multiple(result.currentPage(), columnCount));
  }

  /**
   * Is used to find a single row
   * @param value The primary key value
   * @return A future that contains the database row
   */
  default CompletableFuture<DatabaseRow> selectRow(Object value) {
    return selectRow(DatabaseCondition.of(findPrimaryKeyColumn().name(), value));
  }

  /**
   * Is used to find a single row
   * @param condition The condition with which the row can be found
   * @return A future that contains the database row
   */
  default CompletableFuture<DatabaseRow> selectRow(
    DatabaseCondition condition
  ) {
    return selectRows(condition)
      .thenApply(rows -> rows.isEmpty() ? DatabaseRow.of() : rows.get(0));
  }

  /**
   * Is used to find a single row
   * @param value The primary key value
   * @param columns The columns which should be retrieved
   * @return A future that contains the database row
   */
  default CompletableFuture<DatabaseRow> selectRowColumns(
    Object value, List<DatabaseColumn> columns
  ) {
    return selectRowColumns(
      DatabaseCondition.of(findPrimaryKeyColumn().name(), value), columns);
  }

  /**
   * Is used to find a single row
   * @param condition The condition with which the row can be found
   * @param columns The columns which should be retrieved
   * @return A future that contains the database row
   */
  default CompletableFuture<DatabaseRow> selectRowColumns(
    DatabaseCondition condition, List<DatabaseColumn> columns
  ) {
    return selectRows(condition, selectColumnNameCompilation(columns))
      .thenApply(rows -> rows.isEmpty() ? DatabaseRow.of() : rows.get(0));
  }

  /**
   * Is used to find a multiple row
   * @param value The primary key value
   * @param columns The columns which should be retrieved
   * @return A future that contains the database rows
   */
  default CompletableFuture<List<DatabaseRow>> selectRowsColumns(
    Object value, List<DatabaseColumn> columns
  ) {
    return selectRowsColumns(
      DatabaseCondition.of(findPrimaryKeyColumn().name(), value), columns);
  }

  /**
   * Is used to find a multiple row
   * @param condition The condition with which the row can be found
   * @param columns The columns which should be retrieved
   * @return A future that contains the database rows
   */
  default CompletableFuture<List<DatabaseRow>> selectRowsColumns(
    DatabaseCondition condition, List<DatabaseColumn> columns
  ) {
    return selectRows(condition, selectColumnNameCompilation(columns));
  }

  /**
   * Is used to find a single row secured (optional result)
   * @param value The primary key value
   * @return A future that contains the database row
   */
  default CompletableFuture<Optional<DatabaseRow>> selectRowSecure(Object value) {
    return selectRowSecure(DatabaseCondition.of(findPrimaryKeyColumn().name(), value));
  }

  /**
   * Is used to find a single row secured (optional result)
   * @param condition The condition with which the row can be found
   * @return A future that contains the database row
   */
  default CompletableFuture<Optional<DatabaseRow>> selectRowSecure(
    DatabaseCondition condition
  ) {
    return selectRows(condition).thenApply(rows -> rows.stream().findFirst());
  }

  /**
   * Is used to find a multiple rows
   * @param condition The condition with which the rows can be found
   * @return A future that contains the database rows
   */
  default CompletableFuture<List<DatabaseRow>> selectRows(
    DatabaseCondition condition
  ) {
    return selectRows(condition, columnNameCompilation(), -1);
  }

  /**
   * Is used to find a multiple rows
   * @param condition The condition with which the rows can be found
   * @param columnNames The names of the columns to be selected
   * @return A future that contains the database rows
   */
  default CompletableFuture<List<DatabaseRow>> selectRows(
    DatabaseCondition condition, String columnNames
  ) {
    return selectRows(condition, columnNames, -1);
  }

  /**
   * Is used to find a multiple rows
   * @param condition The condition with which the rows can be found
   * @param limit The limit of entries that should be returned
   * @return A future that contains the database rows
   */
  default CompletableFuture<List<DatabaseRow>> selectRows(
    DatabaseCondition condition, long limit
  ) {
    return selectRows(condition, columnNameCompilation(), limit);
  }

  /**
   * Is used to find a multiple rows
   * @param condition The condition with which the rows can be found
   * @param columnNames The names of the columns to be selected
   * @param limit The limit of entries that should be returned
   * @return A future that contains the database rows
   */
  default CompletableFuture<List<DatabaseRow>> selectRows(
    DatabaseCondition condition, String columnNames, long limit
  ) {
    return selectRowsFix(condition, columnNames, limit);
  }

  /**
   * Is used to find a multiple rows ignoring transformation processes
   * @param condition The condition with which the rows can be found
   * @param columnNames The name of the columns that should be selected
   * @param limit The limit of entries that should be returned
   * @return A future that contains the database rows
   */
  default CompletableFuture<List<DatabaseRow>> selectRowsFix(
    DatabaseCondition condition, String columnNames, long limit
  ) {
    var query = new StringBuilder("SELECT ");
    query.append(columnNames);
    query.append(" FROM ");
    query.append(fullName());
    var conditionValue = condition.build();
    if (!conditionValue.isEmpty()) {
      query.append(" WHERE ");
      query.append(conditionValue);
    }
    if (limit > 0) {
      query.append(" LIMIT ");
      query.append(limit);
    }
    query.append(condition.filteringAddition());
    query.append(";");
    var columnCount = columnNames.length() - columnNames.replace(",", "").length() + 1;
    return connection().execute(query, condition.values()).thenApply(result ->
      DatabaseRow.multiple(result.currentPage(), columnCount));
  }

  private String selectColumnNameCompilation(List<DatabaseColumn> columns) {
    var compilation = new StringBuilder();
    for (var i = 0; i < columns.size(); i++) {
      compilation.append(columns.get(i).name());
      if (i < columns.size() - 1) {
        compilation.append(", ");
      }
    }
    return compilation.toString();
  }
}

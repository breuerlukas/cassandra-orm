package de.lukasbreuer.cassandra.orm.skeleton;

import de.lukasbreuer.cassandra.orm.DatabaseRow;
import de.lukasbreuer.cassandra.orm.condition.DatabaseCondition;

import java.util.concurrent.CompletableFuture;

public interface UpdatableDatabaseTable extends AbstractDatabaseTable {
  /**
   * Updates a row inside the database table
   * @param value The primary key value
   * @param row The updated row (with all the columns)
   * @return A future that is completed when the update is completed
   */
  default CompletableFuture<Void> update(Object value, DatabaseRow row) {
    return update(value, row, "");
  }

  /**
   * Updates a row inside the database table
   * @param value The primary key value
   * @param row The updated row (with all the columns)
   * @param addition An addition update argument (for example for ttl)
   * @return A future that is completed when the update is completed
   */
  default CompletableFuture<Void> update(
    Object value, DatabaseRow row, String addition
  ) {
    return update(DatabaseCondition.of(findPrimaryKeyColumn().name(), value),
      row, addition);
  }

  /**
   * Updates a row inside the database table
   * @param condition The condition with which the row can be found
   * @param row The updated row (with all the columns)
   * @return A future that is completed when the update is completed
   */
  default CompletableFuture<Void> update(
    DatabaseCondition condition, DatabaseRow row
  ) {
    return update(condition, row, "");
  }

  /**
   * Updates a row inside the database table
   * @param condition The condition with which the row can be found
   * @param row The updated row (with all the columns)
   * @param addition An addition update argument (for example for ttl)
   * @return A future that is completed when the update is completed
   */
  default CompletableFuture<Void> update(
    DatabaseCondition condition, DatabaseRow row, String addition
  ) {
    return update(condition, row, buildUpdateChange(), addition);
  }

  private String buildUpdateChange() {
    var pairs = new StringBuilder();
    var columns = columns();
    for (var i = 0; i < columns.size(); i++) {
      var column = columns.get(i);
      if (!column.type().isRegular()) {
        continue;
      }
      pairs.append(column.name());
      pairs.append(" = ?");
      if (i < columns.size() - 1) {
        pairs.append(", ");
      }
    }
    return pairs.toString();
  }

  /**
   * Updates a row inside the database table
   * @param value The primary key value
   * @param row The updated row (with all the columns)
   * @return A future that is completed when the update is completed
   */
  default CompletableFuture<Void> updateCounter(Object value, DatabaseRow row) {
    return updateCounter(DatabaseCondition.of(findPrimaryKeyColumn().name(), value),
      row);
  }

  /**
   * Updates a row inside the database table
   * @param condition The condition with which the row can be found
   * @param row The updated row (with all the columns)
   * @return A future that is completed when the update is completed
   */
  default CompletableFuture<Void> updateCounter(
    DatabaseCondition condition, DatabaseRow row
  ) {
    var values = row.values();
    for (var i = condition.values().length; i < values.length; i++) {
      values[i] = Math.abs((long) values[i]);
    }
    return update(condition, DatabaseRow.of(values),
      buildUpdateCounterChange(row), "");
  }

  private String buildUpdateCounterChange(DatabaseRow row) {
    var pairs = new StringBuilder();
    var columns = columns();
    for (var i = 0; i < columns.size(); i++) {
      var column = columns.get(i);
      if (!column.type().isRegular()) {
        continue;
      }
      pairs.append(column.name());
      pairs.append(" = ");
      pairs.append(column.name());
      var value = row.findCell(i).longValue();
      pairs.append(value >= 0 ? " + " : " - ");
      pairs.append("?");
      if (i < columns.size() - 1) {
        pairs.append(", ");
      }
    }
    return pairs.toString();
  }

  private CompletableFuture<Void> update(
    DatabaseCondition condition, DatabaseRow row, String updateChange,
    String addition
  ) {
    return updateFix(condition, row, updateChange, addition);
  }

  /**
   * Updates a row inside the database table ignoring transformation processes
   * @param condition The condition with which the row can be found
   * @param row The row used to replace the placeholders
   * @param updateChange The key value pairs
   * @param addition An addition update argument (for example for ttl)
   * @return A future that is completed when the update is completed
   */
  default CompletableFuture<Void> updateFix(
    DatabaseCondition condition, DatabaseRow row, String updateChange,
    String addition
  ) {
    var query = new StringBuilder("UPDATE ");
    query.append(fullName());
    if (!addition.isEmpty()) {
      query.append(" ");
    }
    query.append(addition);
    query.append(" SET ");
    query.append(updateChange);
    var conditionValue = condition.build();
    if (!conditionValue.isEmpty()) {
      query.append(" WHERE ");
      query.append(conditionValue);
    }
    query.append(condition.filteringAddition());
    query.append(";");
    return connection().execute(query, buildUpdateValues(condition, row))
      .thenApply(value -> null);
  }

  private Object[] buildUpdateValues(DatabaseCondition condition, DatabaseRow row) {
    var columns = columns();
    var values = new Object[columns.size()];
    var valueIndex = 0;
    var comparisons = condition.comparisons();
    for (var i = 0; i < columns.size(); i++) {
      var column = columns.get(i).name();
      if (comparisons.stream().anyMatch(entry -> entry.column().equals(column))) {
        continue;
      }
      values[valueIndex] = row.values()[i];
      valueIndex++;
    }
    for (var comparison : comparisons) {
      values[valueIndex] = comparison.value();
      valueIndex++;
    }
    return values;
  }
}

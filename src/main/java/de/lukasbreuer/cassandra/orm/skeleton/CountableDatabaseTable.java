package de.lukasbreuer.cassandra.orm.skeleton;

import de.lukasbreuer.cassandra.orm.condition.DatabaseCondition;

import java.util.concurrent.CompletableFuture;

public interface CountableDatabaseTable extends AbstractDatabaseTable {
  /**
   * Is used to find the number of rows inside a database table
   * @return The number of rows
   */
  default CompletableFuture<Long> count() {
    return count(DatabaseCondition.empty());
  }

  /**
   * Is used to find the number of rows inside a database table
   * @param condition The condition for counting
   * @return The number of rows
   */
  default CompletableFuture<Long> count(DatabaseCondition condition) {
    return countFix(condition);
  }

  /**
   * Is used to find the number of rows inside a database table ignoring
   * transformation processes
   * @return The number of rows
   */
  default CompletableFuture<Long> countFix() {
    return countFix(DatabaseCondition.empty());
  }

  /**
   * Is used to find the number of rows inside a database table ignoring
   * transformation processes
   * @param condition The condition for counting
   * @return The number of rows
   */
  default CompletableFuture<Long> countFix(DatabaseCondition condition) {
    var query = new StringBuilder("SELECT COUNT(*) FROM ");
    query.append(fullName());
    var conditionValue = condition.build();
    if (!conditionValue.isEmpty()) {
      query.append(" WHERE ");
      query.append(conditionValue);
    }
    query.append(condition.filteringAddition());
    query.append(";");
    return connection().execute(query, condition.values())
      .thenApply(result -> result.one().get(0, Long.class));
  }
}

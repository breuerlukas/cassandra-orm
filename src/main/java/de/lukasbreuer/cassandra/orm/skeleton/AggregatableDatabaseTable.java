package de.lukasbreuer.cassandra.orm.skeleton;

import de.lukasbreuer.cassandra.orm.aggregation.DatabaseAggregation;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

public interface AggregatableDatabaseTable extends AbstractDatabaseTable {
  /**
   * Is used to perform an aggregation operation on a database column
   * @param aggregation The aggregation function
   * @param column The name of the column you want to aggregate
   * @return The aggregation value / result
   */
  default CompletableFuture<BigDecimal> aggregate(
    DatabaseAggregation aggregation, String column
  ) {
    var query = new StringBuilder("SELECT ");
    query.append(aggregation.name());
    query.append("(");
    query.append(column);
    query.append(")");
    query.append(" FROM ");
    query.append(fullName());
    query.append(";");
    return connection().execute(query).thenApply(result ->
      result.one().get(0, BigDecimal.class));
  }
}

package de.lukasbreuer.cassandra.orm.skeleton;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PagingState;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import de.lukasbreuer.cassandra.orm.DatabaseRow;
import de.lukasbreuer.cassandra.orm.condition.DatabaseCondition;
import de.lukasbreuer.cassandra.orm.paging.DatabaseDirection;
import de.lukasbreuer.cassandra.orm.paging.DatabaseOrder;
import de.lukasbreuer.cassandra.orm.paging.DatabasePage;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface PageableDatabaseTable extends AbstractDatabaseTable,
  CountableDatabaseTable
{
  /**
   * Used to find a specific page inside the table
   * @param partitionValue The partition key value that specifies the
   *                       basic set of elements to be paged
   * @param condition The condition that is used for filtering
   * @param order The direction in which sorting should take place
   * @param pageSize The page size that is used for the paging process
   * @param targetPage The page the requester wants to jump to
   * @return A future that contains the page
   */
  default CompletableFuture<DatabasePage<DatabaseRow>> selectPage(
    Object partitionValue, DatabaseCondition condition, DatabaseOrder order,
    int pageSize, int targetPage
  ) {
    return countPagingRows(partitionValue, condition).thenCompose(count ->
      selectPage(partitionValue, condition, order, pageSize, count, targetPage));
  }

  private CompletableFuture<DatabasePage<DatabaseRow>> selectPage(
    Object partitionValue, DatabaseCondition condition, DatabaseOrder order,
    int pageSize, long rowNumber, int targetPage
  ) {
    return selectPageFix(partitionValue, condition, order, pageSize,
      rowNumber, targetPage);
  }

  default CompletableFuture<DatabasePage<DatabaseRow>> selectPageFix(
    Object partitionValue, DatabaseCondition condition, DatabaseOrder order,
    int pageSize, long rowNumber, int targetPage
  ) {
    var pageNumber = calculatePageNumber(pageSize, rowNumber);
    if (targetPage != 0 && targetPage != pageNumber - 1) {
      return CompletableFuture.completedFuture(DatabasePage.empty());
    }
    var direction = targetPage == 0 ? DatabaseDirection.FORWARD :
      DatabaseDirection.BACKWARD;
    var pagingCondition = createPagingCondition(partitionValue, condition);
    var statement = createPagingStatement(pagingCondition,
      direction.isForward() ? order : order.reverse(), pageSize, "");
    if (targetPage == pageNumber - 1) {
      var offset = (int) (rowNumber % pageSize);
      statement = statement.setPageSize(offset == 0 ? pageSize : offset);
    }
    return connection().execute(statement, pagingCondition.values())
      .thenApply(result -> createDatabasePage(pageNumber, result, direction));
  }

  /**
   * Used to shift an existing paging state (next or previous page)
   * @param partitionValue The partition key value that specifies the
   *                       basic set of elements to be paged
   * @param condition The condition that is used for filtering
   * @param order The direction in which sorting should take place
   * @param pageSize The page size that is used for the paging process
   * @param pageState The current page state
   * @param startingPoint Whether you come from the back or from the front
   * @param direction The direction in which you want to shift
   * @return A future that contains the page
   */
  default CompletableFuture<DatabasePage<DatabaseRow>> shiftPage(
    Object partitionValue, DatabaseCondition condition, DatabaseOrder order,
    int pageSize, String pageState, DatabaseDirection startingPoint,
    DatabaseDirection direction
  ) {
    return countPagingRows(partitionValue, condition).thenCompose(count ->
      shiftPage(partitionValue, condition, order,
        pageSize, count, pageState, startingPoint, direction));
  }

  private CompletableFuture<DatabasePage<DatabaseRow>> shiftPage(
    Object partitionValue, DatabaseCondition condition, DatabaseOrder order,
    int pageSize, long rowNumber, String pageState, DatabaseDirection startingPoint,
    DatabaseDirection direction
  ) {
    return shiftPageFix(partitionValue, condition, order, pageSize,
      rowNumber, pageState, startingPoint, direction);
  }

  default CompletableFuture<DatabasePage<DatabaseRow>> shiftPageFix(
    Object partitionValue, DatabaseCondition condition, DatabaseOrder order,
    int pageSize, long rowNumber, String pageState, DatabaseDirection startingPoint,
    DatabaseDirection direction
  ) {
    var pageNumber = calculatePageNumber(pageSize, rowNumber);
    var pagingCondition = createPagingCondition(partitionValue, condition);
    var statement = createPagingStatement(pagingCondition,
      direction.isForward() ? order : order.reverse(), pageSize, pageState);
    return connection().execute(statement, pagingCondition.values())
      .thenCompose(result -> findShiftedPage(pageSize, pageNumber, result,
        startingPoint, direction));
  }

  private CompletableFuture<DatabasePage<DatabaseRow>> findShiftedPage(
    int pageSize, int pageNumber, AsyncResultSet firstResult,
    DatabaseDirection startingPoint, DatabaseDirection direction
  ) {
    if (startingPoint == direction) {
      return CompletableFuture.completedFuture(createDatabasePage(pageNumber,
        firstResult, direction));
    }
    return (CompletableFuture<DatabasePage<DatabaseRow>>)
      firstResult.fetchNextPage().thenApply(secondResult ->
        createDatabasePage(pageNumber, firstResult,
          combineShiftResults(pageSize, firstResult, secondResult), direction));
  }

  private List<Row> combineShiftResults(
    int pageSize, AsyncResultSet firstResult,
    AsyncResultSet secondResult
  ) {
    var firstBlock = Lists.newArrayList(firstResult.currentPage())
      .stream().sorted((a, b) -> -1).limit(1).sorted((a, b) -> -1).toList();
    var secondBlock = Lists.newArrayList(secondResult.currentPage()).stream()
      .limit(pageSize - 1).toList();
    var result = Lists.<Row>newArrayList();
    result.addAll(firstBlock);
    result.addAll(secondBlock);
    return result;
  }

  private DatabasePage<DatabaseRow> createDatabasePage(
    int pageNumber, AsyncResultSet resultSet, DatabaseDirection direction
  ) {
    return createDatabasePage(pageNumber, resultSet,
      Lists.newArrayList(resultSet.currentPage()), direction);
  }

  private DatabasePage<DatabaseRow> createDatabasePage(
    int pageNumber, AsyncResultSet resultSet, List<Row> rows,
    DatabaseDirection direction
  ) {
    if (direction.isBackward()) {
      Collections.reverse(rows);
    }
    return DatabasePage.create(DatabaseRow.multiple(rows, columns().size()),
      resultSet.hasMorePages() ?
        resultSet.getExecutionInfo().getSafePagingState().toString() : "",
      pageNumber);
  }

  private SimpleStatement createPagingStatement(
    DatabaseCondition condition, DatabaseOrder order, int pageSize,
    String pageState
  ) {
    var query = new StringBuilder("SELECT ");
    query.append(columnNameCompilation());
    query.append(" FROM ");
    query.append(fullName());
    var conditionValue = condition.build();
    if (!conditionValue.isEmpty()) {
      query.append(" WHERE ");
      query.append(conditionValue);
    }
    query.append(" ORDER BY ");
    query.append(columns().stream().filter(column -> column.type().isClusteringKey())
      .findFirst().get().name());
    query.append(" ");
    query.append(order.value());
    query.append(" ALLOW FILTERING;");
    var statement = SimpleStatement.builder(query.toString())
      .setPageSize(pageSize).build();
    if (!pageState.isEmpty()) {
      statement = statement.setPagingState(PagingState.fromString(pageState)
        .getRawPagingState());
    }
    return statement;
  }

  private CompletableFuture<Long> countPagingRows(
    Object partitionValue, DatabaseCondition condition
  ) {
    return count(createPagingCondition(partitionValue, condition));
  }

  private DatabaseCondition createPagingCondition(
    Object partitionValue, DatabaseCondition condition
  ) {
    var finalCondition = DatabaseCondition.of(findPartitionKeyColumn().name(),
      partitionValue, DatabaseCondition.Filtering.ALLOWED);
    finalCondition.concat(condition);
    return finalCondition;
  }

  private int calculatePageNumber(int pageSize, long rowNumber) {
    return (int) Math.ceil(((double) rowNumber) / pageSize);
  }
}

package de.lukasbreuer.cassandra.orm.condition;

import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor(staticName = "create")
public final class DatabaseCondition {
  public static DatabaseCondition empty() {
    return create(Lists.newArrayList());
  }

  public static DatabaseCondition of(String column, Object value) {
    return create(Lists.newArrayList(DatabaseComparison.create(column, value)));
  }

  public static DatabaseCondition of(
    String column, Object value, Filtering filtering
  ) {
    return create(Lists.newArrayList(DatabaseComparison.create(column, value)),
      filtering);
  }

  public static DatabaseCondition of(
    String column1, Object value1, String column2, Object value2
  ) {
    return create(Lists.newArrayList(DatabaseComparison.create(column1, value1),
      DatabaseComparison.create(column2, value2)));
  }

  public static DatabaseCondition of(
    String column1, Object value1, String column2, Object value2,
    Filtering filtering
  ) {
    return create(Lists.newArrayList(DatabaseComparison.create(column1, value1),
      DatabaseComparison.create(column2, value2)), filtering);
  }

  public static DatabaseCondition of(
    String column1, Object value1, String column2, Object value2,
    String column3, Object value3
  ) {
    return create(Lists.newArrayList(DatabaseComparison.create(column1, value1),
      DatabaseComparison.create(column2, value2),
      DatabaseComparison.create(column3, value3)));
  }

  public static DatabaseCondition of(
    String column1, Object value1, String column2, Object value2,
    String column3, Object value3, Filtering filtering
  ) {
    return create(Lists.newArrayList(DatabaseComparison.create(column1, value1),
      DatabaseComparison.create(column2, value2),
      DatabaseComparison.create(column3, value3)), filtering);
  }

  public static DatabaseCondition of(
    String column1, Object value1, String column2, Object value2,
    String column3, Object value3, String column4, Object value4
  ) {
    return create(Lists.newArrayList(DatabaseComparison.create(column1, value1),
      DatabaseComparison.create(column2, value2),
      DatabaseComparison.create(column3, value3),
      DatabaseComparison.create(column4, value4)));
  }

  public static DatabaseCondition of(
    String column1, Object value1, String column2, Object value2,
    String column3, Object value3, String column4, Object value4,
    Filtering filtering
  ) {
    return create(Lists.newArrayList(DatabaseComparison.create(column1, value1),
      DatabaseComparison.create(column2, value2),
      DatabaseComparison.create(column3, value3),
      DatabaseComparison.create(column4, value4)), filtering);
  }

  public static DatabaseCondition of(DatabaseComparison... comparisons) {
    return create(Lists.newArrayList(comparisons));
  }

  public static DatabaseCondition of(
    Filtering filtering, DatabaseComparison... comparisons
  ) {
    return create(Lists.newArrayList(comparisons), filtering);
  }

  public static DatabaseCondition create(List<DatabaseComparison> comparisons) {
    return create(comparisons, Filtering.DENIED);
  }

  public enum Filtering {
    ALLOWED,
    DENIED
  }

  private final List<DatabaseComparison> comparisons;
  private final Filtering filtering;

  /**
   * Is used to build the column and placeholder combination
   * @return The condition string
   */
  public String build() {
    var condition = new StringBuilder();
    for (var i = 0; i < comparisons.size(); i++) {
      if (i > 0) {
        condition.append(" AND ");
      }
      condition.append(comparisons.get(i).build());
    }
    return condition.toString();
  }

  public String filteringAddition() {
    if (isFilteringAllowed()) {
      return " ALLOW FILTERING";
    }
    return "";
  }

  /**
   * Adds the comparisons of the other condition to this condition
   * @param other The other condition
   */
  public void concat(DatabaseCondition other) {
    comparisons.addAll(other.comparisons());
  }

  /**
   * Is used the get the comparisons of the condition
   * @return The list of comparisons
   */
  public List<DatabaseComparison> comparisons() {
    return Lists.newArrayList(comparisons);
  }

  /**
   * Is used to get the values of the condition
   * @return The value array
   */
  public Object[] values() {
    return comparisons.stream().map(DatabaseComparison::value).toArray();
  }

  /**
   * Checks whether filtering is allowed
   * @return True if filtering is allowed, otherwise false
   */
  public boolean isFilteringAllowed() {
    return filtering == Filtering.ALLOWED;
  }

  /**
   * Checks whether filtering is denied
   * @return True if filtering is denied, otherwise false
   */
  public boolean isFilteringDenied() {
    return filtering == Filtering.DENIED;
  }
}

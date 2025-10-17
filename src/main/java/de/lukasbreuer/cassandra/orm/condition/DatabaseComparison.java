package de.lukasbreuer.cassandra.orm.condition;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(staticName = "create")
public final class DatabaseComparison {
  public static DatabaseComparison create(String column, Object value) {
    return create(column, value, Type.EQUALS);
  }

  public enum Type {
    EQUALS,
    GREATER,
    SMALLER,
    GREATER_EQUALS,
    SMALLER_EQUALS,
    CONTAINS,
    LIKE
  }

  private final String column;
  private final Object value;
  private final Type type;

  /**
   * Is used to build the comparison query
   * @return The comparison string
   */
  public String build() {
    var comparison = new StringBuilder();
    comparison.append(column);
    if (isEquals()) {
      comparison.append(" = ");
    } else if (isGreater()) {
      comparison.append(" > ");
    } else if (isSmaller()) {
      comparison.append(" < ");
    } else if (isGreaterEquals()) {
      comparison.append(" >= ");
    } else if (isSmallerEquals()) {
      comparison.append(" <= ");
    } else if (isContains()) {
      comparison.append(" CONTAINS ");
    } else if (isLike()) {
      comparison.append(" LIKE ");
    }
    comparison.append("?");
    return comparison.toString();
  }

  public boolean isEquals() {
    return type == Type.EQUALS;
  }

  public boolean isGreater() {
    return type == Type.GREATER;
  }

  public boolean isSmaller() {
    return type == Type.SMALLER;
  }

  public boolean isGreaterEquals() {
    return type == Type.GREATER_EQUALS;
  }

  public boolean isSmallerEquals() {
    return type == Type.SMALLER_EQUALS;
  }

  public boolean isContains() {
    return type == Type.CONTAINS;
  }

  public boolean isLike() {
    return type == Type.LIKE;
  }

  public String column() {
    return column;
  }

  public Object value() {
    return value;
  }
}

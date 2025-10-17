package de.lukasbreuer.cassandra.orm;

import de.lukasbreuer.cassandra.orm.paging.DatabaseOrder;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Optional;

@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class DatabaseColumn {
  public static DatabaseColumn create(String name, DatabaseDataType dataType) {
    return create(name, dataType, Type.REGULAR);
  }

  public static DatabaseColumn create(
    String name, DatabaseDataType dataType, Type type
  ) {
    return new DatabaseColumn(name, dataType, type, Optional.empty());
  }

  public static DatabaseColumn create(
    String name, DatabaseDataType dataType, Type type, DatabaseOrder order
  ) {
    return new DatabaseColumn(name, dataType, type, Optional.of(order));
  }

  public enum Type {
    PRIMARY_KEY,
    PARTITION_KEY,
    CLUSTERING_KEY,
    REGULAR;

    public boolean isPrimaryKey() {
      return this == PRIMARY_KEY;
    }

    public boolean isPartitionKey() {
      return this == PARTITION_KEY;
    }

    public boolean isClusteringKey() {
      return this == CLUSTERING_KEY;
    }

    public boolean isRegular() {
      return this == REGULAR;
    }
  }

  @Getter
  private final String name;
  @Getter
  private final DatabaseDataType dataType;
  @Getter
  private Type type;
  private final Optional<DatabaseOrder> order;

  /**
   * Is used by the {@link DatabaseTable} to e.g. initialize the column / table
   * @return The value of the column that can be interpreted by cassandra
   */
  public String databaseEntry() {
    var entry = new StringBuilder();
    entry.append(name);
    entry.append(" ");
    entry.append(dataType);
    return entry.toString();
  }

  public boolean hasOrder() {
    return order.isPresent();
  }

  public DatabaseOrder order() {
    return order.get();
  }

  public void updateType(Type newType) {
    type = newType;
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof DatabaseColumn otherColumn)) {
      return false;
    }
    return name.equals(otherColumn.name()) &&
      dataType == otherColumn.dataType() && type == otherColumn.type();
  }
}

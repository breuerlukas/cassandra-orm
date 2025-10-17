package de.lukasbreuer.cassandra.orm;

import de.lukasbreuer.cassandra.orm.paging.DatabaseOrder;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Optional;

@Accessors(fluent = true)
public class DatabaseMapColumn extends DatabaseColumn {
  public static DatabaseMapColumn create(
    String name, DatabaseDataType mapKeyDataType, DatabaseDataType mapValueDataType
  ) {
    return create(name, mapKeyDataType, mapValueDataType, Type.REGULAR);
  }

  public static DatabaseMapColumn create(
    String name, DatabaseDataType mapKeyDataType,
    DatabaseDataType mapValueDataType, Type type
  ) {
    return new DatabaseMapColumn(name, mapKeyDataType, mapValueDataType, type,
      Optional.empty());
  }

  public static DatabaseColumn create(
    String name, DatabaseDataType dataType, Type type, DatabaseOrder order
  ) {
    return new DatabaseColumn(name, dataType, type, Optional.of(order));
  }

  @Getter
  private final DatabaseDataType mapKeyDataType;
  @Getter
  private final DatabaseDataType mapValueDataType;

  private DatabaseMapColumn(
    String name, DatabaseDataType mapKeyDataType,
    DatabaseDataType mapValueDataType, Type type, Optional<DatabaseOrder> order
  ) {
    super(name, DatabaseDataType.MAP, type, order);
    this.mapKeyDataType = mapKeyDataType;
    this.mapValueDataType = mapValueDataType;
  }

  /**
   * Is used by the {@link DatabaseTable} to e.g. initialize the column / table
   * @return The value of the column that can be interpreted by cassandra
   */
  @Override
  public String databaseEntry() {
    var entry = new StringBuilder();
    entry.append(name());
    entry.append(" ");
    entry.append(dataType());
    entry.append("<");
    entry.append(mapKeyDataType);
    entry.append(", ");
    entry.append(mapValueDataType);
    entry.append(">");
    return entry.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof DatabaseMapColumn otherColumn)) {
      return false;
    }
    return name().equals(otherColumn.name()) &&
      dataType() == otherColumn.dataType() && type() == otherColumn.type() &&
      mapKeyDataType == otherColumn.mapKeyDataType() &&
      mapValueDataType == otherColumn.mapValueDataType();
  }
}

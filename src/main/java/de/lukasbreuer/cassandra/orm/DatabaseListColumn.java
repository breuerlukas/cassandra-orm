package de.lukasbreuer.cassandra.orm;

import de.lukasbreuer.cassandra.orm.paging.DatabaseOrder;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Optional;

@Accessors(fluent = true)
public class DatabaseListColumn extends DatabaseColumn {
  public static DatabaseListColumn create(String name, DatabaseDataType dataType) {
    return create(name, dataType, Type.REGULAR);
  }

  public static DatabaseListColumn create(
    String name, DatabaseDataType dataType, Type type
  ) {
    return new DatabaseListColumn(name, dataType, type, Optional.empty());
  }

  public static DatabaseColumn create(
    String name, DatabaseDataType dataType, Type type, DatabaseOrder order
  ) {
    return new DatabaseColumn(name, dataType, type, Optional.of(order));
  }

  @Getter
  private final DatabaseDataType listDataType;

  private DatabaseListColumn(
    String name, DatabaseDataType listDataType, Type type,
    Optional<DatabaseOrder> order
  ) {
    super(name, DatabaseDataType.LIST, type, order);
    this.listDataType = listDataType;
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
    entry.append(listDataType);
    entry.append(">");
    return entry.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof DatabaseListColumn otherColumn)) {
      return false;
    }
    return name().equals(otherColumn.name()) &&
      dataType() == otherColumn.dataType() && type() == otherColumn.type() &&
      listDataType == otherColumn.listDataType();
  }
}

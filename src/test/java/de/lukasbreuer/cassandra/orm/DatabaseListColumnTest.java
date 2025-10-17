package de.lukasbreuer.cassandra.orm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

final class DatabaseListColumnTest {
  @Test
  void testDatabaseListColumn() {
    var column = DatabaseListColumn.create("test", DatabaseDataType.TEXT,
      DatabaseColumn.Type.PRIMARY_KEY);
    Assertions.assertEquals(column.name(), "test");
    Assertions.assertEquals(column.dataType(), DatabaseDataType.LIST);
    Assertions.assertEquals(column.type(), DatabaseColumn.Type.PRIMARY_KEY);
    Assertions.assertTrue(column.type().isPrimaryKey());
    Assertions.assertEquals(column.databaseEntry(), "test LIST<TEXT>");
  }
}

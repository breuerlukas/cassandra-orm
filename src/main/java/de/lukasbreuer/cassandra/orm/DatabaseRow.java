package de.lukasbreuer.cassandra.orm;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.List;

@RequiredArgsConstructor(staticName = "create")
public final class DatabaseRow {
  public static List<DatabaseRow> multiple(Iterable<Row> iterable, int columnsLength) {
    List<DatabaseRow> rows = Lists.newArrayList();
    for (Row row : iterable) {
      rows.add(of(row, columnsLength));
    }
    return rows;
  }

  public static DatabaseRow of(Row row, int columnsLength) {
    var cells = new DatabaseCell[columnsLength];
    var values = new Object[columnsLength];
    for (var i = 0; i < columnsLength; i++) {
      var value = row.getObject(i);
      cells[i] = DatabaseCell.create(value);
      values[i] = value;
    }
    return create(cells, values);
  }

  public static DatabaseRow of(Object... values) {
    var cells = new DatabaseCell[values.length];
    for (var i = 0; i < values.length; i++) {
      cells[i] = DatabaseCell.create(values[i]);
    }
    return create(cells, values);
  }

  private final DatabaseCell[] cells;
  private final Object[] values;

  /**
   * Creates a string that contains the number of placeholder question marks
   * that are required for the query
   * @return The placeholder compilation
   */
  public String placeholderCompilation() {
    var compilation = new StringBuilder();
    for (var i = 0; i < cellNumber(); i++) {
      if (i > 0) {
        compilation.append(", ");
      }
      compilation.append("?");
    }
    return compilation.toString();
  }

  /**
   * Combines two rows into one common row
   * @param other The other row
   * @return The common row
   */
  public DatabaseRow concat(DatabaseRow other) {
    var combinedCells = Arrays.copyOf(cells,
      this.cellNumber() + other.cellNumber());
    System.arraycopy(other.cells(), 0, combinedCells, this.cellNumber(),
      other.cellNumber());
    var combinedValues = Arrays.copyOf(values,
      this.cellNumber() + other.cellNumber());
    System.arraycopy(other.values(), 0, combinedValues, this.cellNumber(),
      other.cellNumber());
    return DatabaseRow.create(combinedCells, combinedValues);
  }

  public void updateCell(int index, Object value) {
    cells[index] = DatabaseCell.create(value);
    values[index] = value;
  }

  /**
   * Used to search fo single cell in row
   * @param index The index of the target cell
   * @return The searched cell of the row
   */
  public DatabaseCell findCell(int index) {
    return cells[index];
  }

  /**
   * Calculates the number of stored cells in the row
   * @return The number of cells
   */
  public int cellNumber() {
    return cells.length;
  }

  /**
   * Is used to get the raw cell array of the database row
   * @return The raw cell array
   */
  public DatabaseCell[] cells() {
    return Arrays.copyOf(cells, cells.length);
  }

  /**
   * Is used to get the raw object array of the database row
   * @return The raw object array
   */
  public Object[] values() {
    return Arrays.copyOf(values, values.length);
  }
}

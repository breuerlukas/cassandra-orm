package de.lukasbreuer.cassandra.orm.skeleton;

import de.lukasbreuer.cassandra.orm.DatabaseColumn;
import de.lukasbreuer.cassandra.orm.DatabaseConnection;
import de.lukasbreuer.cassandra.orm.DatabaseKeyspace;
import de.lukasbreuer.cassandra.orm.DatabaseTable;

import java.util.List;

public interface AbstractDatabaseTable {
  /**
   * The implemented database table
   * @return The table
   */
  DatabaseTable table();

  /**
   * Is used to find the connection of the table
   * @return The connection of the table
   */
  DatabaseConnection connection();

  /**
   * Is used to find the keyspace in which the table is located
   * @return The keyspace
   */
  DatabaseKeyspace keyspace();

  /**
   * Returns the name of the table
   * @return The name
   */
  String name();

  /**
   * Is used to construct the full name of the table (keyspace and name combined)
   * @return The full name of the table
   */
  String fullName();

  /**
   * Is used to find the columns of the table
   * @return The columns
   */
  List<DatabaseColumn> columns();

  /**
   * Creates a compilation of the names of the columns
   * @return The name compilation
   */
  String columnNameCompilation();

  /**
   * Another way to create a column name compilation with more unique parameters
   * @param columns The columns from which the compilation in created
   * @param prefix The prefix of the compilation
   * @param suffix The suffix of the compilation
   * @return The column name compilation
   */
  String columnNameCompilation(
    List<DatabaseColumn> columns, String prefix, String suffix
  );

  /**
   * Is used to find a single primary key column
   * @return The single primary key column
   */
  DatabaseColumn findPrimaryKeyColumn();

  /**
   * Is used to find all column with type primary key
   * @return The list of columns
   */
  List<DatabaseColumn> findPrimaryKeyColumns();

  /**
   * Is used to find a single partition key column
   * @return The single partition key column
   */
  DatabaseColumn findPartitionKeyColumn();

  /**
   * Is used to find all column with type partition key
   * @return The list of columns
   */
  List<DatabaseColumn> findPartitionKeyColumns();
}

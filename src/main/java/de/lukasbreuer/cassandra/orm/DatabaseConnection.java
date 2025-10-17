package de.lukasbreuer.cassandra.orm;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

@Accessors(fluent = true)
@RequiredArgsConstructor(staticName = "create")
public final class DatabaseConnection {
  private final DatabaseConfiguration databaseConfiguration;
  private CqlSession session;

  /**
   * Used to connect to cassandra database
   */
  public void connect() {
    try {
      var loader = DriverConfigLoader.programmaticBuilder()
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
        .build();
      session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress(databaseConfiguration.hostname(),
          databaseConfiguration.port()))
        .withLocalDatacenter(databaseConfiguration.datacenter())
        .withConfigLoader(loader)
        .build();
    } catch (Exception exception) {
      exception.printStackTrace();
      System.err.println("The connection to cassandra failed");
    }
  }

  /**
   * Is used to execute a cql query
   * @param stringBuilder The string builder that contains the query
   * @param values The placeholder values
   * @return The future that contains the result set
   */
  public CompletableFuture<AsyncResultSet> execute(
    StringBuilder stringBuilder, Object... values
  ) {
    return execute(stringBuilder.toString(), values);
  }

  /**
   * Is used to execute a cql query
   * @param query The query
   * @param values The placeholder values
   * @return The future that contains the result set
   */
  public CompletableFuture<AsyncResultSet> execute(
    String query, Object... values
  ) {
    var result = session.prepareAsync(query)
      .thenApply(statement -> statement.bind(values))
      .thenCompose(statement -> session.executeAsync(statement))
      .toCompletableFuture();
    result.exceptionally(throwable -> exceptionally(throwable, query, values));
    return result;
  }

  /**
   * Is used to execute a cql query
   * @param simpleStatement The statement that is to be executed
   * @param values The placeholder values
   * @return The future that contains the result set
   */
  public CompletableFuture<AsyncResultSet> execute(
    SimpleStatement simpleStatement, Object... values
  ) {
    var result = session.prepareAsync(simpleStatement)
      .thenApply(statement -> statement.bind(values))
      .thenCompose(statement -> session.executeAsync(statement))
      .toCompletableFuture();
    result.exceptionally(throwable ->
      exceptionally(throwable, simpleStatement.getQuery(), values));
    return result;
  }

  private AsyncResultSet exceptionally(
    Throwable throwable, String query, Object... values
  ) {
    System.err.println(query + " -> " + Arrays.toString(values));
    throwable.printStackTrace();
    return null;
  }

  /**
   * Is used to execute a cql query synchronously
   * @param stringBuilder The string builder that contains the query
   * @param values The placeholder values
   * @return The future that contains the result set
   */
  public ResultSet executesSynchronously(
    StringBuilder stringBuilder, Object... values
  ) {
    return executesSynchronously(stringBuilder.toString(), values);
  }

  /**
   * Is used to execute a cql query synchronously
   * @param query The query
   * @param values The placeholder values
   * @return The future that contains the result set
   */
  public ResultSet executesSynchronously(
    String query, Object... values
  ) {
    try {
      var preparedStatement = session.prepare(query);
      var boundStatement = preparedStatement.bind(values);
      return session.execute(boundStatement);
    } catch (Exception exception) {
      return exceptionallySynchronously(exception, query, values);
    }
  }

  /**
   * Is used to execute a cql query synchronously
   * @param simpleStatement The statement that is to be executed
   * @param values The placeholder values
   * @return The future that contains the result set
   */
  public ResultSet executesSynchronously(
    SimpleStatement simpleStatement, Object... values
  ) {
    try {
      var preparedStatement = session.prepare(simpleStatement);
      var boundStatement = preparedStatement.bind(values);
      return session.execute(boundStatement);
    } catch (Exception exception) {
      return exceptionallySynchronously(exception, simpleStatement.getQuery(),
        values);
    }
  }

  private ResultSet exceptionallySynchronously(
    Throwable throwable, String query, Object... values
  ) {
    System.err.println(query + " -> " + Arrays.toString(values));
    throwable.printStackTrace();
    return null;
  }

  public Metadata metadata() {
    return session.getMetadata();
  }

  public boolean tableExists(DatabaseTable table) {
    return tableExists(table.keyspace().name(), table.name());
  }

  public boolean tableExists(String keyspaceName, String tableName) {
    return session.getMetadata().getKeyspace(keyspaceName).get()
      .getTable(tableName).isPresent();
  }
}

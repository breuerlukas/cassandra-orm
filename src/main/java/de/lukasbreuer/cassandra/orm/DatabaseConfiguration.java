package de.lukasbreuer.cassandra.orm;

import de.lukasbreuer.cassandra.orm.configuration.Configuration;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.json.JSONObject;

@Getter
@Accessors(fluent = true)
public final class DatabaseConfiguration extends Configuration {
  private static final String CONFIGURATION_PATH = "/configurations/database/database.json";

  public static DatabaseConfiguration createAndLoad() throws Exception {
    var configuration = new DatabaseConfiguration(CONFIGURATION_PATH);
    configuration.load();
    return configuration;
  }

  private String hostname;
  private int port;
  private String datacenter;

  private DatabaseConfiguration(String path) {
    super(path);
  }

  @Override
  protected void deserialize(JSONObject json) {
    hostname = json.getString("hostname");
    port = json.getInt("port");
    datacenter = json.getString("datacenter");
  }
}
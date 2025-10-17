package de.lukasbreuer.cassandra.orm.configuration;

import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

import java.io.File;
import java.nio.charset.Charset;

@Accessors(fluent = true)
public abstract class Configuration {
  @Getter
  private final String path;

  protected Configuration(String path) {
    this.path = path;
  }

  /**
   * This function builds the actual objects from the content of the
   * configuration file (json), which can be used later on
   * @param json
   */
  protected abstract void deserialize(JSONObject json);

  /**
   * Used to load the json configuration file
   * @throws Exception
   */
  public void load() throws Exception  {
    deserialize(new JSONObject(FileUtils.readFileToString(
      new File(absolutePath()), Charset.defaultCharset())));
  }

  public boolean exists() {
    return new File(absolutePath()).exists();
  }

  private String absolutePath() {
    return System.getProperty("user.dir") + path;
  }
}

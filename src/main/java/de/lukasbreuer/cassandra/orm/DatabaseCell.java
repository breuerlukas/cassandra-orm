package de.lukasbreuer.cassandra.orm;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Accessors(fluent = true)
@RequiredArgsConstructor(staticName = "create")
public final class DatabaseCell {
  @Getter
  private final Object value;

  public int integerValue() {
    if (!(value instanceof Integer)) {
      return -1;
    }
    return (int) value;
  }

  public double doubleValue() {
    if (!(value instanceof Double)) {
      return -1;
    }
    return (double) value;
  }

  public long longValue() {
    if (!(value instanceof Long)) {
      return -1;
    }
    return (long) value;
  }

  public boolean booleanValue() {
    if (!(value instanceof Boolean)) {
      return false;
    }
    return (boolean) value;
  }

  public String stringValue() {
    if (!(value instanceof String)) {
      return null;
    }
    return (String) value;
  }

  public UUID uuidValue() {
    if (!(value instanceof UUID)) {
      return null;
    }
    return (UUID) value;
  }

  public <T> List<T> listValue() {
    if (!(value instanceof List<?>)) {
      return null;
    }
    return (List<T>) value;
  }


  public <K, V> Map<K, V> mapValue() {
    if (!(value instanceof Map<?, ?>)) {
      return null;
    }
    return (Map<K, V>) value;
  }

  public ByteBuffer blobValue() {
    if (!(value instanceof ByteBuffer)) {
      return null;
    }
    return (ByteBuffer) value;
  }

  public Object rawValue() {
    return value;
  }
}

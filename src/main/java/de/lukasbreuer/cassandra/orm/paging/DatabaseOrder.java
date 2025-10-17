package de.lukasbreuer.cassandra.orm.paging;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum DatabaseOrder {
  ASCENDING("ASC"),
  DESCENDING("DESC");

  private final String value;

  public boolean isAscending() {
    return this == ASCENDING;
  }

  public boolean isDescending() {
    return this == DESCENDING;
  }

  public DatabaseOrder reverse() {
    return switch (this) {
      case ASCENDING -> DESCENDING;
      case DESCENDING -> ASCENDING;
    };
  }
}

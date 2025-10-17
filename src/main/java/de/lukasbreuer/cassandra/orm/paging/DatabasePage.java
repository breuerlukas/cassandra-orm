package de.lukasbreuer.cassandra.orm.paging;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(staticName = "create")
public final class DatabasePage<T> {
  public static <T> DatabasePage<T> empty() {
    return create(Lists.newArrayList(), "", 0);
  }

  private final List<T> content;
  private final String pageState;
  private final int pageNumber;
}

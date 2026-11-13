package io.distdb.sql;

import io.distdb.sql.types.Type;
import java.util.List;

/**
 * What a statement returns: its command tag ({@code SELECT 3}, {@code INSERT 0 1}, {@code BEGIN},
 * ...), and for a statement that returns rows, its columns and rows. Values are Java objects of the
 * columns' types; {@link io.distdb.sql.types.Values#toText} gives their text form.
 *
 * @param columns null when the statement returns no rows
 * @param notices messages for the client, such as "relation already exists, skipping"
 */
public record Result(String tag, List<Column> columns, List<Object[]> rows, List<String> notices) {

  public record Column(String name, Type type) {}

  public static Result command(String tag) {
    return new Result(tag, null, List.of(), List.of());
  }

  public static Result command(String tag, String notice) {
    return new Result(tag, null, List.of(), List.of(notice));
  }

  public boolean returnsRows() {
    return columns != null;
  }
}

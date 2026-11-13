package io.distdb.sql;

import io.distdb.sql.parse.Ast;
import io.distdb.sql.plan.Bound;
import io.distdb.sql.types.Type;
import java.util.List;

/**
 * A parsed statement with its parameter types and result columns (null when it returns no rows),
 * for the extended query protocol. Its binding is reused while the catalog version stays the same.
 */
public final class Prepared {

  final String sql;
  final Ast.Statement statement;
  final List<Type> hints;
  List<Type> paramTypes;
  List<Result.Column> columns;
  Bound bound;
  long catalogVersion = -1;

  Prepared(String sql, Ast.Statement statement, List<Type> hints) {
    this.sql = sql;
    this.statement = statement;
    this.hints = hints;
  }

  public String sql() {
    return sql;
  }

  /** The type of each parameter {@code $1, $2, ...}. */
  public List<Type> paramTypes() {
    return paramTypes;
  }

  /** The result columns, or null when the statement returns no rows. */
  public List<Result.Column> columns() {
    return columns;
  }

  /** Whether this is an empty query string. */
  public boolean empty() {
    return statement == null;
  }
}

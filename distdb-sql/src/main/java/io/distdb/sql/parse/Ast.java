package io.distdb.sql.parse;

import io.distdb.sql.types.Type;
import io.distdb.sql.types.TypeSpec;
import java.util.List;

/**
 * The syntax tree: what the parser produces and the binder consumes. Sealed interfaces of records,
 * so every {@code switch} over them is checked for exhaustiveness by the compiler. Names are
 * already case-folded (unquoted identifiers lowercased, as PostgreSQL does).
 */
public final class Ast {

  private Ast() {}

  // -----------------------------------------------------------------------------------------------
  // Statements

  public sealed interface Statement
      permits QueryStatement,
          Insert,
          Update,
          Delete,
          CreateTable,
          CreateIndex,
          DropTable,
          DropIndex,
          Begin,
          Commit,
          Rollback,
          SetVar,
          ShowVar,
          Explain {}

  public record QueryStatement(Query query) implements Statement {}

  /** {@code INSERT INTO table [(columns)] source [ON CONFLICT DO NOTHING] [RETURNING ...]}. */
  public record Insert(
      String table,
      List<String> columns,
      Query source,
      boolean onConflictDoNothing,
      List<SelectItem> returning)
      implements Statement {}

  public record Update(
      String table, String alias, List<Assignment> set, Expr where, List<SelectItem> returning)
      implements Statement {}

  public record Assignment(String column, Expr value) {}

  public record Delete(String table, String alias, Expr where, List<SelectItem> returning)
      implements Statement {}

  public record CreateTable(
      String name,
      boolean ifNotExists,
      List<ColumnDef> columns,
      List<String> primaryKey,
      List<List<String>> uniques)
      implements Statement {}

  /**
   * A column definition; {@code defaultSql} is the default expression's source text, kept in the
   * catalog and parsed again when the table is loaded.
   */
  public record ColumnDef(
      String name,
      TypeSpec type,
      boolean notNull,
      boolean primaryKey,
      boolean unique,
      Expr defaultExpr,
      String defaultSql) {}

  public record CreateIndex(
      String name, String table, List<String> columns, boolean unique, boolean ifNotExists)
      implements Statement {}

  public record DropTable(String name, boolean ifExists) implements Statement {}

  public record DropIndex(String name, boolean ifExists) implements Statement {}

  public record Begin() implements Statement {}

  public record Commit() implements Statement {}

  public record Rollback() implements Statement {}

  /** {@code SET name = value}; accepted and remembered, mostly for drivers. */
  public record SetVar(String name, String value) implements Statement {}

  public record ShowVar(String name) implements Statement {}

  public record Explain(Statement statement) implements Statement {}

  // -----------------------------------------------------------------------------------------------
  // Queries

  /** A query with its ORDER BY, LIMIT and OFFSET (null when absent), and WITH clauses. */
  public record Query(
      List<Cte> with, QueryBody body, List<OrderItem> orderBy, Expr limit, Expr offset) {}

  public record Cte(String name, List<String> columns, Query query) {}

  public sealed interface QueryBody permits Select, SetOp, Values, Nested {}

  public record Select(
      boolean distinct,
      List<SelectItem> items,
      List<TableRef> from,
      Expr where,
      List<Expr> groupBy,
      Expr having)
      implements QueryBody {}

  public enum SetOpKind {
    UNION,
    INTERSECT,
    EXCEPT
  }

  public record SetOp(SetOpKind kind, boolean all, QueryBody left, QueryBody right)
      implements QueryBody {}

  public record Values(List<List<Expr>> rows) implements QueryBody {}

  /** A parenthesized query in a set operation, with its own ORDER BY and LIMIT. */
  public record Nested(Query query) implements QueryBody {}

  public sealed interface SelectItem permits Star, ExprItem {}

  /** {@code *} or {@code t.*}; {@code qualifier} is null for a bare star. */
  public record Star(String qualifier) implements SelectItem {}

  /** An expression with its alias, or null alias. */
  public record ExprItem(Expr expr, String alias) implements SelectItem {}

  public record OrderItem(Expr expr, boolean desc, Boolean nullsFirst) {}

  public sealed interface TableRef permits TableName, Join, Derived {}

  public record TableName(String name, String alias) implements TableRef {}

  public enum JoinKind {
    INNER,
    LEFT,
    RIGHT,
    FULL,
    CROSS
  }

  /** A join; {@code on} or {@code using} (or neither, for CROSS). */
  public record Join(JoinKind kind, TableRef left, TableRef right, Expr on, List<String> using)
      implements TableRef {}

  /** A subquery in FROM, with its alias and optional column names. */
  public record Derived(Query query, String alias, List<String> columns) implements TableRef {}

  // -----------------------------------------------------------------------------------------------
  // Expressions

  public sealed interface Expr
      permits Literal,
          ColumnRef,
          Param,
          Unary,
          Binary,
          IsNull,
          IsBool,
          IsDistinct,
          Between,
          InList,
          InSubquery,
          Exists,
          ScalarSubquery,
          Case,
          Cast,
          Func,
          Like,
          Default {}

  /**
   * A constant: {@code Long} (INT), {@code BigDecimal} (NUMERIC), {@code Boolean}, a {@code String}
   * of type UNKNOWN (a quoted literal), or null (NULL, type UNKNOWN).
   */
  public record Literal(Object value, Type type) implements Expr {}

  public record ColumnRef(String qualifier, String name) implements Expr {}

  /** {@code $n}, with {@code index} from 1. */
  public record Param(int index) implements Expr {}

  /** {@code -x}, {@code +x}, {@code NOT x}. */
  public record Unary(String op, Expr operand) implements Expr {}

  /** Arithmetic, comparison, {@code ||}, {@code AND}, {@code OR}. */
  public record Binary(String op, Expr left, Expr right) implements Expr {}

  public record IsNull(Expr operand, boolean not) implements Expr {}

  /** {@code x IS [NOT] TRUE} or {@code FALSE}. */
  public record IsBool(Expr operand, boolean value, boolean not) implements Expr {}

  public record IsDistinct(Expr left, Expr right, boolean not) implements Expr {}

  public record Between(Expr operand, Expr low, Expr high, boolean not) implements Expr {}

  public record InList(Expr operand, List<Expr> list, boolean not) implements Expr {}

  public record InSubquery(Expr operand, Query query, boolean not) implements Expr {}

  public record Exists(Query query) implements Expr {}

  public record ScalarSubquery(Query query) implements Expr {}

  /** {@code CASE [operand] WHEN ... THEN ... [ELSE ...] END}; null operand and else allowed. */
  public record Case(Expr operand, List<When> whens, Expr otherwise) implements Expr {}

  public record When(Expr condition, Expr result) {}

  public record Cast(Expr operand, TypeSpec type) implements Expr {}

  /** A function or aggregate call; {@code star} for {@code count(*)}. */
  public record Func(String name, List<Expr> args, boolean distinct, boolean star)
      implements Expr {}

  public record Like(Expr operand, Expr pattern, Expr escape, boolean not, boolean caseInsensitive)
      implements Expr {}

  /** {@code DEFAULT} in a VALUES row or a SET clause. */
  public record Default() implements Expr {}
}

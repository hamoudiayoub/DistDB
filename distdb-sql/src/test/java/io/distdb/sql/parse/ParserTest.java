package io.distdb.sql.parse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.parse.Ast.Binary;
import io.distdb.sql.parse.Ast.ColumnRef;
import io.distdb.sql.parse.Ast.Expr;
import io.distdb.sql.parse.Ast.Literal;
import io.distdb.sql.parse.Ast.Unary;
import io.distdb.sql.types.Type;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class ParserTest {

  private static String tree(Expr e) {
    return switch (e) {
      case Binary b -> "(" + tree(b.left()) + " " + b.op() + " " + tree(b.right()) + ")";
      case Unary u -> "(" + u.op() + " " + tree(u.operand()) + ")";
      case Literal l -> String.valueOf(l.value());
      case ColumnRef c -> (c.qualifier() == null ? "" : c.qualifier() + ".") + c.name();
      case Ast.IsNull n -> "(" + tree(n.operand()) + (n.not() ? " IS NOT NULL)" : " IS NULL)");
      case Ast.Between b ->
          "(" + tree(b.operand()) + " BETWEEN " + tree(b.low()) + " AND " + tree(b.high()) + ")";
      case Ast.Cast c -> "(" + tree(c.operand()) + "::" + c.type().type() + ")";
      default -> e.getClass().getSimpleName();
    };
  }

  private static String parse(String expr) {
    return tree(Parser.parseExpression(expr));
  }

  @Test
  void precedenceFollowsPostgres() {
    assertThat(parse("1 + 2 * 3")).isEqualTo("(1 + (2 * 3))");
    assertThat(parse("1 - 2 - 3")).isEqualTo("((1 - 2) - 3)");
    assertThat(parse("a OR b AND c")).isEqualTo("(a OR (b AND c))");
    assertThat(parse("NOT a = b")).isEqualTo("(NOT (a = b))");
    assertThat(parse("a = b IS NULL")).isEqualTo("((a = b) IS NULL)");
    assertThat(parse("a || b = c")).isEqualTo("((a || b) = c)");
    assertThat(parse("-a ^ 2")).isEqualTo("((- a) ^ 2)");
    assertThat(parse("2 ^ 3 ^ 2")).isEqualTo("((2 ^ 3) ^ 2)");
    assertThat(parse("-x::int")).isEqualTo("(- (x::INT))");
    assertThat(parse("a BETWEEN 1 AND 2 AND b")).isEqualTo("((a BETWEEN 1 AND 2) AND b)");
    assertThat(parse("t.a + 1 > 2 OR NOT c")).isEqualTo("(((t.a + 1) > 2) OR (NOT c))");
  }

  @Test
  void literals() {
    assertThat(Parser.parseExpression("42")).isEqualTo(new Literal(42L, Type.INT));
    assertThat(Parser.parseExpression("-9223372036854775808"))
        .isEqualTo(new Literal(Long.MIN_VALUE, Type.INT));
    assertThat(Parser.parseExpression("99999999999999999999"))
        .isEqualTo(new Literal(new BigDecimal("99999999999999999999"), Type.NUMERIC));
    assertThat(Parser.parseExpression("1.50"))
        .isEqualTo(new Literal(new BigDecimal("1.50"), Type.NUMERIC));
    assertThat(Parser.parseExpression("1e3"))
        .isEqualTo(new Literal(new BigDecimal("1000"), Type.NUMERIC));
    assertThat(Parser.parseExpression("'it''s'")).isEqualTo(new Literal("it's", Type.UNKNOWN));
    assertThat(Parser.parseExpression("\"Quoted\"")).isEqualTo(new ColumnRef(null, "Quoted"));
    assertThat(Parser.parseExpression("Unquoted")).isEqualTo(new ColumnRef(null, "unquoted"));
  }

  @Test
  void statementsAndScripts() {
    assertThat(Parser.parse("SELECT 1; ; SELECT 2;")).hasSize(2);
    assertThat(Parser.parse("-- only a comment\n")).isEmpty();
    assertThat(Parser.parseOne("INSERT INTO t (a, b) VALUES (1, DEFAULT) RETURNING *"))
        .isInstanceOf(Ast.Insert.class);
    assertThat(Parser.parseOne("SELECT a FROM t x JOIN u USING (a) WHERE x.b > 1"))
        .isInstanceOf(Ast.QueryStatement.class);
    Ast.CreateTable ct =
        (Ast.CreateTable)
            Parser.parseOne(
                "CREATE TABLE t (a INT PRIMARY KEY, b VARCHAR(10) NOT NULL DEFAULT 'x' UNIQUE,"
                    + " c NUMERIC(10, 2), UNIQUE (b, c))");
    assertThat(ct.columns()).hasSize(3);
    assertThat(ct.columns().get(1).defaultSql()).isEqualTo("'x'");
    assertThat(ct.uniques()).hasSize(1);
  }

  @Test
  void errorsAreSyntaxErrorsWithAPosition() {
    assertThatThrownBy(() -> Parser.parse("SELECT FROM WHERE"))
        .isInstanceOfSatisfying(
            SqlException.class,
            e -> {
              assertThat(e.state()).isEqualTo(SqlState.SYNTAX_ERROR);
              assertThat(e.getMessage()).contains("at character");
            });
    assertThatThrownBy(() -> Parser.parse("SELECT 'unterminated")).isInstanceOf(SqlException.class);
    assertThatThrownBy(() -> Parser.parse("SELECT 1 2")).isInstanceOf(SqlException.class);
    assertThatThrownBy(() -> Parser.parse("BEGIN ISOLATION LEVEL SERIALIZABLE"))
        .isInstanceOfSatisfying(
            SqlException.class,
            e -> assertThat(e.state()).isEqualTo(SqlState.FEATURE_NOT_SUPPORTED));
  }
}

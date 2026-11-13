package io.distdb.shell;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Where statements end: never inside a string, a quoted name or a comment. */
class SqlLexerTest {

  @Test
  void aStatementEndsWithASemicolonOutsideStringsAndComments() {
    assertThat(SqlLexer.complete("SELECT 1;")).isTrue();
    assertThat(SqlLexer.complete("SELECT 1")).isFalse();
    assertThat(SqlLexer.complete("SELECT ';'")).isFalse();
    assertThat(SqlLexer.complete("SELECT 'it''s';")).isTrue();
    assertThat(SqlLexer.complete("SELECT \"a;b\" FROM t")).isFalse();
    assertThat(SqlLexer.complete("SELECT 1; -- done")).isTrue();
    assertThat(SqlLexer.complete("SELECT 1 /* ; */")).isFalse();
    assertThat(SqlLexer.complete("SELECT 'open;")).isFalse();
    assertThat(SqlLexer.complete("\\dt")).isTrue();
    assertThat(SqlLexer.complete("   ")).isTrue();
  }

  @Test
  void aScriptSplitsIntoItsStatements() {
    assertThat(
            SqlLexer.split(
                "CREATE TABLE t (s TEXT);\n-- a comment; with a semicolon\nINSERT INTO t VALUES"
                    + " ('a;b');;SELECT 1"))
        .containsExactly(
            "CREATE TABLE t (s TEXT);",
            "-- a comment; with a semicolon\nINSERT INTO t" + " VALUES ('a;b');",
            "SELECT 1");
  }

  @Test
  void tokensKnowKeywordsStringsAndNumbers() {
    String sql = "SELECT name, 42 FROM t WHERE x = 'y' AND z = $1 -- c";
    StringBuilder kinds = new StringBuilder();
    for (SqlLexer.Token t : SqlLexer.tokens(sql)) {
      if (t.kind() != SqlLexer.Kind.SPACE) {
        kinds.append(t.kind().name().charAt(0));
      }
    }
    // K=keyword W=word P=punct/parameter N=number S=string C=comment
    assertThat(kinds.toString()).isEqualTo("KWPNKWKWPSKWPPC");
    assertThat(SqlLexer.firstWord("  -- hi\n  create table t (x int)")).isEqualTo("create");
  }
}

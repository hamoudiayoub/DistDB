package io.distdb.shell;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Just enough of SQL's lexical rules for a shell: where strings, quoted names and comments start
 * and end, so that highlighting is right, a {@code ;} inside a string does not end a statement, and
 * a script splits into statements.
 */
final class SqlLexer {

  private SqlLexer() {}

  enum Kind {
    KEYWORD,
    WORD,
    QUOTED_NAME,
    STRING,
    NUMBER,
    COMMENT,
    PARAMETER,
    PUNCT,
    SPACE,
    /** An unterminated string, quoted name or comment, to the end of the input. */
    OPEN
  }

  record Token(Kind kind, int start, int end) {
    String text(String s) {
      return s.substring(start, end);
    }
  }

  static final Set<String> KEYWORDS =
      Set.of(
          "select",
          "from",
          "where",
          "and",
          "or",
          "not",
          "null",
          "is",
          "in",
          "like",
          "ilike",
          "between",
          "as",
          "on",
          "join",
          "inner",
          "left",
          "right",
          "full",
          "outer",
          "cross",
          "natural",
          "using",
          "group",
          "by",
          "having",
          "order",
          "asc",
          "desc",
          "nulls",
          "first",
          "last",
          "limit",
          "offset",
          "distinct",
          "all",
          "union",
          "intersect",
          "except",
          "insert",
          "into",
          "values",
          "update",
          "set",
          "delete",
          "returning",
          "create",
          "drop",
          "table",
          "index",
          "unique",
          "primary",
          "key",
          "default",
          "references",
          "if",
          "exists",
          "begin",
          "commit",
          "rollback",
          "transaction",
          "start",
          "work",
          "explain",
          "analyze",
          "show",
          "case",
          "when",
          "then",
          "else",
          "end",
          "cast",
          "with",
          "recursive",
          "conflict",
          "do",
          "nothing",
          "true",
          "false",
          "int",
          "integer",
          "bigint",
          "smallint",
          "text",
          "varchar",
          "char",
          "character",
          "varying",
          "numeric",
          "decimal",
          "boolean",
          "bool",
          "double",
          "precision",
          "real",
          "float",
          "serial",
          "tables",
          "columns",
          "indexes",
          "ranges",
          "nodes",
          "statistics",
          "for",
          "isolation",
          "level",
          "any",
          "some",
          "array",
          "only",
          "collate",
          "escape",
          "similar",
          "lateral",
          "filter",
          "over",
          "partition",
          "window",
          "count",
          "sum",
          "avg",
          "min",
          "max",
          "coalesce",
          "nullif",
          "greatest",
          "least");

  static List<Token> tokens(String s) {
    List<Token> out = new ArrayList<>();
    int i = 0;
    int n = s.length();
    while (i < n) {
      char c = s.charAt(i);
      int start = i;
      Kind kind;
      if (Character.isWhitespace(c)) {
        while (i < n && Character.isWhitespace(s.charAt(i))) {
          i++;
        }
        kind = Kind.SPACE;
      } else if (c == '-' && i + 1 < n && s.charAt(i + 1) == '-') {
        while (i < n && s.charAt(i) != '\n') {
          i++;
        }
        kind = Kind.COMMENT;
      } else if (c == '/' && i + 1 < n && s.charAt(i + 1) == '*') {
        int close = s.indexOf("*/", i + 2);
        i = close < 0 ? n : close + 2;
        kind = close < 0 ? Kind.OPEN : Kind.COMMENT;
      } else if (c == '\'' || ((c == 'e' || c == 'E') && i + 1 < n && s.charAt(i + 1) == '\'')) {
        boolean escapes = c != '\'';
        i += escapes ? 2 : 1;
        kind = Kind.OPEN;
        while (i < n) {
          char d = s.charAt(i);
          if (escapes && d == '\\' && i + 1 < n) {
            i += 2;
          } else if (d == '\'') {
            if (i + 1 < n && s.charAt(i + 1) == '\'') {
              i += 2;
            } else {
              i++;
              kind = Kind.STRING;
              break;
            }
          } else {
            i++;
          }
        }
      } else if (c == '"') {
        i++;
        kind = Kind.OPEN;
        while (i < n) {
          if (s.charAt(i) == '"') {
            if (i + 1 < n && s.charAt(i + 1) == '"') {
              i += 2;
            } else {
              i++;
              kind = Kind.QUOTED_NAME;
              break;
            }
          } else {
            i++;
          }
        }
      } else if (Character.isDigit(c)
          || (c == '.' && i + 1 < n && Character.isDigit(s.charAt(i + 1)))) {
        while (i < n && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '.')) {
          i++;
        }
        kind = Kind.NUMBER;
      } else if (c == '$' && i + 1 < n && Character.isDigit(s.charAt(i + 1))) {
        i++;
        while (i < n && Character.isDigit(s.charAt(i))) {
          i++;
        }
        kind = Kind.PARAMETER;
      } else if (Character.isLetter(c) || c == '_') {
        while (i < n && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) {
          i++;
        }
        String word = s.substring(start, i).toLowerCase(Locale.ROOT);
        kind = KEYWORDS.contains(word) ? Kind.KEYWORD : Kind.WORD;
      } else {
        i++;
        kind = Kind.PUNCT;
      }
      out.add(new Token(kind, start, i));
    }
    return out;
  }

  /**
   * Whether {@code buffer} is ready to run: a backslash command, nothing at all, or SQL whose last
   * meaningful character is a {@code ;} outside any string, name or comment.
   */
  static boolean complete(String buffer) {
    String t = buffer.strip();
    if (t.isEmpty() || t.startsWith("\\")) {
      return true;
    }
    Token last = null;
    for (Token tok : tokens(buffer)) {
      if (tok.kind() == Kind.OPEN) {
        return false;
      }
      if (tok.kind() != Kind.SPACE && tok.kind() != Kind.COMMENT) {
        last = tok;
      }
    }
    return last != null && last.kind() == Kind.PUNCT && buffer.charAt(last.start()) == ';';
  }

  /** {@code script}'s statements, each with its {@code ;}; comments and blank ones dropped. */
  static List<String> split(String script) {
    List<String> out = new ArrayList<>();
    int from = 0;
    boolean meaningful = false;
    for (Token tok : tokens(script)) {
      if (tok.kind() == Kind.PUNCT && script.charAt(tok.start()) == ';') {
        if (meaningful) {
          out.add(script.substring(from, tok.end()).strip());
        }
        from = tok.end();
        meaningful = false;
      } else if (tok.kind() != Kind.SPACE && tok.kind() != Kind.COMMENT) {
        meaningful = true;
      }
    }
    if (meaningful) {
      out.add(script.substring(from).strip());
    }
    return out;
  }

  /** The statement's first word, lowercased ("select", "create", ...), or "". */
  static String firstWord(String sql) {
    for (Token tok : tokens(sql)) {
      if (tok.kind() == Kind.KEYWORD || tok.kind() == Kind.WORD) {
        return tok.text(sql).toLowerCase(Locale.ROOT);
      }
      if (tok.kind() != Kind.SPACE && tok.kind() != Kind.COMMENT) {
        return "";
      }
    }
    return "";
  }
}

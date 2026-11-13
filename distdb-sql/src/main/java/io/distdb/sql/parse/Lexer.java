package io.distdb.sql.parse;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Splits SQL text into tokens. Unquoted identifiers and keywords are folded to lower case; {@code
 * "Quoted"} identifiers keep their case. String literals use standard SQL quoting ({@code
 * 'it''s'}); backslashes are ordinary characters ({@code standard_conforming_strings = on}).
 * Comments ({@code --} to end of line, and {@code /* *}{@code /}) are skipped.
 */
public final class Lexer {

  public enum Kind {
    /** An identifier or keyword, lowercased. */
    WORD,
    /** A double-quoted identifier, case kept. */
    QUOTED,
    INTEGER,
    DECIMAL,
    STRING,
    /** {@code $n}. */
    PARAM,
    /** An operator or punctuation. */
    OP,
    EOF
  }

  /** A token, with its offset in the source (for error messages and source slices). */
  public record Token(Kind kind, String text, int start, int end) {

    public boolean is(String word) {
      return kind == Kind.WORD && text.equals(word);
    }

    public boolean isOp(String op) {
      return kind == Kind.OP && text.equals(op);
    }
  }

  private static final String[] OPERATORS = {
    "::", "<=", ">=", "<>", "!=", "||", "(", ")", ",", ";", ".", "+", "-", "*", "/", "%", "^", "=",
    "<", ">", "[", "]"
  };

  private final String sql;
  private int pos;

  private Lexer(String sql) {
    this.sql = sql;
  }

  public static List<Token> tokenize(String sql) {
    return new Lexer(sql).run();
  }

  private List<Token> run() {
    List<Token> out = new ArrayList<>();
    while (true) {
      skipSpaceAndComments();
      if (pos >= sql.length()) {
        out.add(new Token(Kind.EOF, "", pos, pos));
        return out;
      }
      out.add(next());
    }
  }

  private void skipSpaceAndComments() {
    while (pos < sql.length()) {
      char c = sql.charAt(pos);
      if (Character.isWhitespace(c)) {
        pos++;
      } else if (c == '-' && pos + 1 < sql.length() && sql.charAt(pos + 1) == '-') {
        while (pos < sql.length() && sql.charAt(pos) != '\n') {
          pos++;
        }
      } else if (c == '/' && pos + 1 < sql.length() && sql.charAt(pos + 1) == '*') {
        int depth = 0;
        do {
          if (sql.startsWith("/*", pos)) {
            depth++;
            pos += 2;
          } else if (sql.startsWith("*/", pos)) {
            depth--;
            pos += 2;
          } else {
            pos++;
          }
        } while (depth > 0 && pos < sql.length());
        if (depth > 0) {
          throw error("unterminated /* comment", pos);
        }
      } else {
        return;
      }
    }
  }

  private Token next() {
    int start = pos;
    char c = sql.charAt(pos);
    if (Character.isLetter(c) || c == '_') {
      while (pos < sql.length() && isIdentPart(sql.charAt(pos))) {
        pos++;
      }
      return new Token(Kind.WORD, sql.substring(start, pos).toLowerCase(Locale.ROOT), start, pos);
    }
    if (c == '"') {
      StringBuilder b = new StringBuilder();
      pos++;
      while (true) {
        if (pos >= sql.length()) {
          throw error("unterminated quoted identifier", start);
        }
        char d = sql.charAt(pos++);
        if (d == '"') {
          if (pos < sql.length() && sql.charAt(pos) == '"') {
            b.append('"');
            pos++;
          } else {
            break;
          }
        } else {
          b.append(d);
        }
      }
      if (b.isEmpty()) {
        throw error("zero-length delimited identifier", start);
      }
      return new Token(Kind.QUOTED, b.toString(), start, pos);
    }
    if (c == '\'') {
      return string(start);
    }
    if (Character.isDigit(c)
        || (c == '.' && pos + 1 < sql.length() && Character.isDigit(sql.charAt(pos + 1)))) {
      return number(start);
    }
    if (c == '$' && pos + 1 < sql.length() && Character.isDigit(sql.charAt(pos + 1))) {
      pos++;
      while (pos < sql.length() && Character.isDigit(sql.charAt(pos))) {
        pos++;
      }
      return new Token(Kind.PARAM, sql.substring(start + 1, pos), start, pos);
    }
    for (String op : OPERATORS) {
      if (sql.startsWith(op, pos)) {
        pos += op.length();
        return new Token(Kind.OP, op.equals("!=") ? "<>" : op, start, pos);
      }
    }
    throw error("syntax error at or near \"" + c + "\"", start);
  }

  private Token string(int start) {
    StringBuilder b = new StringBuilder();
    pos++; // the opening quote
    while (true) {
      if (pos >= sql.length()) {
        throw error("unterminated quoted string", start);
      }
      char d = sql.charAt(pos++);
      if (d == '\'') {
        if (pos < sql.length() && sql.charAt(pos) == '\'') {
          b.append('\'');
          pos++;
        } else {
          break;
        }
      } else {
        b.append(d);
      }
    }
    return new Token(Kind.STRING, b.toString(), start, pos);
  }

  private Token number(int start) {
    boolean decimal = false;
    while (pos < sql.length() && Character.isDigit(sql.charAt(pos))) {
      pos++;
    }
    if (pos < sql.length()
        && sql.charAt(pos) == '.'
        && !(pos + 1 < sql.length() && sql.charAt(pos + 1) == '.')) {
      decimal = true;
      pos++;
      while (pos < sql.length() && Character.isDigit(sql.charAt(pos))) {
        pos++;
      }
    }
    if (pos < sql.length() && (sql.charAt(pos) == 'e' || sql.charAt(pos) == 'E')) {
      int save = pos;
      pos++;
      if (pos < sql.length() && (sql.charAt(pos) == '+' || sql.charAt(pos) == '-')) {
        pos++;
      }
      if (pos < sql.length() && Character.isDigit(sql.charAt(pos))) {
        decimal = true;
        while (pos < sql.length() && Character.isDigit(sql.charAt(pos))) {
          pos++;
        }
      } else {
        pos = save;
      }
    }
    if (pos < sql.length() && isIdentPart(sql.charAt(pos))) {
      throw error(
          "trailing junk after numeric literal at or near \""
              + sql.substring(start, pos + 1)
              + "\"",
          start);
    }
    return new Token(decimal ? Kind.DECIMAL : Kind.INTEGER, sql.substring(start, pos), start, pos);
  }

  private static boolean isIdentPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$';
  }

  static SqlException error(String message, int position) {
    return new SqlException(
        SqlState.SYNTAX_ERROR, message + " (at character " + (position + 1) + ")");
  }
}

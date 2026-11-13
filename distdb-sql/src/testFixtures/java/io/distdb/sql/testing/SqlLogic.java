package io.distdb.sql.testing;

import io.distdb.sql.Result;
import io.distdb.sql.Session;
import io.distdb.sql.SqlException;
import io.distdb.sql.types.Values;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * A runner for sqllogictest files, the format SQLite and CockroachDB test their SQL with:
 *
 * <pre>
 *   statement ok
 *   CREATE TABLE t (a INT PRIMARY KEY, b TEXT)
 *
 *   statement error 23505
 *   INSERT INTO t VALUES (1, 'x')
 *
 *   query IT rowsort
 *   SELECT a, b FROM t
 *   ----
 *   1 x
 * </pre>
 *
 * A query's type letters say how to print each column: {@code I} as is, {@code R} with three
 * decimals, {@code T} as text; NULL prints as {@code NULL} and the empty string as {@code (empty)}.
 * {@code rowsort} sorts the rows, {@code valuesort} every value, {@code nosort} keeps the order
 * (use it only with an ORDER BY that orders every row). The expected results in DistDB's files were
 * recorded from PostgreSQL ({@link #complete}).
 */
public final class SqlLogic {

  /** The test files, under {@code slt/} on the classpath. */
  public static final List<String> FILES =
      List.of(
          "basics.test",
          "types.test",
          "null.test",
          "select.test",
          "join.test",
          "aggregate.test",
          "subquery.test",
          "setop.test",
          "dml.test",
          "index.test",
          "transaction.test",
          "fuzz.test");

  /** A failed statement, with its SQLSTATE. */
  public static final class Failure extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String state;

    public Failure(String state, String message) {
      super(message);
      this.state = state;
    }

    public String state() {
      return state;
    }
  }

  /** A database under test: DistDB through a session, or PostgreSQL through JDBC. */
  public interface Database {
    /** Runs a statement; throws a {@link Failure} if it fails. */
    void statement(String sql);

    /** Runs a query; each value is its text form, or null for NULL. */
    List<List<String>> query(String sql);
  }

  enum Kind {
    STATEMENT_OK,
    STATEMENT_ERROR,
    QUERY
  }

  /** One record of a test file; {@code line} is where it starts. */
  record Record(
      int line,
      Kind kind,
      String sql,
      String errorState,
      String types,
      String sort,
      List<String> expected,
      List<String> header) {}

  public record Counts(int statements, int queries) {}

  private SqlLogic() {}

  public static String load(String name) {
    try (InputStream in = SqlLogic.class.getResourceAsStream("/slt/" + name)) {
      if (in == null) {
        throw new IllegalArgumentException("no test file slt/" + name);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Parsing

  static List<Record> parse(String text) {
    List<Record> out = new ArrayList<>();
    String[] lines = text.split("\r?\n", -1);
    int i = 0;
    while (i < lines.length) {
      String line = lines[i].strip();
      if (line.isEmpty() || line.startsWith("#")) {
        i++;
        continue;
      }
      int start = i + 1;
      String[] words = line.split("\\s+");
      List<String> header = List.of(words);
      i++;
      StringBuilder sql = new StringBuilder();
      while (i < lines.length && !lines[i].isBlank() && !lines[i].strip().equals("----")) {
        if (!sql.isEmpty()) {
          sql.append('\n');
        }
        sql.append(lines[i]);
        i++;
      }
      List<String> expected = new ArrayList<>();
      if (i < lines.length && lines[i].strip().equals("----")) {
        i++;
        while (i < lines.length && !lines[i].isBlank()) {
          expected.add(lines[i].strip());
          i++;
        }
      }
      switch (words[0]) {
        case "statement" -> {
          if (words.length < 2) {
            throw new IllegalArgumentException("line " + start + ": statement ok|error");
          }
          if (words[1].equals("ok")) {
            out.add(
                new Record(
                    start, Kind.STATEMENT_OK, sql.toString(), null, null, null, List.of(), header));
          } else if (words[1].equals("error")) {
            out.add(
                new Record(
                    start,
                    Kind.STATEMENT_ERROR,
                    sql.toString(),
                    words.length > 2 ? words[2] : null,
                    null,
                    null,
                    List.of(),
                    header));
          } else {
            throw new IllegalArgumentException("line " + start + ": statement ok|error");
          }
        }
        case "query" -> {
          String types = words.length > 1 ? words[1] : "T";
          String sort = words.length > 2 ? words[2] : "nosort";
          out.add(
              new Record(start, Kind.QUERY, sql.toString(), null, types, sort, expected, header));
        }
        default -> throw new IllegalArgumentException("line " + start + ": unknown record " + line);
      }
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Running

  /** Runs a test file against {@code db}; throws an AssertionError at the first difference. */
  public static Counts run(String name, String text, Database db) {
    int statements = 0;
    int queries = 0;
    for (Record r : parse(text)) {
      switch (r.kind()) {
        case STATEMENT_OK -> {
          try {
            db.statement(r.sql());
          } catch (Failure f) {
            throw new AssertionError(
                name
                    + ":"
                    + r.line()
                    + ": expected success, got "
                    + f.state()
                    + " "
                    + f.getMessage()
                    + "\n"
                    + r.sql());
          }
          statements++;
        }
        case STATEMENT_ERROR -> {
          try {
            db.statement(r.sql());
          } catch (Failure f) {
            if (r.errorState() != null && !r.errorState().equals(f.state())) {
              throw new AssertionError(
                  name
                      + ":"
                      + r.line()
                      + ": expected error "
                      + r.errorState()
                      + ", got "
                      + f.state()
                      + " "
                      + f.getMessage()
                      + "\n"
                      + r.sql());
            }
            statements++;
            continue;
          }
          throw new AssertionError(
              name
                  + ":"
                  + r.line()
                  + ": expected error "
                  + r.errorState()
                  + ", but it succeeded\n"
                  + r.sql());
        }
        case QUERY -> {
          List<List<String>> rows;
          try {
            rows = db.query(r.sql());
          } catch (Failure f) {
            throw new AssertionError(
                name
                    + ":"
                    + r.line()
                    + ": query failed: "
                    + f.state()
                    + " "
                    + f.getMessage()
                    + "\n"
                    + r.sql());
          }
          List<String> actual = format(r, rows);
          if (!actual.equals(r.expected())) {
            throw new AssertionError(
                name
                    + ":"
                    + r.line()
                    + ": wrong result for\n"
                    + r.sql()
                    + "\nexpected:\n"
                    + String.join("\n", r.expected())
                    + "\nactual:\n"
                    + String.join("\n", actual));
          }
          queries++;
        }
      }
    }
    return new Counts(statements, queries);
  }

  /**
   * The file {@code text} with every query's expected results, and every expected error's SQLSTATE,
   * taken from {@code reference} (PostgreSQL). Fails if the reference rejects a statement the file
   * expects to succeed, or accepts one it expects to fail.
   */
  public static String complete(String name, String text, Database reference) {
    StringBuilder out = new StringBuilder();
    String[] lines = text.split("\r?\n", -1);
    List<Record> records = parse(text);
    int next = 0;
    int i = 0;
    while (i < lines.length) {
      Record r =
          next < records.size() && records.get(next).line() == i + 1 ? records.get(next++) : null;
      if (r == null) {
        out.append(lines[i]).append('\n');
        i++;
        continue;
      }
      // Skip the record's lines in the input.
      i++;
      while (i < lines.length && !lines[i].isBlank() && !lines[i].strip().equals("----")) {
        i++;
      }
      if (i < lines.length && lines[i].strip().equals("----")) {
        i++;
        while (i < lines.length && !lines[i].isBlank()) {
          i++;
        }
      }
      switch (r.kind()) {
        case STATEMENT_OK -> {
          try {
            reference.statement(r.sql());
          } catch (Failure f) {
            throw new AssertionError(
                name
                    + ":"
                    + r.line()
                    + ": the reference failed: "
                    + f.state()
                    + " "
                    + f.getMessage()
                    + "\n"
                    + r.sql());
          }
          out.append("statement ok\n").append(r.sql()).append('\n');
        }
        case STATEMENT_ERROR -> {
          String state;
          try {
            reference.statement(r.sql());
            throw new AssertionError(
                name + ":" + r.line() + ": the reference accepted\n" + r.sql());
          } catch (Failure f) {
            state = f.state();
          }
          out.append("statement error ").append(state).append('\n').append(r.sql()).append('\n');
        }
        case QUERY -> {
          List<List<String>> rows;
          try {
            rows = reference.query(r.sql());
          } catch (Failure f) {
            throw new AssertionError(
                name
                    + ":"
                    + r.line()
                    + ": the reference failed: "
                    + f.state()
                    + " "
                    + f.getMessage()
                    + "\n"
                    + r.sql());
          }
          out.append("query ").append(r.types()).append(' ').append(r.sort()).append('\n');
          out.append(r.sql()).append("\n----\n");
          for (String l : format(r, rows)) {
            out.append(l).append('\n');
          }
        }
      }
    }
    String s = out.toString();
    while (s.endsWith("\n\n")) {
      s = s.substring(0, s.length() - 1);
    }
    return s;
  }

  static List<String> format(Record r, List<List<String>> rows) {
    List<List<String>> formatted = new ArrayList<>();
    for (List<String> row : rows) {
      if (row.size() != r.types().length()) {
        throw new AssertionError(
            "line " + r.line() + ": " + row.size() + " columns, but types " + r.types());
      }
      List<String> f = new ArrayList<>();
      for (int k = 0; k < row.size(); k++) {
        f.add(value(row.get(k), r.types().charAt(k)));
      }
      formatted.add(f);
    }
    List<String> out = new ArrayList<>();
    switch (r.sort()) {
      case "valuesort" -> {
        for (List<String> row : formatted) {
          row.forEach(v -> out.add(line(v)));
        }
        Collections.sort(out);
      }
      case "rowsort" -> {
        for (List<String> row : formatted) {
          out.add(line(String.join(" ", row)));
        }
        Collections.sort(out);
      }
      default -> {
        for (List<String> row : formatted) {
          out.add(line(String.join(" ", row)));
        }
      }
    }
    return out;
  }

  /**
   * A result line as the file holds it: stripped (the parser strips expected lines), and never
   * blank (a blank line ends the expected results).
   */
  private static String line(String s) {
    String t = s.strip();
    return t.isEmpty() ? "(blank)" : t;
  }

  private static String value(String v, char type) {
    if (v == null) {
      return "NULL";
    }
    if (v.isEmpty()) {
      return "(empty)";
    }
    if (type == 'R') {
      try {
        String s = String.format(Locale.ROOT, "%.3f", Double.parseDouble(v));
        return s.equals("-0.000") ? "0.000" : s;
      } catch (NumberFormatException e) {
        return v;
      }
    }
    return v;
  }

  // -----------------------------------------------------------------------------------------------
  // DistDB

  /** A message with its chain of causes, for failures that are not SQL errors. */
  static String causes(Throwable e) {
    StringBuilder b = new StringBuilder(String.valueOf(e.getMessage()));
    for (Throwable c = e.getCause(); c != null && c != c.getCause(); c = c.getCause()) {
      b.append(" <- ").append(c);
    }
    return b.toString();
  }

  /** DistDB, through a session. */
  public static Database of(Session session) {
    return new Database() {
      @Override
      public void statement(String sql) {
        try {
          session.execute(sql);
        } catch (SqlException e) {
          throw new Failure(e.state(), e.getMessage());
        }
      }

      @Override
      public List<List<String>> query(String sql) {
        List<Result> results;
        try {
          results = session.execute(sql);
        } catch (SqlException e) {
          throw new Failure(e.state(), causes(e));
        }
        Result last = results.get(results.size() - 1);
        if (!last.returnsRows()) {
          throw new Failure("XX000", "not a query: " + sql);
        }
        List<List<String>> rows = new ArrayList<>();
        for (Object[] row : last.rows()) {
          List<String> r = new ArrayList<>();
          for (int k = 0; k < row.length; k++) {
            r.add(row[k] == null ? null : Values.toText(row[k], last.columns().get(k).type()));
          }
          rows.add(r);
        }
        return rows;
      }
    };
  }
}

package io.distdb.shell;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.jline.reader.EOFError;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * {@code distdb sql}: an interactive SQL shell over PostgreSQL's wire protocol. For a person: line
 * editing with history, highlighting and completion, framed tables, plan trees and timings. For a
 * script ({@code -c}, {@code -f}, or piped input): psql's plain output, and exit status 3 at the
 * first error.
 */
public final class Shell {

  public static final String VERSION = "1.0";

  static final int OK = 0;
  static final int CANNOT_CONNECT = 1;
  static final int USAGE = 2;
  static final int FAILED = 3;

  /** The backslash commands. */
  enum Meta {
    HELP("\\?", "", "this help"),
    TABLES("\\dt", "", "list the tables"),
    DESCRIBE("\\d", "[TABLE]", "a table's columns and indexes (no TABLE: the tables)"),
    INDEXES("\\di", "[TABLE]", "indexes"),
    STATS("\\stats", "TABLE", "what ANALYZE learned: distinct values, nulls, min, max"),
    RANGES("\\ranges", "[TABLE]", "where the data lives: ranges, leaders, replicas"),
    NODES("\\nodes", "", "the data nodes: alive, replicas, leaders"),
    PUSHDOWN("\\pushdown", "[on|off]", "run filters and aggregates on the ranges"),
    TIMING("\\timing", "[on|off]", "show how long each statement took"),
    EXPANDED("\\x", "[on|off|auto]", "one column per line"),
    INCLUDE("\\i", "FILE", "run the statements in FILE"),
    CONNINFO("\\conninfo", "", "the connection"),
    CLEAR("\\clear", "", "clear the screen"),
    QUIT("\\q", "", "quit (or Ctrl-D)");

    private final String command;
    private final String args;
    private final String help;

    Meta(String command, String args, String help) {
      this.command = command;
      this.args = args;
      this.help = help;
    }

    String command() {
      return command;
    }

    String help() {
      return help;
    }

    static Meta of(String command) {
      for (Meta m : values()) {
        if (m.command.equals(command)) {
          return m;
        }
      }
      return null;
    }
  }

  private final PgClient client;
  private final PrintWriter out;
  private final PrintWriter err;
  private final Theme theme;
  private final boolean boxed;
  private final java.util.function.IntSupplier width;
  private final SqlCompleter completer = new SqlCompleter();
  private boolean timing;

  /** Print the banner, and each statement after the prompt, as a person would see it. */
  private boolean echo;

  private Render.Expanded expanded = Render.Expanded.AUTO;
  private boolean quit;

  private Shell(
      PgClient client,
      PrintWriter out,
      PrintWriter err,
      Theme theme,
      boolean boxed,
      java.util.function.IntSupplier width) {
    this.client = client;
    this.out = out;
    this.err = err;
    this.theme = theme;
    this.boxed = boxed;
    this.width = width;
    this.timing = boxed;
  }

  // -----------------------------------------------------------------------------------------------
  // Entry

  static final String HELP =
      """
      usage: distdb sql [HOST:PORT] [--user NAME] [--db NAME] [-c SQL]... [-f FILE]
                        [--color|--no-color] [--pretty]

        Connects to a node's SQL port (default 127.0.0.1:5433, or $DISTDB_SQL) and opens a shell.
        With -c or -f, or with input from a pipe, it runs the statements and prints psql's plain
        output instead, stopping at the first error (exit status 3). --pretty keeps the shell's
        framed, colored layout for scripts too (for demos and screenshots).
      """;

  /** Runs the shell; returns the exit status. */
  public static int run(
      List<String> args, InputStream stdin, PrintStream stdout, PrintStream stderr) {
    String address = System.getenv().getOrDefault("DISTDB_SQL", "127.0.0.1:5433");
    String user = "distdb";
    String db = "distdb";
    List<String> commands = new ArrayList<>();
    String file = null;
    Boolean color = System.getenv("NO_COLOR") != null ? Boolean.FALSE : null;
    boolean pretty = false;
    boolean echo = false;
    for (int i = 0; i < args.size(); i++) {
      String a = args.get(i);
      try {
        switch (a) {
          case "-c", "--command" -> commands.add(args.get(++i));
          case "-f", "--file" -> file = args.get(++i);
          case "--user", "-U" -> user = args.get(++i);
          case "--db", "-d" -> db = args.get(++i);
          case "--host", "-h" -> address = args.get(++i);
          case "--color" -> color = Boolean.TRUE;
          case "--pretty" -> pretty = true;
          case "--echo" -> {
            pretty = true;
            echo = true;
          }
          case "--no-color" -> color = Boolean.FALSE;
          case "--help" -> {
            stdout.print(HELP);
            return OK;
          }
          default -> {
            if (a.startsWith("-") || !a.contains(":")) {
              stderr.println("error: unknown argument " + a);
              stderr.print(HELP);
              return USAGE;
            }
            address = a;
          }
        }
      } catch (IndexOutOfBoundsException e) {
        stderr.println("error: " + a + " needs a value");
        return USAGE;
      }
    }
    int colon = address.lastIndexOf(':');
    String host = address.substring(0, colon);
    int port;
    try {
      port = Integer.parseInt(address.substring(colon + 1));
    } catch (NumberFormatException e) {
      stderr.println("error: bad address " + address + " (want HOST:PORT)");
      return USAGE;
    }
    boolean scripted = !commands.isEmpty() || file != null || !interactiveConsole();
    PgClient client;
    try {
      client =
          PgClient.connect(
              host, port, user, db, System.getenv("PGPASSWORD"), Duration.ofSeconds(5));
    } catch (IOException | PgClient.PgError e) {
      Theme t = scripted || Boolean.FALSE.equals(color) ? Theme.PLAIN : Theme.COLOR;
      stderr.println(
          t.paint("✖ cannot connect to " + address + ": ", "1;" + Theme.ERROR) + e.getMessage());
      stderr.println(
          t.dim(
              "  is a node serving SQL there? start one with `distdb node ... --sql "
                  + address
                  + "`, or try `distdb demo`"));
      return CANNOT_CONNECT;
    }
    try (client) {
      if (scripted) {
        Theme t = Boolean.TRUE.equals(color) ? Theme.COLOR : Theme.PLAIN;
        PrintWriter o = new PrintWriter(stdout, true, StandardCharsets.UTF_8);
        PrintWriter e = new PrintWriter(stderr, true, StandardCharsets.UTF_8);
        if (pretty && color == null) {
          t = Theme.COLOR;
        }
        int columns = pretty ? 110 : 1000;
        Shell s = new Shell(client, o, pretty ? o : e, t, pretty, () -> columns);
        s.echo = echo;
        if (echo) {
          s.banner();
        }
        if (!commands.isEmpty()) {
          for (String c : commands) {
            if (!s.runText(c, !pretty) && !pretty) {
              return FAILED;
            }
          }
          return OK;
        }
        String script =
            file != null
                ? Files.readString(Path.of(file), StandardCharsets.UTF_8)
                : new String(stdin.readAllBytes(), StandardCharsets.UTF_8);
        return s.runScript(script) ? OK : FAILED;
      }
      return interactive(client, Boolean.FALSE.equals(color) ? Theme.PLAIN : Theme.COLOR);
    } catch (IOException e) {
      stderr.println("error: " + e.getMessage());
      return FAILED;
    }
  }

  private static boolean interactiveConsole() {
    java.io.Console c = System.console();
    return c != null && c.isTerminal();
  }

  /** Opens the line editor on the terminal and reads statements until \q or Ctrl-D. */
  static int interactive(PgClient client, Theme theme) throws IOException {
    try (Terminal terminal = TerminalBuilder.builder().system(true).name("distdb").build()) {
      PrintWriter w = terminal.writer();
      Shell shell =
          new Shell(
              client,
              w,
              w,
              theme,
              true,
              () -> terminal.getWidth() > 20 ? terminal.getWidth() : 100);
      LineReader reader =
          LineReaderBuilder.builder()
              .terminal(terminal)
              .appName("distdb")
              .parser(new StatementParser())
              .highlighter(theme.colors() ? new SqlHighlighter() : null)
              .completer(shell.completer)
              .variable(
                  LineReader.HISTORY_FILE,
                  Path.of(System.getProperty("user.home"), ".distdb_history"))
              .variable(LineReader.HISTORY_SIZE, 2000)
              .variable(LineReader.SECONDARY_PROMPT_PATTERN, "%P-> ")
              .variable(LineReader.INDENTATION, 2)
              .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
              .option(LineReader.Option.CASE_INSENSITIVE, true)
              .option(LineReader.Option.AUTO_FRESH_LINE, true)
              .build();
      terminal.handle(Terminal.Signal.INT, s -> client.cancel());
      shell.banner();
      shell.refreshSchema();
      while (!shell.quit) {
        String line;
        try {
          line = reader.readLine(shell.prompt());
        } catch (UserInterruptException e) {
          continue; // Ctrl-C at the prompt: drop the line
        } catch (EndOfFileException e) {
          break;
        }
        String text = line.strip();
        if (text.isEmpty()) {
          continue;
        }
        try {
          shell.runText(text, false);
        } catch (IOException e) {
          w.println(theme.error("✖ the connection broke: " + e.getMessage()));
          w.flush();
          return FAILED;
        }
        w.flush();
      }
      try {
        reader.getHistory().save();
      } catch (IOException ignored) {
        // history is a convenience
      }
      w.println(theme.dim("bye"));
      w.flush();
      return OK;
    }
  }

  /** Multi-line input: Enter continues the statement until it ends with a {@code ;}. */
  static final class StatementParser implements Parser {
    private final DefaultParser words = new DefaultParser();

    StatementParser() {
      words.setEscapeChars(new char[0]);
    }

    @Override
    public ParsedLine parse(String line, int cursor, ParseContext context) throws SyntaxError {
      if (context == ParseContext.ACCEPT_LINE && !SqlLexer.complete(line)) {
        throw new EOFError(-1, -1, "the statement continues", ";");
      }
      return words.parse(line, cursor, context);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // The banner and the prompt

  private static final String[] LOGO = {
    "   ___  _     _   ___  ___ ",
    "  |   \\(_)___| |_|   \\| _ )",
    "  | |) | (_-<|  _| |) | _ \\",
    "  |___/|_/__/ \\__|___/|___/",
  };

  private static final String[] LOGO_COLORS = {"38;5;69", "38;5;75", "38;5;111", "38;5;147"};

  void banner() {
    out.println();
    for (int i = 0; i < LOGO.length; i++) {
      out.println(theme.paint(LOGO[i], "1;" + LOGO_COLORS[i]));
    }
    out.println();
    String version = client.parameters().getOrDefault("server_version", "?");
    out.println(
        "  "
            + theme.bold("DistDB " + VERSION)
            + theme.dim(" · ")
            + "connected to "
            + theme.accent(client.host() + ":" + client.port())
            + theme.dim(" · PostgreSQL protocol 3.0, server " + version));
    String cluster = clusterSummary();
    if (cluster != null) {
      out.println("  " + cluster);
    }
    out.println(
        theme.dim("  ")
            + theme.paint("\\?", Theme.META)
            + theme.dim(" help · ")
            + theme.paint("\\dt", Theme.META)
            + theme.dim(" tables · ")
            + theme.paint("\\nodes", Theme.META)
            + theme.dim(" cluster · Tab completes · ↑ history · ")
            + theme.paint("\\q", Theme.META)
            + theme.dim(" quits"));
    out.println();
    out.flush();
  }

  /** "cluster: 5 nodes (5 alive), 23 ranges", or null if this is not a DistDB cluster. */
  private String clusterSummary() {
    try {
      PgClient.Result nodes = single("SHOW NODES");
      PgClient.Result ranges = single("SHOW RANGES");
      if (nodes == null || ranges == null || nodes.rows().isEmpty()) {
        return null;
      }
      long alive = nodes.rows().stream().filter(r -> "t".equals(r[2])).count();
      return theme.dim("cluster: ")
          + theme.ok(nodes.rows().size() + " nodes")
          + theme.dim(" (" + alive + " alive), ")
          + theme.remote(ranges.rows().size() + (ranges.rows().size() == 1 ? " range" : " ranges"));
    } catch (IOException e) {
      return null;
    }
  }

  String prompt() {
    char s = client.transactionStatus();
    String mark =
        switch (s) {
          case 'T' -> theme.warn("*>");
          case 'E' -> theme.error("!>");
          default -> theme.accent("=>");
        };
    return theme.paint("distdb", "1;" + Theme.ACCENT) + mark + " ";
  }

  // -----------------------------------------------------------------------------------------------
  // Running statements

  /**
   * Runs one input: a backslash command, or SQL (one or more statements). Returns whether every
   * statement succeeded. {@code wholeString}: send it as one query, as psql's {@code -c} does.
   */
  boolean runText(String text, boolean wholeString) throws IOException {
    String t = text.strip();
    if (t.startsWith("\\")) {
      echo(t);
      return meta(t);
    }
    if (wholeString) {
      return sql(t);
    }
    boolean ok = true;
    for (String statement : SqlLexer.split(t)) {
      echo(statement);
      ok &= statement.startsWith("\\") ? meta(statement) : sql(statement);
      if (!ok && !boxed) {
        return false;
      }
    }
    return ok;
  }

  /** A script: backslash lines run as commands, the rest as statements; stops at an error. */
  boolean runScript(String script) throws IOException {
    StringBuilder pending = new StringBuilder();
    for (String line : script.split("\\R", -1)) {
      if (pending.isEmpty() && line.strip().startsWith("\\")) {
        if (!meta(line.strip())) {
          return false;
        }
        continue;
      }
      pending.append(line).append('\n');
      if (SqlLexer.complete(pending.toString())) {
        String text = pending.toString().strip();
        pending.setLength(0);
        if (!text.isEmpty() && !runText(text, false)) {
          return false;
        }
      }
    }
    String rest = pending.toString().strip();
    return rest.isEmpty() || SqlLexer.split(rest).isEmpty() || runText(rest, false);
  }

  /** Runs SQL and prints what came back; returns whether everything succeeded. */
  boolean sql(String sql) throws IOException {
    long started = System.nanoTime();
    List<PgClient.Outcome> outcomes = client.query(sql);
    Duration took = Duration.ofNanos(System.nanoTime() - started);
    boolean ok = true;
    boolean schemaChanged = false;
    for (int i = 0; i < outcomes.size(); i++) {
      PgClient.Outcome o = outcomes.get(i);
      Duration shown = timing && i == outcomes.size() - 1 ? took : null;
      for (PgClient.Notice n : o.notices()) {
        (boxed ? out : err).println(Render.notice(n, theme, boxed));
      }
      switch (o) {
        case PgClient.Ok r -> {
          print(r.result(), shown);
          String tag = r.result().tag();
          schemaChanged |=
              tag.startsWith("CREATE") || tag.startsWith("DROP") || tag.startsWith("ALTER");
        }
        case PgClient.Failed f -> {
          ok = false;
          for (String line : Render.error(f.error(), sql, theme, boxed)) {
            (boxed ? out : err).println(line);
          }
          if (boxed) {
            out.println();
          }
        }
      }
    }
    if (outcomes.isEmpty() && boxed) {
      out.println(theme.dim("(empty statement)"));
    }
    if (schemaChanged && boxed) {
      refreshSchema();
    }
    out.flush();
    err.flush();
    return ok;
  }

  private void print(PgClient.Result r, Duration took) {
    if (!r.hasRows()) {
      out.println(boxed ? Render.command(r.tag(), took, theme) : r.tag());
      if (boxed) {
        out.println();
      }
      return;
    }
    Render.Options o = new Render.Options(theme, width.getAsInt(), expanded, boxed);
    for (String line : Render.rows(r, o)) {
      out.println(line);
    }
    boolean plan = r.columns().size() == 1 && r.columns().getFirst().name().equals("QUERY PLAN");
    if (boxed) {
      if (!plan) {
        out.println(Render.footer(r.rows().size(), took, theme));
      }
      out.println();
    } else {
      out.println(Render.rowCount(r.rows().size()));
      out.println();
    }
  }

  /** Runs one statement for the shell's own use; null if it failed. */
  private PgClient.Result single(String sql) throws IOException {
    List<PgClient.Outcome> outcomes = client.query(sql);
    if (outcomes.size() == 1 && outcomes.getFirst() instanceof PgClient.Ok ok) {
      return ok.result();
    }
    return null;
  }

  /** Reloads table and column names for completion (only outside a transaction). */
  void refreshSchema() {
    if (client.transactionStatus() != 'I') {
      return;
    }
    try {
      PgClient.Result tables = single("SHOW TABLES");
      if (tables == null || !tables.hasRows()) {
        return;
      }
      Map<String, List<String>> schema = new LinkedHashMap<>();
      for (String[] row : tables.rows()) {
        PgClient.Result cols = single("SHOW COLUMNS FROM " + quote(row[0]));
        List<String> names = new ArrayList<>();
        if (cols != null) {
          cols.rows().forEach(c -> names.add(c[0]));
        }
        schema.put(row[0], names);
      }
      completer.schema(schema);
    } catch (IOException ignored) {
      // completion is a convenience
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Backslash commands

  /** In {@code --echo} mode, the prompt and the statement, highlighted. */
  private void echo(String text) {
    if (echo) {
      String shown = theme.colors() ? new SqlHighlighter().highlight(null, text).toAnsi() : text;
      out.println(prompt() + shown);
    }
  }

  boolean meta(String line) throws IOException {
    String[] parts = line.strip().split("\\s+", 2);
    String arg = parts.length > 1 ? parts[1].strip() : "";
    if (arg.endsWith(";")) {
      arg = arg.substring(0, arg.length() - 1).strip();
    }
    Meta m = Meta.of(parts[0]);
    if (m == null) {
      (boxed ? out : err)
          .println(
              theme.error("✖ unknown command " + parts[0])
                  + theme.dim(" (\\? lists the commands)"));
      return false;
    }
    switch (m) {
      case HELP -> help();
      case QUIT -> quit = true;
      case TABLES -> {
        return titled("Tables", "SHOW TABLES");
      }
      case DESCRIBE -> {
        if (arg.isEmpty()) {
          return titled("Tables", "SHOW TABLES");
        }
        return titled("Table " + arg, "SHOW COLUMNS FROM " + quote(arg))
            && titled("Indexes", "SHOW INDEXES FROM " + quote(arg));
      }
      case INDEXES -> {
        if (!arg.isEmpty()) {
          return titled("Indexes of " + arg, "SHOW INDEXES FROM " + quote(arg));
        }
        PgClient.Result tables = single("SHOW TABLES");
        if (tables != null) {
          for (String[] t : tables.rows()) {
            titled("Indexes of " + t[0], "SHOW INDEXES FROM " + quote(t[0]));
          }
        }
      }
      case STATS -> {
        if (arg.isEmpty()) {
          out.println(theme.error("✖ \\stats needs a table"));
          return false;
        }
        return titled("Statistics of " + arg, "SHOW STATISTICS FOR " + quote(arg));
      }
      case RANGES -> {
        return titled(
            arg.isEmpty() ? "Ranges" : "Ranges of " + arg,
            arg.isEmpty() ? "SHOW RANGES" : "SHOW RANGES FROM TABLE " + quote(arg));
      }
      case NODES -> {
        return titled("Nodes", "SHOW NODES");
      }
      case PUSHDOWN -> {
        if (arg.isEmpty()) {
          return sql("SHOW distdb_pushdown");
        }
        return sql("SET distdb_pushdown = " + arg);
      }
      case TIMING -> {
        timing = arg.isEmpty() ? !timing : arg.equalsIgnoreCase("on");
        out.println(theme.dim("timing is " + (timing ? "on" : "off")));
      }
      case EXPANDED -> {
        expanded =
            switch (arg.toLowerCase(Locale.ROOT)) {
              case "on" -> Render.Expanded.ON;
              case "off" -> Render.Expanded.OFF;
              case "auto" -> Render.Expanded.AUTO;
              default -> expanded == Render.Expanded.ON ? Render.Expanded.OFF : Render.Expanded.ON;
            };
        out.println(theme.dim("expanded display is " + expanded.name().toLowerCase(Locale.ROOT)));
      }
      case INCLUDE -> {
        if (arg.isEmpty()) {
          out.println(theme.error("✖ \\i needs a file"));
          return false;
        }
        return runScript(Files.readString(Path.of(arg), StandardCharsets.UTF_8));
      }
      case CONNINFO -> {
        out.println(
            "connected to "
                + theme.accent(client.host() + ":" + client.port())
                + " · server "
                + client.parameters().getOrDefault("server_version", "?")
                + " · backend "
                + client.backendPid()
                + " · transaction "
                + switch (client.transactionStatus()) {
                  case 'T' -> "open";
                  case 'E' -> "failed";
                  default -> "none";
                });
      }
      case CLEAR -> out.print("\u001b[H\u001b[2J");
    }
    out.flush();
    return true;
  }

  private boolean titled(String title, String sql) throws IOException {
    if (boxed) {
      out.println(theme.bold(title));
    }
    return sql(sql);
  }

  private void help() {
    out.println(theme.bold("Commands"));
    for (Meta m : Meta.values()) {
      String left = m.command + (m.args.isEmpty() ? "" : " " + m.args);
      out.println("  " + theme.paint(Text.pad(left, 22, false), Theme.META) + theme.dim(m.help));
    }
    out.println();
    out.println(theme.bold("SQL"));
    out.println(
        theme.dim("  end a statement with ")
            + theme.accent(";")
            + theme.dim(" · Enter continues it on the next line · Ctrl-C clears the line"));
    out.println(
        theme.dim("  try ")
            + theme.accent("EXPLAIN ANALYZE SELECT ...")
            + theme.dim(" to see where each operator ran, and ")
            + theme.accent("ANALYZE")
            + theme.dim(" to give the planner statistics"));
    out.println();
  }

  /** {@code name} as a SQL identifier: quoted unless it is a plain lowercase name. */
  static String quote(String name) {
    if (name.length() > 1 && name.startsWith("\"") && name.endsWith("\"")) {
      return name;
    }
    String lower = name.toLowerCase(Locale.ROOT);
    if (lower.matches("[a-z_][a-z0-9_]*")) {
      return lower;
    }
    return "\"" + name.replace("\"", "\"\"") + "\"";
  }
}

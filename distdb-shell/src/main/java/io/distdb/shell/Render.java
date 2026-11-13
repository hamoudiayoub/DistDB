package io.distdb.shell;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns results into lines of text: framed tables for people, psql's aligned format for scripts,
 * the expanded layout for wide rows, and query plans as trees. Pure functions of their input, so
 * the shell's look is unit-tested.
 */
final class Render {

  private Render() {}

  /** How results are laid out. */
  enum Expanded {
    OFF,
    ON,
    /** Expanded only when the table would not fit the terminal. */
    AUTO
  }

  /**
   * @param boxed frames and colors for a person; {@code false}: psql's aligned text for scripts
   * @param width the terminal's width in columns
   */
  record Options(Theme theme, int width, Expanded expanded, boolean boxed) {}

  /** PostgreSQL type OIDs whose values are numbers, which are right-aligned. */
  private static final Set<Integer> NUMERIC = Set.of(20, 21, 23, 26, 700, 701, 790, 1700);

  static boolean numeric(int oid) {
    return NUMERIC.contains(oid);
  }

  /** A query's rows as a table (or as records, or as a plan tree), without the footer. */
  static List<String> rows(PgClient.Result r, Options o) {
    List<String> heads = new ArrayList<>();
    boolean[] right = new boolean[r.columns().size()];
    for (int i = 0; i < r.columns().size(); i++) {
      heads.add(r.columns().get(i).name());
      right[i] = numeric(r.columns().get(i).typeOid());
    }
    if (o.boxed() && heads.size() == 1 && heads.getFirst().equals("QUERY PLAN")) {
      List<String> lines = new ArrayList<>();
      for (String[] row : r.rows()) {
        lines.add(row[0] == null ? "" : row[0]);
      }
      return plan(lines, o.theme(), o.width());
    }
    if (!o.boxed()) {
      return o.expanded() == Expanded.ON
          ? plainRecords(heads, r.rows())
          : plain(heads, right, r.rows());
    }
    boolean records =
        o.expanded() == Expanded.ON
            || (o.expanded() == Expanded.AUTO
                && heads.size() > 1
                && naturalWidth(heads, r.rows()) > o.width());
    return records ? records(heads, r.rows(), o) : boxed(heads, right, r.rows(), o);
  }

  // -----------------------------------------------------------------------------------------------
  // Framed tables

  private static int naturalWidth(List<String> heads, List<String[]> rows) {
    int[] w = widths(heads, rows);
    int total = 1;
    for (int x : w) {
      total += x + 3;
    }
    return total;
  }

  private static int[] widths(List<String> heads, List<String[]> rows) {
    int[] w = new int[heads.size()];
    for (int i = 0; i < w.length; i++) {
      w[i] = Text.width(heads.get(i));
    }
    for (String[] row : rows) {
      for (int i = 0; i < w.length && i < row.length; i++) {
        w[i] = Math.max(w[i], Text.width(cell(row[i])));
      }
    }
    return w;
  }

  private static String cell(String v) {
    return v == null ? "NULL" : Text.oneLine(v);
  }

  static List<String> boxed(List<String> heads, boolean[] right, List<String[]> rows, Options o) {
    Theme t = o.theme();
    int n = heads.size();
    int[] w = widths(heads, rows);
    // Shrink the widest columns until the frame fits (each keeps at least a few characters).
    int total = 1;
    for (int x : w) {
      total += x + 3;
    }
    while (total > o.width()) {
      int widest = 0;
      for (int i = 1; i < n; i++) {
        if (w[i] > w[widest]) {
          widest = i;
        }
      }
      if (w[widest] <= 6) {
        break;
      }
      w[widest]--;
      total--;
    }
    List<String> out = new ArrayList<>();
    out.add(t.border(frame('╭', '┬', '╮', w)));
    StringBuilder h = new StringBuilder(t.border("│"));
    for (int i = 0; i < n; i++) {
      h.append(' ')
          .append(t.header(Text.pad(Text.cut(heads.get(i), w[i]), w[i], false)))
          .append(' ')
          .append(t.border("│"));
    }
    out.add(h.toString());
    out.add(t.border(frame('├', '┼', '┤', w)));
    for (String[] row : rows) {
      StringBuilder b = new StringBuilder(t.border("│"));
      for (int i = 0; i < n; i++) {
        String v = i < row.length ? row[i] : null;
        String text = Text.pad(Text.cut(cell(v), w[i]), w[i], right[i]);
        b.append(' ').append(v == null ? t.nul(text) : text).append(' ').append(t.border("│"));
      }
      out.add(b.toString());
    }
    out.add(t.border(frame('╰', '┴', '╯', w)));
    return out;
  }

  private static String frame(char left, char mid, char rightCorner, int[] w) {
    StringBuilder b = new StringBuilder().append(left);
    for (int i = 0; i < w.length; i++) {
      b.append("─".repeat(w[i] + 2)).append(i == w.length - 1 ? rightCorner : mid);
    }
    return b.toString();
  }

  static List<String> records(List<String> heads, List<String[]> rows, Options o) {
    Theme t = o.theme();
    int keyWidth = 0;
    for (String h : heads) {
      keyWidth = Math.max(keyWidth, Text.width(h));
    }
    int valueWidth = Math.max(10, o.width() - keyWidth - 4);
    List<String> out = new ArrayList<>();
    for (int r = 0; r < rows.size(); r++) {
      String title = "─ row " + (r + 1) + " ";
      out.add(t.border(title + "─".repeat(Math.max(3, Math.min(o.width(), 60) - title.length()))));
      String[] row = rows.get(r);
      for (int i = 0; i < heads.size(); i++) {
        String v = i < row.length ? row[i] : null;
        String text = Text.cut(cell(v), valueWidth);
        out.add(
            " "
                + t.header(Text.pad(heads.get(i), keyWidth, false))
                + t.border(" │ ")
                + (v == null ? t.nul(text) : text));
      }
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // psql's aligned format, for scripts

  static List<String> plain(List<String> heads, boolean[] right, List<String[]> rows) {
    int[] w = new int[heads.size()];
    for (int i = 0; i < w.length; i++) {
      w[i] = Text.width(heads.get(i));
    }
    for (String[] row : rows) {
      for (int i = 0; i < w.length; i++) {
        w[i] = Math.max(w[i], Text.width(plainCell(row[i])));
      }
    }
    List<String> out = new ArrayList<>();
    StringBuilder h = new StringBuilder();
    StringBuilder rule = new StringBuilder();
    for (int i = 0; i < w.length; i++) {
      // psql centers column names
      int extra = w[i] - Text.width(heads.get(i));
      String centered = " ".repeat(extra / 2) + heads.get(i) + " ".repeat(extra - extra / 2);
      h.append(i == 0 ? " " : " | ").append(centered);
      rule.append(i == 0 ? "-" : "-+-").append("-".repeat(w[i]));
    }
    out.add(Text.stripTrailing(h.toString()));
    out.add(rule.append('-').toString());
    for (String[] row : rows) {
      StringBuilder b = new StringBuilder();
      for (int i = 0; i < w.length; i++) {
        b.append(i == 0 ? " " : " | ").append(Text.pad(plainCell(row[i]), w[i], right[i]));
      }
      out.add(Text.stripTrailing(b.toString()));
    }
    return out;
  }

  private static String plainCell(String v) {
    return v == null ? "" : Text.oneLine(v);
  }

  static List<String> plainRecords(List<String> heads, List<String[]> rows) {
    int keyWidth = 0;
    for (String h : heads) {
      keyWidth = Math.max(keyWidth, Text.width(h));
    }
    List<String> out = new ArrayList<>();
    for (int r = 0; r < rows.size(); r++) {
      out.add("-[ RECORD " + (r + 1) + " ]" + "-".repeat(Math.max(1, keyWidth - 10)));
      for (int i = 0; i < heads.size(); i++) {
        out.add(Text.pad(heads.get(i), keyWidth, false) + " | " + plainCell(rows.get(r)[i]));
      }
    }
    return out;
  }

  /** psql's row count footer: {@code (1 row)}, {@code (3 rows)}. */
  static String rowCount(int n) {
    return "(" + n + (n == 1 ? " row)" : " rows)");
  }

  // -----------------------------------------------------------------------------------------------
  // Plans

  /** Operators, longest first, so "remote scan" wins over "scan". */
  private static final List<String> OPERATORS =
      List.of(
          "merge aggregate",
          "hash aggregate",
          "remote scan",
          "index scan",
          "full scan",
          "point get",
          "nested loop",
          "sort by",
          "one row",
          "distinct",
          "project",
          "filter",
          "values",
          "lookup",
          "limit",
          "hash",
          "scan",
          "union",
          "intersect",
          "except",
          "insert into",
          "update",
          "delete from");

  private static final Pattern STATS = Pattern.compile("^(.*?)  \\((.*)\\)$");
  private static final Pattern TIME = Pattern.compile("time=([0-9.]+) ms");
  private static final Pattern DONE = Pattern.compile("^[A-Z]+( [0-9]+)* in [0-9.]+ ms$");

  private record PlanLine(int depth, String text, boolean label, double time) {}

  /** EXPLAIN's indented lines as a tree, colored: operators, their details, what they did. */
  static List<String> plan(List<String> raw, Theme t) {
    return plan(raw, t, Integer.MAX_VALUE);
  }

  /**
   * The plan as a tree no wider than {@code width} where it can be: an operator whose measured
   * numbers do not fit on its line gets them on the next one, under it.
   */
  static List<String> plan(List<String> raw, Theme t, int width) {
    List<PlanLine> lines = new ArrayList<>();
    List<String> footer = new ArrayList<>();
    for (String s : raw) {
      int spaces = 0;
      while (spaces < s.length() && s.charAt(spaces) == ' ') {
        spaces++;
      }
      String rest = s.substring(spaces);
      if (spaces == 0 && DONE.matcher(rest).matches()) {
        footer.add(rest);
        continue;
      }
      Matcher m = TIME.matcher(rest);
      double time = m.find() ? Double.parseDouble(m.group(1)) : -1;
      if (rest.startsWith("-> ")) {
        lines.add(new PlanLine(spaces / 2, rest.substring(3), false, time));
      } else if (spaces > 0) {
        lines.add(new PlanLine((spaces - 3) / 2 + 1, rest, true, -1));
      } else {
        lines.add(new PlanLine(0, rest, false, time));
      }
    }
    // An operator's time includes its inputs'. Its own share is its time minus theirs.
    double total = 0;
    double[] self = new double[lines.size()];
    for (int i = 0; i < lines.size(); i++) {
      PlanLine l = lines.get(i);
      if (l.time() < 0) {
        continue;
      }
      total = Math.max(total, l.time());
      double children = 0;
      for (int j = i + 1; j < lines.size() && lines.get(j).depth() > l.depth(); j++) {
        PlanLine c = lines.get(j);
        if (c.depth() == l.depth() + 1 && c.time() >= 0 && !c.label()) {
          children += c.time();
        }
      }
      self[i] = Math.max(0, l.time() - children);
    }
    List<String> out = new ArrayList<>();
    for (int i = 0; i < lines.size(); i++) {
      PlanLine l = lines.get(i);
      StringBuilder prefix = new StringBuilder();
      for (int level = 1; level <= l.depth(); level++) {
        boolean more = laterSibling(lines, i, level);
        if (level < l.depth()) {
          prefix.append(more ? "│  " : "   ");
        } else {
          prefix.append(more ? "├─ " : "└─ ");
        }
      }
      String bar = "";
      if (total > 0 && l.time() >= 0) {
        double share = self[i] / total;
        bar =
            "  "
                + t.warn(Text.pad(bar(share), 8, false))
                + t.dim(String.format(Locale.ROOT, " %3.0f%%", share * 100));
      }
      String[] parts = nodeParts(l, t);
      String one = t.border(prefix.toString()) + parts[0] + parts[1] + bar;
      if (parts[1].isEmpty() || Text.width(one) <= width) {
        out.add(one);
        continue;
      }
      // Too wide: the numbers go on a line of their own, under the operator, the tree kept intact.
      StringBuilder cont = new StringBuilder();
      for (int level = 1; level <= l.depth(); level++) {
        cont.append(laterSibling(lines, i, level) ? "│  " : "   ");
      }
      boolean children = i + 1 < lines.size() && lines.get(i + 1).depth() > l.depth();
      cont.append(children ? "│  " : "   ");
      out.add(t.border(prefix.toString()) + parts[0] + bar);
      out.add(t.border(cont.toString()) + parts[1].strip());
    }
    for (String f : footer) {
      out.add(t.dim("⏱  " + f));
    }
    return out;
  }

  /** Whether a line at {@code level} follows line {@code i} before the tree climbs above it. */
  private static boolean laterSibling(List<PlanLine> lines, int i, int level) {
    for (int j = i + 1; j < lines.size(); j++) {
      int d = lines.get(j).depth();
      if (d < level) {
        return false;
      }
      if (d == level) {
        return true;
      }
    }
    return false;
  }

  /** An operator's line in two parts: what it is, and (with two spaces) what it did, if known. */
  private static String[] nodeParts(PlanLine l, Theme t) {
    if (l.label()) {
      return new String[] {t.dim(l.text()), ""};
    }
    String body = l.text();
    String stats = null;
    Matcher m = STATS.matcher(body);
    if (m.matches()) {
      body = m.group(1);
      stats = m.group(2);
    }
    String op = "";
    String lower = body.toLowerCase(Locale.ROOT);
    for (String candidate : OPERATORS) {
      if (lower.startsWith(candidate)) {
        op = body.substring(0, candidate.length());
        // "hash left join", "lookup join", "nested loop anti join": the join's kind is the operator
        int join = lower.indexOf("join", candidate.length());
        if (join >= 0
            && join < candidate.length() + 12
            && candidate.matches("hash|lookup|nested loop")) {
          op = body.substring(0, join + 4);
        }
        break;
      }
    }
    String details = body.substring(op.length());
    boolean remote = op.startsWith("remote") || op.startsWith("merge");
    StringBuilder b = new StringBuilder();
    b.append(
        op.isEmpty()
            ? ""
            : remote ? t.paint(op, "1;" + Theme.REMOTE) : t.paint(op, "1;" + Theme.ACCENT));
    b.append(
        details.contains("partial aggregate")
            ? details.replace("partial aggregate", t.remote("partial aggregate"))
            : details);
    return new String[] {b.toString(), stats == null ? "" : "  " + stats(stats, t)};
  }

  /** The measured numbers of EXPLAIN ANALYZE, numbers highlighted. */
  private static String stats(String s, Theme t) {
    if (s.contains("ran on the gateway")) {
      return t.warn("(" + s + ")");
    }
    StringBuilder b = new StringBuilder(t.dim("("));
    Matcher m = Pattern.compile("[0-9][0-9.,]*").matcher(s);
    int at = 0;
    while (m.find()) {
      b.append(t.dim(s.substring(at, m.start()))).append(t.warn(m.group()));
      at = m.end();
    }
    return b.append(t.dim(s.substring(at) + ")")).toString();
  }

  /** A bar of eight cells, filled to {@code fraction}, with eighth-cell resolution. */
  static String bar(double fraction) {
    String blocks = " ▏▎▍▌▋▊▉█";
    int eighths = (int) Math.round(Math.max(0, Math.min(1, fraction)) * 64);
    StringBuilder b = new StringBuilder();
    for (int cell = 0; cell < 8; cell++) {
      int fill = Math.max(0, Math.min(8, eighths - cell * 8));
      b.append(blocks.charAt(fill));
    }
    return Text.stripTrailing(b.toString());
  }

  // -----------------------------------------------------------------------------------------------
  // Footers, commands, errors, notices

  static String elapsed(Duration d) {
    double ms = d.toNanos() / 1e6;
    if (ms < 1) {
      return String.format(Locale.ROOT, "%.0f µs", ms * 1000);
    }
    if (ms < 1000) {
      return String.format(Locale.ROOT, "%.1f ms", ms);
    }
    return String.format(Locale.ROOT, "%.2f s", ms / 1000);
  }

  /** "3 rows · 4.2 ms", dimmed. */
  static String footer(int rows, Duration elapsed, Theme t) {
    String n = rows + (rows == 1 ? " row" : " rows");
    return t.dim(elapsed == null ? n : n + " · " + elapsed(elapsed));
  }

  /** A command's tag: "✔ CREATE TABLE · 12 ms". */
  static String command(String tag, Duration elapsed, Theme t) {
    return t.ok("✔ ") + t.bold(tag) + (elapsed == null ? "" : t.dim(" · " + elapsed(elapsed)));
  }

  static List<String> error(PgClient.PgError e, String sql, Theme t, boolean boxed) {
    List<String> out = new ArrayList<>();
    if (!boxed) {
      out.add(e.severity() + ":  " + e.getMessage());
      if (e.detail() != null) {
        out.add("DETAIL:  " + e.detail());
      }
      if (e.hint() != null) {
        out.add("HINT:  " + e.hint());
      }
      return out;
    }
    out.add(
        t.paint("✖ " + e.severity(), "1;" + Theme.ERROR)
            + " "
            + t.dim(e.code())
            + "  "
            + t.error(e.getMessage()));
    if (e.position() > 0 && sql != null && e.position() <= sql.length()) {
      int lineStart = sql.lastIndexOf('\n', e.position() - 1) + 1;
      int lineEnd = sql.indexOf('\n', e.position() - 1);
      String line = sql.substring(lineStart, lineEnd < 0 ? sql.length() : lineEnd);
      int col = e.position() - 1 - lineStart;
      out.add("  " + t.dim("│ ") + line);
      out.add("  " + t.dim("│ ") + " ".repeat(Math.max(0, col)) + t.error("^"));
    }
    if (e.detail() != null) {
      out.add("  " + t.dim("detail: ") + e.detail());
    }
    if (e.hint() != null) {
      out.add("  " + t.dim("hint:   ") + t.accent(e.hint()));
    }
    hintFor(e.code()).ifPresent(h -> out.add("  " + t.dim("hint:   ") + t.accent(h)));
    return out;
  }

  /** What to do next, for the errors a newcomer meets first. */
  static java.util.Optional<String> hintFor(String code) {
    return java.util.Optional.ofNullable(
        switch (code) {
          case "40001" -> "another transaction changed these rows first; run the transaction again";
          case "25P02" -> "the transaction failed: ROLLBACK, then start again";
          case "42P01" -> "\\dt lists the tables";
          case "42703" -> "\\d TABLE lists a table's columns";
          default -> null;
        });
  }

  static String notice(PgClient.Notice n, Theme t, boolean boxed) {
    if (!boxed) {
      return n.severity() + ":  " + n.message();
    }
    return t.warn("● " + n.severity().toLowerCase(Locale.ROOT) + ": ") + n.message();
  }
}

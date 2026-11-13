package io.distdb.shell;

/**
 * The shell's colors, as ANSI SGR sequences from the 256-color palette. {@link #PLAIN} paints
 * nothing, for scripts, pipes and {@code NO_COLOR}.
 */
public final class Theme {

  public static final Theme COLOR = new Theme(true);
  public static final Theme PLAIN = new Theme(false);

  // Roles, not hues: change the palette here.
  static final String ACCENT = "38;5;75"; // sky blue: the brand, keywords, operators
  static final String REMOTE = "38;5;141"; // lavender: work done on the ranges
  static final String BORDER = "38;5;240"; // dark gray: table frames, tree lines
  static final String HEADER = "1;38;5;117"; // bold light blue: column names
  static final String DIM = "38;5;245"; // gray: footers, NULL, details
  static final String OK = "38;5;114"; // green: success, strings
  static final String WARN = "38;5;221"; // amber: notices, measured numbers
  static final String ERROR = "38;5;203"; // coral red: errors
  static final String NUMBER = "38;5;215"; // orange: numeric literals
  static final String META = "38;5;176"; // pink: backslash commands
  static final String BOLD = "1";
  static final String ITALIC = "3";

  private final boolean on;

  private Theme(boolean on) {
    this.on = on;
  }

  public boolean colors() {
    return on;
  }

  /** {@code text} in the style {@code sgr} (and back to normal after it). */
  String paint(String text, String sgr) {
    if (!on || text.isEmpty()) {
      return text;
    }
    return "\u001b[" + sgr + "m" + text + "\u001b[0m";
  }

  String accent(String s) {
    return paint(s, ACCENT);
  }

  String bold(String s) {
    return paint(s, BOLD);
  }

  String dim(String s) {
    return paint(s, DIM);
  }

  String border(String s) {
    return paint(s, BORDER);
  }

  String header(String s) {
    return paint(s, HEADER);
  }

  String ok(String s) {
    return paint(s, OK);
  }

  String warn(String s) {
    return paint(s, WARN);
  }

  String error(String s) {
    return paint(s, ERROR);
  }

  String remote(String s) {
    return paint(s, REMOTE);
  }

  String nul(String s) {
    return paint(s, DIM + ";" + ITALIC);
  }

  /** Removes ANSI SGR sequences: what the text looks like without colors. */
  static String strip(String s) {
    return s.replaceAll("\u001b\\[[0-9;]*m", "");
  }
}

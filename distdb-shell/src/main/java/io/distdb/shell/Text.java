package io.distdb.shell;

import org.jline.utils.WCWidth;

/** Display widths in terminal columns (wide CJK characters count two), padding and cutting. */
final class Text {

  private Text() {}

  /** The columns {@code s} takes on a terminal (ANSI colors take none). */
  static int width(String s) {
    String plain = Theme.strip(s);
    int w = 0;
    for (int i = 0; i < plain.length(); ) {
      int cp = plain.codePointAt(i);
      w += Math.max(0, WCWidth.wcwidth(cp));
      i += Character.charCount(cp);
    }
    return w;
  }

  /** {@code s} padded with spaces to {@code width} columns, on the left if {@code right}. */
  static String pad(String s, int width, boolean right) {
    int gap = Math.max(0, width - width(s));
    return right ? " ".repeat(gap) + s : s + " ".repeat(gap);
  }

  /** {@code s} cut to {@code width} columns, ending in an ellipsis if anything was cut. */
  static String cut(String s, int width) {
    if (width(s) <= width) {
      return s;
    }
    StringBuilder b = new StringBuilder();
    int w = 0;
    for (int i = 0; i < s.length(); ) {
      int cp = s.codePointAt(i);
      int cw = Math.max(0, WCWidth.wcwidth(cp));
      if (w + cw > width - 1) {
        break;
      }
      b.appendCodePoint(cp);
      w += cw;
      i += Character.charCount(cp);
    }
    return b.append('…').toString();
  }

  /** {@code s} on one line: newlines shown as ↵, tabs as spaces, other controls dropped. */
  static String oneLine(String s) {
    StringBuilder b = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\n') {
        b.append('↵');
      } else if (c == '\t') {
        b.append("    ");
      } else if (c >= ' ' || c == '\u001b') {
        b.append(c == '\u001b' ? '?' : c);
      }
    }
    return b.toString();
  }

  static String stripTrailing(String s) {
    return s.stripTrailing();
  }
}

package io.distdb.sql.exec;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import java.util.Locale;

/** SQL {@code LIKE}: {@code %} matches any run of characters, {@code _} exactly one. */
final class Like {

  private Like() {}

  static boolean matches(String text, String pattern, String escape, boolean caseInsensitive) {
    if (caseInsensitive) {
      text = text.toLowerCase(Locale.ROOT);
      pattern = pattern.toLowerCase(Locale.ROOT);
    }
    int[] t = text.codePoints().toArray();
    int[] p = pattern.codePoints().toArray();
    int esc = escape.isEmpty() ? -1 : escape.codePointAt(0);
    // Compile: each pattern element is a literal code point, ANY_ONE, or ANY_RUN.
    int[] elements = new int[p.length];
    int n = 0;
    for (int k = 0; k < p.length; k++) {
      int c = p[k];
      if (c == esc) {
        if (k + 1 >= p.length) {
          throw new SqlException(
              SqlState.INVALID_ESCAPE_SEQUENCE, "LIKE pattern must not end with escape character");
        }
        elements[n++] = p[++k];
      } else if (c == '%') {
        if (n == 0 || elements[n - 1] != ANY_RUN) {
          elements[n++] = ANY_RUN;
        }
      } else if (c == '_') {
        elements[n++] = ANY_ONE;
      } else {
        elements[n++] = c;
      }
    }
    return match(t, 0, elements, 0, n);
  }

  private static final int ANY_ONE = -2;
  private static final int ANY_RUN = -3;

  /** Greedy match with backtracking to the last {@code %}: linear for typical patterns. */
  private static boolean match(int[] t, int ti, int[] e, int ei, int n) {
    int starE = -1;
    int starT = -1;
    while (ti < t.length) {
      if (ei < n && e[ei] == ANY_RUN) {
        starE = ei++;
        starT = ti;
      } else if (ei < n && (e[ei] == ANY_ONE || e[ei] == t[ti])) {
        ei++;
        ti++;
      } else if (starE >= 0) {
        ei = starE + 1;
        ti = ++starT;
      } else {
        return false;
      }
    }
    while (ei < n && e[ei] == ANY_RUN) {
      ei++;
    }
    return ei == n;
  }
}

package io.distdb.bench;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Writes maps, lists, numbers, strings and booleans as indented JSON. */
final class Json {

  private Json() {}

  static String write(Object o) {
    StringBuilder b = new StringBuilder();
    write(b, o, "");
    return b.toString();
  }

  private static void write(StringBuilder b, Object o, String indent) {
    switch (o) {
      case null -> b.append("null");
      case Map<?, ?> m -> {
        if (m.isEmpty()) {
          b.append("{}");
          return;
        }
        b.append("{\n");
        Iterator<? extends Map.Entry<?, ?>> it = m.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<?, ?> e = it.next();
          b.append(indent).append("  ");
          string(b, String.valueOf(e.getKey()));
          b.append(": ");
          write(b, e.getValue(), indent + "  ");
          b.append(it.hasNext() ? ",\n" : "\n");
        }
        b.append(indent).append('}');
      }
      case List<?> l -> {
        boolean flat = l.stream().allMatch(x -> x instanceof Number);
        b.append('[');
        for (int i = 0; i < l.size(); i++) {
          if (!flat) {
            b.append('\n').append(indent).append("  ");
          }
          write(b, l.get(i), indent + "  ");
          if (i < l.size() - 1) {
            b.append(flat ? ", " : ",");
          }
        }
        if (!flat && !l.isEmpty()) {
          b.append('\n').append(indent);
        }
        b.append(']');
      }
      case Double d -> b.append(d.isNaN() || d.isInfinite() ? "null" : trim(d));
      case Float f -> b.append(trim(f));
      case Number n -> b.append(n);
      case Boolean x -> b.append(x);
      default -> string(b, o.toString());
    }
  }

  private static String trim(double d) {
    return d == Math.rint(d) && Math.abs(d) < 1e15
        ? Long.toString((long) d)
        : String.format(Locale.ROOT, "%.4g", d);
  }

  private static void string(StringBuilder b, String s) {
    b.append('"');
    for (char c : s.toCharArray()) {
      switch (c) {
        case '"' -> b.append("\\\"");
        case '\\' -> b.append("\\\\");
        case '\n' -> b.append("\\n");
        default -> {
          if (c < ' ') {
            b.append(String.format("\\u%04x", (int) c));
          } else {
            b.append(c);
          }
        }
      }
    }
    b.append('"');
  }

  // -----------------------------------------------------------------------------------------------
  // Reading (enough for our own output: objects, arrays, numbers, strings, booleans, null)

  static Object parse(String text) {
    int[] at = {0};
    Object v = value(text, at);
    return v;
  }

  private static void space(String s, int[] at) {
    while (at[0] < s.length() && Character.isWhitespace(s.charAt(at[0]))) {
      at[0]++;
    }
  }

  private static Object value(String s, int[] at) {
    space(s, at);
    char c = s.charAt(at[0]);
    switch (c) {
      case '{' -> {
        at[0]++;
        java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
        space(s, at);
        if (s.charAt(at[0]) == '}') {
          at[0]++;
          return m;
        }
        while (true) {
          space(s, at);
          String k = (String) value(s, at);
          space(s, at);
          at[0]++; // ':'
          m.put(k, value(s, at));
          space(s, at);
          if (s.charAt(at[0]++) == '}') {
            return m;
          }
        }
      }
      case '[' -> {
        at[0]++;
        java.util.List<Object> l = new java.util.ArrayList<>();
        space(s, at);
        if (s.charAt(at[0]) == ']') {
          at[0]++;
          return l;
        }
        while (true) {
          l.add(value(s, at));
          space(s, at);
          if (s.charAt(at[0]++) == ']') {
            return l;
          }
        }
      }
      case '"' -> {
        StringBuilder b = new StringBuilder();
        at[0]++;
        while (s.charAt(at[0]) != '"') {
          char d = s.charAt(at[0]++);
          if (d == '\\') {
            char e = s.charAt(at[0]++);
            switch (e) {
              case 'n' -> b.append('\n');
              case 'u' -> {
                b.append((char) Integer.parseInt(s.substring(at[0], at[0] + 4), 16));
                at[0] += 4;
              }
              default -> b.append(e);
            }
          } else {
            b.append(d);
          }
        }
        at[0]++;
        return b.toString();
      }
      default -> {
        if (s.startsWith("true", at[0])) {
          at[0] += 4;
          return Boolean.TRUE;
        }
        if (s.startsWith("false", at[0])) {
          at[0] += 5;
          return Boolean.FALSE;
        }
        if (s.startsWith("null", at[0])) {
          at[0] += 4;
          return null;
        }
        int start = at[0];
        while (at[0] < s.length() && "+-0123456789.eE".indexOf(s.charAt(at[0])) >= 0) {
          at[0]++;
        }
        return Double.parseDouble(s.substring(start, at[0]));
      }
    }
  }
}

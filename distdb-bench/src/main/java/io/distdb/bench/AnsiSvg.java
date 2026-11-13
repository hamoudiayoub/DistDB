package io.distdb.bench;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Renders a terminal transcript with ANSI colors as an SVG "screenshot" (a window with the text in
 * it), for the README: {@code distdb-bench screenshot IN.ansi OUT.svg [TITLE]}. Understands SGR
 * reset, bold, dim, italic and 256-color foregrounds, which is all the shell uses.
 */
final class AnsiSvg {

  private AnsiSvg() {}

  private static final double CHAR_W = 7.83;
  private static final int LINE_H = 19;
  private static final String FG = "#d7dae0";
  private static final String BG = "#16181d";

  static void main(Path in, Path out, String title) throws Exception {
    String text = Files.readString(in, StandardCharsets.UTF_8).replace("\r", "");
    Files.writeString(out, render(text, title), StandardCharsets.UTF_8);
  }

  private record Span(String text, String color, boolean bold, boolean italic, boolean dim) {}

  static String render(String text, String title) {
    List<List<Span>> lines = new ArrayList<>();
    String color = null;
    boolean bold = false;
    boolean italic = false;
    boolean dim = false;
    int maxCols = 0;
    for (String raw : text.split("\n", -1)) {
      List<Span> spans = new ArrayList<>();
      StringBuilder cur = new StringBuilder();
      int cols = 0;
      for (int i = 0; i < raw.length(); i++) {
        char c = raw.charAt(i);
        if (c == '\u001b' && i + 1 < raw.length() && raw.charAt(i + 1) == '[') {
          int end = i + 2;
          while (end < raw.length() && !Character.isLetter(raw.charAt(end))) {
            end++;
          }
          if (end < raw.length() && raw.charAt(end) == 'm') {
            if (!cur.isEmpty()) {
              spans.add(new Span(cur.toString(), color, bold, italic, dim));
              cur.setLength(0);
            }
            String[] codes = raw.substring(i + 2, end).split(";");
            for (int k = 0; k < codes.length; k++) {
              switch (codes[k]) {
                case "", "0" -> {
                  color = null;
                  bold = false;
                  italic = false;
                  dim = false;
                }
                case "1" -> bold = true;
                case "2" -> dim = true;
                case "3" -> italic = true;
                case "38" -> {
                  if (k + 2 < codes.length && codes[k + 1].equals("5")) {
                    int n = Integer.parseInt(codes[k + 2]);
                    // Frames are drawn in a dark gray that a scaled-down image would lose.
                    color = n == 240 ? "#6e7480" : xterm(n);
                    k += 2;
                  }
                }
                default -> {
                  // other attributes are not used
                }
              }
            }
          }
          i = end;
          continue;
        }
        if (c == '\r') {
          continue;
        }
        cur.append(c);
        cols++;
      }
      if (!cur.isEmpty()) {
        spans.add(new Span(cur.toString(), color, bold, italic, dim));
      }
      maxCols = Math.max(maxCols, cols);
      lines.add(spans);
    }
    while (!lines.isEmpty() && lines.getLast().isEmpty()) {
      lines.removeLast();
    }
    int width = (int) Math.ceil(maxCols * CHAR_W) + 48;
    int height = lines.size() * LINE_H + 70;
    StringBuilder b = new StringBuilder();
    b.append(
        String.format(
            Locale.ROOT,
            "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 %d %d\" width=\"%d\" height=\"%d\""
                + " role=\"img\" aria-label=\"%s\">",
            width,
            height,
            width,
            height,
            esc(title)));
    b.append(
        "<style>text { font-family: 'JetBrains Mono', 'Cascadia Mono', 'SF Mono', Menlo,"
            + " Consolas, monospace; font-size: 13px; white-space: pre; }</style>");
    b.append(
        String.format(
            Locale.ROOT,
            "<rect width=\"%d\" height=\"%d\" rx=\"10\" fill=\"%s\"/>",
            width,
            height,
            BG));
    b.append(
        String.format(
            Locale.ROOT,
            "<rect width=\"%d\" height=\"30\" rx=\"10\" fill=\"#22252c\"/><rect y=\"20\" width=\"%d\""
                + " height=\"10\" fill=\"#22252c\"/>",
            width,
            width));
    String[] dots = {"#ff5f57", "#febc2e", "#28c840"};
    for (int i = 0; i < 3; i++) {
      b.append(
          String.format(
              Locale.ROOT,
              "<circle cx=\"%d\" cy=\"15\" r=\"5.5\" fill=\"%s\"/>",
              18 + i * 18,
              dots[i]));
    }
    b.append(
        String.format(
            Locale.ROOT,
            "<text x=\"%d\" y=\"19\" fill=\"#8b919c\" text-anchor=\"middle\" style=\"font-size:12px\">%s</text>",
            width / 2,
            esc(title)));
    for (int i = 0; i < lines.size(); i++) {
      double y = 52 + i * LINE_H;
      b.append(String.format(Locale.ROOT, "<text x=\"20\" y=\"%.1f\" fill=\"%s\">", y, FG));
      for (Span s : lines.get(i)) {
        StringBuilder attrs = new StringBuilder();
        if (s.color() != null) {
          attrs.append(" fill=\"").append(s.color()).append('"');
        }
        if (s.bold()) {
          attrs.append(" font-weight=\"700\"");
        }
        if (s.italic()) {
          attrs.append(" font-style=\"italic\"");
        }
        if (s.dim() && s.color() == null) {
          attrs.append(" fill=\"#8b919c\"");
        }
        b.append("<tspan").append(attrs).append('>').append(esc(s.text())).append("</tspan>");
      }
      b.append("</text>");
    }
    return b.append("</svg>\n").toString();
  }

  private static String esc(String s) {
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
  }

  /** The xterm 256-color palette. */
  static String xterm(int n) {
    int[] basic = {
      0x000000, 0xcd3131, 0x0dbc79, 0xe5e510, 0x2472c8, 0xbc3fbc, 0x11a8cd, 0xe5e5e5,
      0x666666, 0xf14c4c, 0x23d18b, 0xf5f543, 0x3b8eea, 0xd670d6, 0x29b8db, 0xffffff
    };
    int rgb;
    if (n < 16) {
      rgb = basic[n];
    } else if (n < 232) {
      int i = n - 16;
      int[] level = {0, 95, 135, 175, 215, 255};
      rgb = level[i / 36] << 16 | level[(i / 6) % 6] << 8 | level[i % 6];
    } else {
      int g = 8 + 10 * (n - 232);
      rgb = g << 16 | g << 8 | g;
    }
    return String.format("#%06x", rgb);
  }
}

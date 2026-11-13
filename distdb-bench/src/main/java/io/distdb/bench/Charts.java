package io.distdb.bench;

import java.util.List;
import java.util.Locale;

/**
 * Benchmark charts as standalone SVG, light and dark (the SVG follows the viewer's color scheme).
 * The palette is the data-viz reference palette's first three categorical slots, validated in both
 * modes; every mark carries a {@code <title>} (its tooltip) and a direct value label.
 */
final class Charts {

  private Charts() {}

  private static final int W = 720;

  private static String style() {
    return """
    <style>
      .surface { fill: #fcfcfb; } .t1 { fill: #0b0b0b; } .t2 { fill: #52514e; } .t3 { fill: #75746f; }
      .grid { stroke: #e7e6e2; stroke-width: 1; } .axis { stroke: #c9c8c2; stroke-width: 1; }
      .s1 { fill: #2a78d6; } .s2 { fill: #eb6834; } .s3 { fill: #1baf7a; }
      .l1 { stroke: #2a78d6; } .l2 { stroke: #eb6834; } .l3 { stroke: #1baf7a; }
      .mark { stroke: #52514e; }
      text { font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }
      .num { font-variant-numeric: tabular-nums; }
      @media (prefers-color-scheme: dark) {
        .surface { fill: #1a1a19; } .t1 { fill: #f0efec; } .t2 { fill: #c3c2b7; } .t3 { fill: #9a998f; }
        .grid { stroke: #2c2c2a; } .axis { stroke: #44443f; }
        .s1 { fill: #3987e5; } .s2 { fill: #d95926; } .s3 { fill: #199e70; }
        .l1 { stroke: #3987e5; } .l2 { stroke: #d95926; } .l3 { stroke: #199e70; }
        .mark { stroke: #c3c2b7; }
      }
    </style>
    """;
  }

  private static String esc(String s) {
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
  }

  static String fmt(double v) {
    if (v == 0) {
      return "0";
    }
    if (v >= 10_000) {
      return String.format(Locale.ROOT, "%,.0f", v);
    }
    if (v >= 100) {
      return String.format(Locale.ROOT, "%.0f", v);
    }
    if (v >= 10) {
      return String.format(Locale.ROOT, "%.1f", v);
    }
    return String.format(Locale.ROOT, "%.2f", v);
  }

  /** A round step for an axis from 0 to about {@code max}, with 4 or 5 gridlines. */
  private static double step(double max) {
    double raw = max / 4;
    double mag = Math.pow(10, Math.floor(Math.log10(raw)));
    for (double m : new double[] {1, 2, 2.5, 5, 10}) {
      if (raw <= m * mag) {
        return m * mag;
      }
    }
    return 10 * mag;
  }

  /** A column whose top corners are rounded (4px), square at the baseline. */
  private static String column(double x, double y, double w, double base, String cls, String tip) {
    double h = base - y;
    double r = Math.min(4, Math.min(w / 2, h));
    return String.format(
        Locale.ROOT,
        "<path class=\"%s\" d=\"M%.1f %.1f V%.1f A%.1f %.1f 0 0 1 %.1f %.1f H%.1f A%.1f %.1f 0 0 1"
            + " %.1f %.1f V%.1f Z\"><title>%s</title></path>",
        cls,
        x,
        base,
        y + r,
        r,
        r,
        x + r,
        y,
        x + w - r,
        r,
        r,
        x + w,
        y + r,
        base,
        esc(tip));
  }

  private static StringBuilder open(int height, String title, String subtitle) {
    StringBuilder b = new StringBuilder();
    b.append(
        String.format(
            Locale.ROOT,
            "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 %d %d\" width=\"%d\""
                + " height=\"%d\" role=\"img\" aria-label=\"%s\">",
            W,
            height,
            W,
            height,
            esc(title)));
    b.append(style());
    b.append(
        String.format(
            Locale.ROOT,
            "<rect class=\"surface\" width=\"%d\" height=\"%d\" rx=\"12\"/>",
            W,
            height));
    b.append(
        String.format(
            Locale.ROOT,
            "<text class=\"t1\" x=\"24\" y=\"34\" font-size=\"16\" font-weight=\"600\">%s</text>",
            esc(title)));
    b.append(
        String.format(
            Locale.ROOT,
            "<text class=\"t2\" x=\"24\" y=\"54\" font-size=\"12\">%s</text>",
            esc(subtitle)));
    return b;
  }

  private static void legend(StringBuilder b, List<String> series, int y) {
    double x = 24;
    for (int s = 0; s < series.size(); s++) {
      b.append(
          String.format(
              Locale.ROOT,
              "<rect class=\"s%d\" x=\"%.1f\" y=\"%d\" width=\"10\" height=\"10\" rx=\"2\"/>",
              s + 1,
              x,
              y - 9));
      b.append(
          String.format(
              Locale.ROOT,
              "<text class=\"t2\" x=\"%.1f\" y=\"%d\" font-size=\"12\">%s</text>",
              x + 15,
              y,
              esc(series.get(s))));
      x += 30 + series.get(s).length() * 6.6;
    }
  }

  /**
   * Grouped columns: {@code values[series][group]}, one y-axis in {@code unit}. Up to three series.
   */
  static String columns(
      String title,
      String subtitle,
      List<String> groups,
      List<String> series,
      double[][] values,
      String unit,
      String note) {
    int height = 360;
    StringBuilder b = open(height, title, subtitle);
    legend(b, series, 80);
    double top = 100;
    double base = height - 56;
    double left = 64;
    double right = W - 24;
    double max = 0;
    for (double[] s : values) {
      for (double v : s) {
        max = Math.max(max, v);
      }
    }
    double step = step(max * 1.12);
    double axisMax = Math.ceil(max * 1.12 / step) * step;
    for (double g = 0; g <= axisMax + 1e-9; g += step) {
      double y = base - (base - top) * g / axisMax;
      b.append(
          String.format(
              Locale.ROOT,
              "<line class=\"%s\" x1=\"%.1f\" x2=\"%.1f\" y1=\"%.1f\" y2=\"%.1f\"/>",
              g == 0 ? "axis" : "grid",
              left,
              right,
              y,
              y));
      b.append(
          String.format(
              Locale.ROOT,
              "<text class=\"t3 num\" x=\"%.1f\" y=\"%.1f\" font-size=\"11\" text-anchor=\"end\">%s</text>",
              left - 8,
              y + 4,
              fmt(g)));
    }
    b.append(
        String.format(
            Locale.ROOT,
            "<text class=\"t3\" x=\"%.1f\" y=\"%.1f\" font-size=\"11\">%s</text>",
            left,
            top - 8,
            esc(unit)));
    double band = (right - left) / groups.size();
    double bar = Math.min(24, (band * 0.6 - 2 * (series.size() - 1)) / series.size());
    double cluster = bar * series.size() + 2 * (series.size() - 1);
    for (int g = 0; g < groups.size(); g++) {
      double x0 = left + band * g + (band - cluster) / 2;
      for (int s = 0; s < series.size(); s++) {
        double v = values[s][g];
        double x = x0 + s * (bar + 2);
        double y = base - (base - top) * v / axisMax;
        b.append(
            column(
                x,
                y,
                bar,
                base,
                "s" + (s + 1),
                series.get(s) + ", " + groups.get(g) + ": " + fmt(v) + " " + unit));
        b.append(
            String.format(
                Locale.ROOT,
                "<text class=\"t2 num\" x=\"%.1f\" y=\"%.1f\" font-size=\"10.5\" text-anchor=\"middle\">%s</text>",
                x + bar / 2,
                y - 5,
                fmt(v)));
      }
      b.append(
          String.format(
              Locale.ROOT,
              "<text class=\"t2\" x=\"%.1f\" y=\"%.1f\" font-size=\"12\" text-anchor=\"middle\">%s</text>",
              left + band * g + band / 2,
              base + 20,
              esc(groups.get(g))));
    }
    if (note != null) {
      b.append(
          String.format(
              Locale.ROOT,
              "<text class=\"t3\" x=\"24\" y=\"%d\" font-size=\"11\">%s</text>",
              height - 14,
              esc(note)));
    }
    return b.append("</svg>\n").toString();
  }

  /** Horizontal bars, one series, a value label at each bar's end ({@code suffix} after it). */
  static String bars(
      String title,
      String subtitle,
      List<String> labels,
      double[] values,
      String suffix,
      String note) {
    int rowH = 40;
    int height = 96 + rowH * labels.size() + 30;
    StringBuilder b = open(height, title, subtitle);
    double left = 210;
    double right = W - 70;
    double max = 0;
    for (double v : values) {
      max = Math.max(max, v);
    }
    double top = 80;
    for (int i = 0; i < labels.size(); i++) {
      double y = top + i * rowH + 8;
      double w = (right - left) * values[i] / max;
      b.append(
          String.format(
              Locale.ROOT,
              "<text class=\"t2\" x=\"%.1f\" y=\"%.1f\" font-size=\"12\" text-anchor=\"end\">%s</text>",
              left - 10,
              y + 15,
              esc(labels.get(i))));
      // a horizontal bar: square at the baseline (left), rounded at its data end (right)
      double r = Math.min(4, w / 2);
      b.append(
          String.format(
              Locale.ROOT,
              "<path class=\"s1\" d=\"M%.1f %.1f H%.1f A%.1f %.1f 0 0 1 %.1f %.1f V%.1f A%.1f %.1f 0 0 1 %.1f %.1f H%.1f Z\"><title>%s: %s%s</title></path>",
              left,
              y + 2,
              left + w - r,
              r,
              r,
              left + w,
              y + 2 + r,
              y + 22 - r,
              r,
              r,
              left + w - r,
              y + 22,
              left,
              esc(labels.get(i)),
              fmt(values[i]),
              esc(suffix)));
      b.append(
          String.format(
              Locale.ROOT,
              "<text class=\"t1 num\" x=\"%.1f\" y=\"%.1f\" font-size=\"12\" font-weight=\"600\">%s%s</text>",
              left + w + 8,
              y + 16,
              fmt(values[i]),
              esc(suffix)));
    }
    b.append(
        String.format(
            Locale.ROOT,
            "<line class=\"axis\" x1=\"%.1f\" x2=\"%.1f\" y1=\"%.1f\" y2=\"%.1f\"/>",
            left,
            left,
            top,
            top + rowH * labels.size()));
    if (note != null) {
      b.append(
          String.format(
              Locale.ROOT,
              "<text class=\"t3\" x=\"24\" y=\"%d\" font-size=\"11\">%s</text>",
              height - 14,
              esc(note)));
    }
    return b.append("</svg>\n").toString();
  }

  /**
   * Lines over x ({@code xs}), up to three series, one y-axis; {@code markX} (or NaN) draws a
   * labelled vertical marker (an event, such as a crash).
   */
  static String lines(
      String title,
      String subtitle,
      double[] xs,
      List<String> series,
      double[][] ys,
      String xUnit,
      String yUnit,
      double markX,
      String markLabel,
      String note) {
    int height = 340;
    StringBuilder b = open(height, title, subtitle);
    if (series.size() > 1) {
      legend(b, series, 80);
    }
    double top = 100;
    double base = height - 56;
    double left = 64;
    double right = W - 24;
    double xmin = xs[0];
    double xmax = xs[xs.length - 1];
    double max = 0;
    for (double[] s : ys) {
      for (double v : s) {
        max = Math.max(max, v);
      }
    }
    double step = step(max * 1.1);
    double axisMax = Math.ceil(max * 1.1 / step) * step;
    for (double g = 0; g <= axisMax + 1e-9; g += step) {
      double y = base - (base - top) * g / axisMax;
      b.append(
          String.format(
              Locale.ROOT,
              "<line class=\"%s\" x1=\"%.1f\" x2=\"%.1f\" y1=\"%.1f\" y2=\"%.1f\"/>",
              g == 0 ? "axis" : "grid",
              left,
              right,
              y,
              y));
      b.append(
          String.format(
              Locale.ROOT,
              "<text class=\"t3 num\" x=\"%.1f\" y=\"%.1f\" font-size=\"11\" text-anchor=\"end\">%s</text>",
              left - 8,
              y + 4,
              fmt(g)));
    }
    b.append(
        String.format(
            Locale.ROOT,
            "<text class=\"t3\" x=\"%.1f\" y=\"%.1f\" font-size=\"11\">%s</text>",
            left,
            top - 8,
            esc(yUnit)));
    double xstep = step(xmax - xmin);
    for (double g = Math.ceil(xmin / xstep) * xstep; g <= xmax + 1e-9; g += xstep) {
      double x = left + (right - left) * (g - xmin) / (xmax - xmin);
      b.append(
          String.format(
              Locale.ROOT,
              "<text class=\"t3 num\" x=\"%.1f\" y=\"%.1f\" font-size=\"11\" text-anchor=\"middle\">%s</text>",
              x,
              base + 18,
              fmt(g)));
    }
    b.append(
        String.format(
            Locale.ROOT,
            "<text class=\"t3\" x=\"%.1f\" y=\"%.1f\" font-size=\"11\" text-anchor=\"end\">%s</text>",
            right,
            base + 36,
            esc(xUnit)));
    if (!Double.isNaN(markX)) {
      double x = left + (right - left) * (markX - xmin) / (xmax - xmin);
      b.append(
          String.format(
              Locale.ROOT,
              "<line class=\"mark\" x1=\"%.1f\" x2=\"%.1f\" y1=\"%.1f\" y2=\"%.1f\" stroke-width=\"1.5\" stroke-dasharray=\"4 3\"/>",
              x,
              x,
              top - 4,
              base));
      b.append(
          String.format(
              Locale.ROOT,
              "<text class=\"t1\" x=\"%.1f\" y=\"%.1f\" font-size=\"12\" font-weight=\"600\">%s</text>",
              x + 6,
              top + 8,
              esc(markLabel)));
    }
    for (int s = 0; s < series.size(); s++) {
      StringBuilder d = new StringBuilder();
      for (int i = 0; i < xs.length; i++) {
        double x = left + (right - left) * (xs[i] - xmin) / (xmax - xmin);
        double y = base - (base - top) * ys[s][i] / axisMax;
        d.append(i == 0 ? "M" : "L").append(String.format(Locale.ROOT, "%.1f %.1f", x, y));
      }
      b.append(
          String.format(
              Locale.ROOT,
              "<path class=\"l%d\" d=\"%s\" fill=\"none\" stroke-width=\"2\" stroke-linejoin=\"round\" stroke-linecap=\"round\"><title>%s</title></path>",
              s + 1,
              d,
              esc(series.get(s))));
      if (xs.length <= 12) {
        for (int i = 0; i < xs.length; i++) {
          double x = left + (right - left) * (xs[i] - xmin) / (xmax - xmin);
          double y = base - (base - top) * ys[s][i] / axisMax;
          b.append(
              String.format(
                  Locale.ROOT,
                  "<circle class=\"s%d\" cx=\"%.1f\" cy=\"%.1f\" r=\"4\" stroke-width=\"2\" style=\"stroke: var(--ring, transparent)\"><title>%s at %s: %s %s</title></circle>",
                  s + 1,
                  x,
                  y,
                  esc(series.get(s)),
                  fmt(xs[i]),
                  fmt(ys[s][i]),
                  esc(yUnit)));
          b.append(
              String.format(
                  Locale.ROOT,
                  "<text class=\"t2 num\" x=\"%.1f\" y=\"%.1f\" font-size=\"10.5\" text-anchor=\"middle\">%s</text>",
                  x,
                  y - 9,
                  fmt(ys[s][i])));
        }
      }
    }
    if (note != null) {
      b.append(
          String.format(
              Locale.ROOT,
              "<text class=\"t3\" x=\"24\" y=\"%d\" font-size=\"11\">%s</text>",
              height - 14,
              esc(note)));
    }
    return b.append("</svg>\n").toString();
  }
}

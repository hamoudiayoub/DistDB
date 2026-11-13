package io.distdb.common;

import java.util.function.Supplier;

/**
 * Structured log events: an event name and {@code key=value} fields, so that one line can be found
 * by range, node or transaction across every node's log ({@code grep 'range=12 '}). Events go
 * through {@link System.Logger}, and the server prints them one per line.
 *
 * <p>Usage: {@code Log.info(LOG, "became leader", "range", r.id, "term", term)}.
 */
public final class Log {

  private Log() {}

  public static void info(System.Logger logger, String event, Object... fields) {
    log(logger, System.Logger.Level.INFO, event, null, fields);
  }

  public static void warn(System.Logger logger, String event, Object... fields) {
    log(logger, System.Logger.Level.WARNING, event, null, fields);
  }

  public static void debug(System.Logger logger, String event, Object... fields) {
    log(logger, System.Logger.Level.DEBUG, event, null, fields);
  }

  public static void error(
      System.Logger logger, String event, Throwable failure, Object... fields) {
    log(logger, System.Logger.Level.ERROR, event, failure, fields);
  }

  private static void log(
      System.Logger logger,
      System.Logger.Level level,
      String event,
      Throwable failure,
      Object... fields) {
    if (!logger.isLoggable(level)) {
      return;
    }
    Supplier<String> line = () -> format(event, fields);
    if (failure == null) {
      logger.log(level, line);
    } else {
      logger.log(level, line, failure);
    }
  }

  /** {@code event key=value key="a value"}: values with spaces or quotes are quoted. */
  public static String format(String event, Object... fields) {
    StringBuilder b = new StringBuilder(quote(event));
    for (int i = 0; i + 1 < fields.length; i += 2) {
      b.append(' ').append(fields[i]).append('=').append(quote(String.valueOf(fields[i + 1])));
    }
    return b.toString();
  }

  static String quote(String v) {
    boolean plain = !v.isEmpty();
    for (int i = 0; i < v.length() && plain; i++) {
      char c = v.charAt(i);
      plain = c > ' ' && c != '"' && c != '=';
    }
    return plain ? v : '"' + v.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
  }
}

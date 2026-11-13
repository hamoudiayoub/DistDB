package io.distdb.server;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.Locale;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Logs as one event per line, {@code key=value}: the time, the level, this process's role and id,
 * the component, then the event's own fields (from {@link io.distdb.common.Log}). For example:
 *
 * <pre>
 * ts=2026-11-13T09:12:01.532Z level=info proc=node-2 component=kv.Store event="raft role changed" node=2 range=14 role=leader term=3 leader=2
 * </pre>
 */
final class LogSetup {

  private LogSetup() {}

  /** Installs the formatter on stderr, for a process whose role and id are {@code proc}. */
  static void install(String proc) {
    Logger root = LogManager.getLogManager().getLogger("");
    for (Handler h : root.getHandlers()) {
      root.removeHandler(h);
    }
    ConsoleHandler h = new ConsoleHandler();
    h.setFormatter(new KeyValueFormatter(proc));
    h.setLevel(Level.ALL);
    root.addHandler(h);
    String level = System.getenv().getOrDefault("DISTDB_LOG", "info");
    root.setLevel(
        switch (level.toLowerCase(Locale.ROOT)) {
          case "debug" -> Level.FINE;
          case "warn", "warning" -> Level.WARNING;
          case "error" -> Level.SEVERE;
          default -> Level.INFO;
        });
    // gRPC and Netty are chatty at INFO.
    Logger.getLogger("io.grpc").setLevel(Level.WARNING);
    Logger.getLogger("io.netty").setLevel(Level.WARNING);
  }

  static final class KeyValueFormatter extends Formatter {
    private final String proc;

    KeyValueFormatter(String proc) {
      this.proc = proc;
    }

    @Override
    public String format(LogRecord r) {
      StringBuilder b = new StringBuilder(160);
      b.append("ts=").append(Instant.ofEpochMilli(r.getMillis()));
      b.append(" level=").append(level(r.getLevel()));
      b.append(" proc=").append(proc);
      b.append(" component=").append(component(r.getLoggerName()));
      String message = formatMessage(r);
      // Events from Log.format already read "event key=value ..."; anything else is a message.
      if (message.startsWith("\"") || message.contains("=")) {
        b.append(" event=").append(message);
      } else {
        b.append(" event=").append(io.distdb.common.Log.format(message));
      }
      if (r.getThrown() != null) {
        StringWriter w = new StringWriter();
        r.getThrown().printStackTrace(new PrintWriter(w));
        b.append(" error=").append(io.distdb.common.Log.format(r.getThrown().toString()));
        b.append(System.lineSeparator()).append(w);
      }
      return b.append(System.lineSeparator()).toString();
    }

    private static String level(Level l) {
      if (l.intValue() >= Level.SEVERE.intValue()) {
        return "error";
      }
      if (l.intValue() >= Level.WARNING.intValue()) {
        return "warn";
      }
      if (l.intValue() >= Level.INFO.intValue()) {
        return "info";
      }
      return "debug";
    }

    private static String component(String logger) {
      if (logger == null) {
        return "-";
      }
      return logger.startsWith("io.distdb.") ? logger.substring("io.distdb.".length()) : logger;
    }
  }
}

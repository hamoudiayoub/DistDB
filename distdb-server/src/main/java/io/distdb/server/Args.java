package io.distdb.server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Command-line arguments: {@code --name value} options anywhere, everything else positional. */
final class Args {

  /** Bad command-line input; reported with the usage text and exit code 2. */
  static final class UsageException extends Exception {
    private static final long serialVersionUID = 1L;

    UsageException(String message) {
      super(message);
    }
  }

  private final Map<String, String> options = new HashMap<>();
  private final List<String> positional = new ArrayList<>();

  private Args() {}

  /**
   * Splits {@code args}. Every option takes a value and must be one of {@code known}. After {@code
   * --}, everything is positional, so a key may start with a dash.
   */
  static Args parse(List<String> args, Set<String> known) throws UsageException {
    Args parsed = new Args();
    boolean onlyPositional = false;
    for (int i = 0; i < args.size(); i++) {
      String a = args.get(i);
      if (onlyPositional || !a.startsWith("--")) {
        parsed.positional.add(a);
      } else if (a.equals("--")) {
        onlyPositional = true;
      } else {
        String name = a.substring(2);
        if (!known.contains(name)) {
          throw new UsageException("unknown option " + a);
        }
        if (i + 1 >= args.size()) {
          throw new UsageException(a + " needs a value");
        }
        parsed.options.put(name, args.get(++i));
      }
    }
    return parsed;
  }

  List<String> positional() {
    return positional;
  }

  String option(String name, String defaultValue) {
    return options.getOrDefault(name, defaultValue);
  }

  String required(String name) throws UsageException {
    String v = options.get(name);
    if (v == null) {
      throw new UsageException("--" + name + " is required");
    }
    return v;
  }

  /** {@code 250ms}, {@code 5s}, {@code 2m}, {@code 1h}; a bare number is seconds. */
  static Duration duration(String text) throws UsageException {
    String t = text.trim().toLowerCase(Locale.ROOT);
    try {
      if (t.endsWith("ms")) {
        return Duration.ofMillis(Long.parseLong(t.substring(0, t.length() - 2)));
      } else if (t.endsWith("s")) {
        return Duration.ofSeconds(Long.parseLong(t.substring(0, t.length() - 1)));
      } else if (t.endsWith("m")) {
        return Duration.ofMinutes(Long.parseLong(t.substring(0, t.length() - 1)));
      } else if (t.endsWith("h")) {
        return Duration.ofHours(Long.parseLong(t.substring(0, t.length() - 1)));
      }
      return Duration.ofSeconds(Long.parseLong(t));
    } catch (NumberFormatException e) {
      throw new UsageException("not a duration: " + text + " (try 500ms, 5s or 2m)");
    }
  }

  /** {@code host:port}, or {@code [v6-address]:port}. */
  static HostPort hostPort(String text) throws UsageException {
    int colon = text.lastIndexOf(':');
    if (colon <= 0 || colon == text.length() - 1) {
      throw new UsageException("not host:port: " + text);
    }
    String host = text.substring(0, colon);
    if (host.startsWith("[") && host.endsWith("]")) {
      host = host.substring(1, host.length() - 1);
    }
    try {
      int port = Integer.parseInt(text.substring(colon + 1));
      if (port < 0 || port > 65535) {
        throw new NumberFormatException();
      }
      return new HostPort(host, port);
    } catch (NumberFormatException e) {
      throw new UsageException("bad port in " + text);
    }
  }

  record HostPort(String host, int port) {}
}

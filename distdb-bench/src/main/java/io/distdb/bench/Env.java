package io.distdb.bench;

import java.lang.management.ManagementFactory;
import java.util.Locale;

/** A one-line description of this machine, printed with every benchmark result. */
final class Env {

  private Env() {}

  static String describe() {
    String cpu =
        System.getenv().getOrDefault("PROCESSOR_IDENTIFIER", System.getProperty("os.arch"));
    long memory = 0;
    if (ManagementFactory.getOperatingSystemMXBean()
        instanceof com.sun.management.OperatingSystemMXBean os) {
      memory = os.getTotalMemorySize();
    }
    return String.format(
        Locale.ROOT,
        "%s, %d hardware threads, %.1f GB RAM, %s %s, Java %s",
        cpu,
        Runtime.getRuntime().availableProcessors(),
        memory / 1e9,
        System.getProperty("os.name"),
        System.getProperty("os.version"),
        System.getProperty("java.version"));
  }
}

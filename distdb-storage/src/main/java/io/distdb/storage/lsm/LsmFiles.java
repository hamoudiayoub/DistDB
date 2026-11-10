package io.distdb.storage.lsm;

import java.nio.file.Path;
import java.util.OptionalLong;

/**
 * File names inside an engine directory. Every file except CURRENT carries a number from one
 * counter, so numbers order files by creation.
 *
 * <ul>
 *   <li>{@code 000007.log}: a write-ahead log
 *   <li>{@code 000012.sst}: an SSTable
 *   <li>{@code MANIFEST-000003}: the log of version edits
 *   <li>{@code CURRENT}: names the live manifest; replaced atomically via {@code CURRENT.tmp}
 * </ul>
 */
final class LsmFiles {

  static final String CURRENT = "CURRENT";
  static final String CURRENT_TMP = "CURRENT.tmp";
  static final String MANIFEST_PREFIX = "MANIFEST-";

  enum Kind {
    LOG,
    TABLE,
    MANIFEST,
    CURRENT,
    TEMP,
    OTHER
  }

  private LsmFiles() {}

  static Path log(Path dir, long number) {
    return dir.resolve("%06d.log".formatted(number));
  }

  static Path table(Path dir, long number) {
    return dir.resolve("%06d.sst".formatted(number));
  }

  static String manifestName(long number) {
    return MANIFEST_PREFIX + "%06d".formatted(number);
  }

  static Kind kind(String name) {
    if (name.equals(CURRENT)) {
      return Kind.CURRENT;
    }
    if (name.equals(CURRENT_TMP)) {
      return Kind.TEMP;
    }
    if (name.startsWith(MANIFEST_PREFIX)) {
      return number(name).isPresent() ? Kind.MANIFEST : Kind.OTHER;
    }
    if (name.endsWith(".log")) {
      return number(name).isPresent() ? Kind.LOG : Kind.OTHER;
    }
    if (name.endsWith(".sst")) {
      return number(name).isPresent() ? Kind.TABLE : Kind.OTHER;
    }
    return Kind.OTHER;
  }

  /** The number in a log, table or manifest file name. */
  static OptionalLong number(String name) {
    String digits;
    if (name.startsWith(MANIFEST_PREFIX)) {
      digits = name.substring(MANIFEST_PREFIX.length());
    } else if (name.endsWith(".log") || name.endsWith(".sst")) {
      digits = name.substring(0, name.length() - 4);
    } else {
      return OptionalLong.empty();
    }
    if (digits.isEmpty() || !digits.chars().allMatch(Character::isDigit)) {
      return OptionalLong.empty();
    }
    try {
      return OptionalLong.of(Long.parseLong(digits));
    } catch (NumberFormatException e) {
      return OptionalLong.empty();
    }
  }
}

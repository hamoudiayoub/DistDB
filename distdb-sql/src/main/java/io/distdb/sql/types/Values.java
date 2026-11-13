package io.distdb.sql.types;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Operations on SQL values, with PostgreSQL's semantics: comparison, arithmetic (with its overflow
 * errors and NUMERIC scale rules), casts, and the text form a client sees.
 *
 * <p>Arguments are never SQL NULL: callers handle NULL first (almost every operator is strict).
 * Binary operations take two values of the same type; the binder inserts the casts.
 */
public final class Values {

  /** PostgreSQL's NUMERIC_MIN_SIG_DIGITS: a quotient has at least this many significant digits. */
  private static final int MIN_SIG_DIGITS = 16;

  private static final int MAX_DISPLAY_SCALE = 1000;

  private static final Pattern FLOAT_SYNTAX =
      Pattern.compile("[+-]?(\\d+\\.?\\d*([eE][+-]?\\d+)?|\\.\\d+([eE][+-]?\\d+)?)");

  private static final Pattern NUMERIC_SYNTAX = FLOAT_SYNTAX;

  private Values() {}

  // -----------------------------------------------------------------------------------------------
  // Comparison

  /** Compares two non-null values of the same type, in SQL order. */
  public static int compare(Object a, Object b) {
    return switch (a) {
      case Long x -> Long.compare(x, (Long) b);
      case Double x -> compareDoubles(x, (Double) b);
      case BigDecimal x -> x.compareTo((BigDecimal) b);
      case String x -> compareStrings(x, (String) b);
      case Boolean x -> Boolean.compare(x, (Boolean) b);
      default -> throw new IllegalArgumentException("not a SQL value: " + a.getClass());
    };
  }

  /** PostgreSQL's float order: {@code -0 = 0}, and NaN equals itself and is above everything. */
  public static int compareDoubles(double a, double b) {
    if (Double.isNaN(a) || Double.isNaN(b)) {
      return Double.isNaN(a) ? (Double.isNaN(b) ? 0 : 1) : -1;
    }
    return a < b ? -1 : (a > b ? 1 : 0);
  }

  /** Text order of collation C: by code point, which is the byte order of UTF-8. */
  public static int compareStrings(String a, String b) {
    int i = 0;
    int j = 0;
    while (i < a.length() && j < b.length()) {
      int ca = a.codePointAt(i);
      int cb = b.codePointAt(j);
      if (ca != cb) {
        return Integer.compare(ca, cb);
      }
      i += Character.charCount(ca);
      j += Character.charCount(cb);
    }
    return Boolean.compare(i < a.length(), j < b.length());
  }

  /**
   * A value whose {@code equals} and {@code hashCode} agree with SQL equality, for hashing: {@code
   * 1.0} and {@code 1.00} are equal NUMERICs, {@code -0.0} and {@code 0.0} equal FLOATs.
   */
  public static Object hashKey(Object v) {
    return switch (v) {
      case null -> null;
      case BigDecimal d -> d.signum() == 0 ? BigDecimal.ZERO : d.stripTrailingZeros();
      case Double d -> d == 0.0 ? 0.0 : (Double.isNaN(d) ? Double.NaN : d);
      default -> v;
    };
  }

  // -----------------------------------------------------------------------------------------------
  // Arithmetic

  public static Object add(Object a, Object b) {
    return switch (a) {
      case Long x -> {
        try {
          yield Math.addExact(x, (Long) b);
        } catch (ArithmeticException e) {
          throw bigintOutOfRange();
        }
      }
      case Double x -> checkFloat(x + (Double) b, x, (Double) b);
      case BigDecimal x -> x.add((BigDecimal) b);
      default -> throw new IllegalArgumentException("+ on " + a.getClass());
    };
  }

  public static Object subtract(Object a, Object b) {
    return switch (a) {
      case Long x -> {
        try {
          yield Math.subtractExact(x, (Long) b);
        } catch (ArithmeticException e) {
          throw bigintOutOfRange();
        }
      }
      case Double x -> checkFloat(x - (Double) b, x, (Double) b);
      case BigDecimal x -> x.subtract((BigDecimal) b);
      default -> throw new IllegalArgumentException("- on " + a.getClass());
    };
  }

  public static Object multiply(Object a, Object b) {
    return switch (a) {
      case Long x -> {
        try {
          yield Math.multiplyExact(x, (Long) b);
        } catch (ArithmeticException e) {
          throw bigintOutOfRange();
        }
      }
      case Double x -> checkFloat(x * (Double) b, x, (Double) b);
      case BigDecimal x -> {
        BigDecimal p = x.multiply((BigDecimal) b);
        yield p.scale() > MAX_DISPLAY_SCALE
            ? p.setScale(MAX_DISPLAY_SCALE, RoundingMode.HALF_UP)
            : p;
      }
      default -> throw new IllegalArgumentException("* on " + a.getClass());
    };
  }

  /** Division: integers truncate toward zero; NUMERIC uses PostgreSQL's result scale. */
  public static Object divide(Object a, Object b) {
    return switch (a) {
      case Long x -> {
        long y = (Long) b;
        if (y == 0) {
          throw divisionByZero();
        }
        if (x == Long.MIN_VALUE && y == -1) {
          throw bigintOutOfRange();
        }
        yield x / y;
      }
      case Double x -> {
        double y = (Double) b;
        if (y == 0.0) {
          throw divisionByZero();
        }
        yield checkFloat(x / y, x, y);
      }
      case BigDecimal x -> divideNumeric(x, (BigDecimal) b);
      default -> throw new IllegalArgumentException("/ on " + a.getClass());
    };
  }

  /** Remainder, with the sign of the dividend. */
  public static Object modulo(Object a, Object b) {
    return switch (a) {
      case Long x -> {
        long y = (Long) b;
        if (y == 0) {
          throw divisionByZero();
        }
        yield y == -1 ? 0L : x % y;
      }
      case Double x -> {
        double y = (Double) b;
        if (y == 0.0) {
          throw divisionByZero();
        }
        yield x % y;
      }
      case BigDecimal x -> {
        BigDecimal y = (BigDecimal) b;
        if (y.signum() == 0) {
          throw divisionByZero();
        }
        yield x.remainder(y).setScale(Math.max(x.scale(), y.scale()), RoundingMode.UNNECESSARY);
      }
      default -> throw new IllegalArgumentException("% on " + a.getClass());
    };
  }

  public static Object negate(Object a) {
    return switch (a) {
      case Long x -> {
        if (x == Long.MIN_VALUE) {
          throw bigintOutOfRange();
        }
        yield -x;
      }
      case Double x -> -x;
      case BigDecimal x -> x.negate();
      default -> throw new IllegalArgumentException("- on " + a.getClass());
    };
  }

  /**
   * NUMERIC division as PostgreSQL does it ({@code select_div_scale}): enough fractional digits for
   * at least 16 significant digits in the quotient, and never fewer than either input's scale.
   * {@code 1 / 3.0} is {@code 0.33333333333333333333}; {@code 10 / 4.0} is {@code
   * 2.5000000000000000}.
   */
  public static BigDecimal divideNumeric(BigDecimal a, BigDecimal b) {
    if (b.signum() == 0) {
      throw divisionByZero();
    }
    int[] wa = weightAndFirstDigit(a);
    int[] wb = weightAndFirstDigit(b);
    int qweight = wa[0] - wb[0];
    if (wa[1] <= wb[1]) {
      qweight--;
    }
    int rscale = MIN_SIG_DIGITS - qweight * 4;
    rscale = Math.max(rscale, Math.max(a.scale(), b.scale()));
    rscale = Math.max(rscale, 0);
    rscale = Math.min(rscale, MAX_DISPLAY_SCALE);
    return a.divide(b, rscale, RoundingMode.HALF_UP);
  }

  /**
   * The weight (power of 10000) of the first non-zero base-10000 digit of {@code x}, and that
   * digit: PostgreSQL stores NUMERIC in base 10000 and estimates quotient sizes from these.
   */
  private static int[] weightAndFirstDigit(BigDecimal x) {
    if (x.signum() == 0) {
      return new int[] {0, 0};
    }
    BigDecimal abs = x.abs();
    int leading = abs.precision() - 1 - abs.scale(); // exponent of the leading decimal digit
    int weight = Math.floorDiv(leading, 4);
    int first = abs.movePointLeft(4 * weight).setScale(0, RoundingMode.FLOOR).intValueExact();
    return new int[] {weight, first};
  }

  public static double power(double a, double b) {
    if (a == 0.0 && b < 0) {
      throw new SqlException(
          "2201F", "zero raised to a negative power is undefined"); // invalid_argument_for_power
    }
    if (a < 0 && b != Math.rint(b)) {
      throw new SqlException(
          "2201F", "a negative number raised to a non-integer power yields a complex result");
    }
    return checkFloat(Math.pow(a, b), a, b);
  }

  private static double checkFloat(double result, double a, double b) {
    if (Double.isInfinite(result) && !Double.isInfinite(a) && !Double.isInfinite(b)) {
      throw new SqlException(SqlState.NUMERIC_VALUE_OUT_OF_RANGE, "value out of range: overflow");
    }
    return result;
  }

  // -----------------------------------------------------------------------------------------------
  // Casts

  /**
   * An explicit cast ({@code CAST(v AS t)} or {@code v::t}) of a non-null value. Integer casts
   * round: NUMERIC half away from zero, FLOAT half to even, as PostgreSQL does.
   */
  public static Object cast(Object v, Type from, TypeSpec to) {
    Object out = castType(v, from, to.type());
    return applyModifier(out, to, true);
  }

  /** Applies a column's modifier on assignment: VARCHAR(n) refuses longer text. */
  public static Object assign(Object v, Type from, TypeSpec to) {
    Object out = castType(v, from, to.type());
    return applyModifier(out, to, false);
  }

  private static Object applyModifier(Object v, TypeSpec to, boolean explicit) {
    if (to.length() >= 0 && v instanceof String s) {
      int length = s.codePointCount(0, s.length());
      if (length > to.length()) {
        if (explicit) {
          return s.substring(0, s.offsetByCodePoints(0, to.length()));
        }
        throw new SqlException(
            SqlState.STRING_DATA_RIGHT_TRUNCATION,
            "value too long for type character varying(" + to.length() + ")");
      }
    }
    if (to.precision() >= 0 && v instanceof BigDecimal d) {
      int scale = Math.max(to.scale(), 0);
      BigDecimal r = d.setScale(scale, RoundingMode.HALF_UP);
      if (r.precision() - r.scale() > to.precision() - scale && r.signum() != 0) {
        throw new SqlException(
            SqlState.NUMERIC_VALUE_OUT_OF_RANGE,
            "numeric field overflow: a field with precision "
                + to.precision()
                + ", scale "
                + scale
                + " must round to an absolute value less than 10^"
                + (to.precision() - scale));
      }
      return r;
    }
    return v;
  }

  private static Object castType(Object v, Type from, Type to) {
    if (from == to && from != Type.UNKNOWN) {
      return v;
    }
    if (from == Type.UNKNOWN || from == Type.TEXT) {
      return to == Type.TEXT || to == Type.UNKNOWN ? v : parse((String) v, to);
    }
    if (to == Type.TEXT || to == Type.UNKNOWN) {
      return toText(v, from);
    }
    return switch (to) {
      case INT -> toLong(v);
      case FLOAT ->
          switch (v) {
            case Long x -> (double) x;
            case BigDecimal x -> x.doubleValue();
            case Boolean x -> throw cannotCast(from, to);
            default -> v;
          };
      case NUMERIC ->
          switch (v) {
            case Long x -> BigDecimal.valueOf(x);
            case Double x -> floatToNumeric(x);
            case Boolean x -> throw cannotCast(from, to);
            default -> v;
          };
      case BOOL ->
          switch (v) {
            case Long x -> x != 0;
            default -> throw cannotCast(from, to);
          };
      default -> throw cannotCast(from, to);
    };
  }

  private static long toLong(Object v) {
    return switch (v) {
      case Long x -> x;
      case Boolean x -> x ? 1L : 0L;
      case Double x -> {
        if (Double.isNaN(x) || Double.isInfinite(x)) {
          throw bigintOutOfRange();
        }
        double r = Math.rint(x);
        if (r < -0x1p63 || r >= 0x1p63) {
          throw bigintOutOfRange();
        }
        yield (long) r;
      }
      case BigDecimal x -> {
        try {
          yield x.setScale(0, RoundingMode.HALF_UP).longValueExact();
        } catch (ArithmeticException e) {
          throw bigintOutOfRange();
        }
      }
      default -> throw new IllegalArgumentException(v.getClass().toString());
    };
  }

  /** As PostgreSQL: through the float's text at 15 significant digits ({@code DBL_DIG}). */
  private static BigDecimal floatToNumeric(double d) {
    if (Double.isNaN(d) || Double.isInfinite(d)) {
      throw new SqlException(
          SqlState.FEATURE_NOT_SUPPORTED, "cannot convert " + formatFloat(d) + " to numeric");
    }
    if (d == 0.0) {
      return BigDecimal.ZERO;
    }
    BigDecimal b = new BigDecimal(d).round(new MathContext(15, RoundingMode.HALF_EVEN));
    return normalizeScale(b.stripTrailingZeros());
  }

  /** A NUMERIC never has a negative scale: {@code 1e3} is {@code 1000}. */
  public static BigDecimal normalizeScale(BigDecimal d) {
    return d.scale() < 0 ? d.setScale(0, RoundingMode.UNNECESSARY) : d;
  }

  /** Parses the text form of a value of type {@code t}, as the type's input function does. */
  public static Object parse(String text, Type t) {
    String s = text.strip();
    try {
      return switch (t) {
        case INT -> {
          String digits = s.startsWith("+") ? s.substring(1) : s;
          if (digits.isEmpty() || !digits.chars().allMatch(c -> c == '-' || Character.isDigit(c))) {
            throw invalidInput(t, text);
          }
          try {
            yield Long.parseLong(digits);
          } catch (NumberFormatException e) {
            if (digits.matches("-?\\d+")) {
              throw new SqlException(
                  SqlState.NUMERIC_VALUE_OUT_OF_RANGE,
                  "value \"" + text + "\" is out of range for type bigint");
            }
            throw invalidInput(t, text);
          }
        }
        case FLOAT -> {
          String lower = s.toLowerCase(Locale.ROOT);
          yield switch (lower) {
            case "nan" -> Double.NaN;
            case "infinity", "+infinity", "inf", "+inf" -> Double.POSITIVE_INFINITY;
            case "-infinity", "-inf" -> Double.NEGATIVE_INFINITY;
            default -> {
              if (!FLOAT_SYNTAX.matcher(s).matches()) {
                throw invalidInput(t, text);
              }
              double d = Double.parseDouble(s);
              if (Double.isInfinite(d)) {
                throw new SqlException(
                    SqlState.NUMERIC_VALUE_OUT_OF_RANGE,
                    "\"" + text + "\" is out of range for type double precision");
              }
              yield d;
            }
          };
        }
        case NUMERIC -> {
          if (!NUMERIC_SYNTAX.matcher(s).matches()) {
            throw invalidInput(t, text);
          }
          yield normalizeScale(new BigDecimal(s));
        }
        case BOOL -> parseBool(s, text);
        case TEXT, UNKNOWN -> text;
      };
    } catch (NumberFormatException e) {
      throw invalidInput(t, text);
    }
  }

  private static boolean parseBool(String s, String original) {
    String v = s.toLowerCase(Locale.ROOT);
    if (v.equals("t")
        || v.equals("1")
        || v.equals("on")
        || prefixOf(v, "true", 1)
        || prefixOf(v, "yes", 1)) {
      return true;
    }
    if (v.equals("f")
        || v.equals("0")
        || prefixOf(v, "off", 2)
        || prefixOf(v, "false", 1)
        || prefixOf(v, "no", 1)) {
      return false;
    }
    throw invalidInput(Type.BOOL, original);
  }

  private static boolean prefixOf(String v, String word, int min) {
    return v.length() >= min && word.startsWith(v);
  }

  // -----------------------------------------------------------------------------------------------
  // Text output

  /** The text a client sees for a non-null value: PostgreSQL's output function for the type. */
  public static String toText(Object v, Type type) {
    return switch (v) {
      case Boolean b -> b ? "t" : "f";
      case Long l -> Long.toString(l);
      case Double d -> formatFloat(d);
      case BigDecimal d -> d.toPlainString();
      case String s -> s;
      default -> throw new IllegalArgumentException("not a SQL value: " + v.getClass());
    };
  }

  /**
   * A double as PostgreSQL prints it (since version 12): the shortest digits that read back as the
   * same double, in fixed notation when the decimal exponent is from -4 to 14, else as {@code
   * 1.5e+300}.
   */
  public static String formatFloat(double d) {
    if (Double.isNaN(d)) {
      return "NaN";
    }
    if (Double.isInfinite(d)) {
      return d > 0 ? "Infinity" : "-Infinity";
    }
    if (d == 0.0) {
      return (1 / d < 0) ? "-0" : "0";
    }
    // Double.toString gives the shortest repeatable digits (JDK 19 and later).
    BigDecimal exact = new BigDecimal(Double.toString(Math.abs(d))).stripTrailingZeros();
    String digits = exact.unscaledValue().toString();
    int exponent = digits.length() - 1 - exact.scale();
    StringBuilder out = new StringBuilder();
    if (d < 0) {
      out.append('-');
    }
    if (exponent < -4 || exponent >= 15) {
      out.append(digits.charAt(0));
      if (digits.length() > 1) {
        out.append('.').append(digits, 1, digits.length());
      }
      out.append('e').append(exponent < 0 ? '-' : '+');
      int e = Math.abs(exponent);
      if (e < 10) {
        out.append('0');
      }
      out.append(e);
    } else {
      out.append(exact.toPlainString());
    }
    return out.toString();
  }

  // -----------------------------------------------------------------------------------------------
  // Numeric functions

  /**
   * {@code round(numeric, n)}: half away from zero; a negative {@code n} rounds left of the point.
   */
  public static BigDecimal round(BigDecimal x, int places) {
    BigDecimal r = x.setScale(places, RoundingMode.HALF_UP);
    return places < 0 ? r.setScale(0, RoundingMode.UNNECESSARY) : r;
  }

  public static BigDecimal trunc(BigDecimal x, int places) {
    BigDecimal r = x.setScale(places, RoundingMode.DOWN);
    return places < 0 ? r.setScale(0, RoundingMode.UNNECESSARY) : r;
  }

  public static BigDecimal ceil(BigDecimal x) {
    return x.setScale(0, RoundingMode.CEILING);
  }

  public static BigDecimal floor(BigDecimal x) {
    return x.setScale(0, RoundingMode.FLOOR);
  }

  /** The exact integer value of a NUMERIC sum of longs (for overflow-free sums). */
  public static BigDecimal exact(long v) {
    return new BigDecimal(BigInteger.valueOf(v));
  }

  // -----------------------------------------------------------------------------------------------
  // Errors

  public static SqlException bigintOutOfRange() {
    return new SqlException(SqlState.NUMERIC_VALUE_OUT_OF_RANGE, "bigint out of range");
  }

  public static SqlException divisionByZero() {
    return new SqlException(SqlState.DIVISION_BY_ZERO, "division by zero");
  }

  private static SqlException invalidInput(Type t, String text) {
    return new SqlException(
        SqlState.INVALID_TEXT_REPRESENTATION,
        "invalid input syntax for type " + t.sqlName() + ": \"" + text + "\"");
  }

  private static SqlException cannotCast(Type from, Type to) {
    return new SqlException(
        SqlState.CANNOT_COERCE, "cannot cast type " + from.sqlName() + " to " + to.sqlName());
  }
}

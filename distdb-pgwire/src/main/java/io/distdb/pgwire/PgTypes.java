package io.distdb.pgwire;

import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.types.Type;
import io.distdb.sql.types.TypeSpec;
import io.distdb.sql.types.Values;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Values on the wire: PostgreSQL's text and binary formats for DistDB's types, and for the integer
 * and float widths a client may send parameters in (int2, int4, float4).
 */
final class PgTypes {

  static final int INT2 = 21;
  static final int INT4 = 23;
  static final int INT8 = 20;
  static final int FLOAT4 = 700;
  static final int FLOAT8 = 701;
  static final int BOOL = 16;
  static final int NUMERIC = 1700;
  static final int TEXT = 25;
  static final int VARCHAR = 1043;

  private PgTypes() {}

  /** A value of type {@code t} in the requested format (0 text, 1 binary). */
  static byte[] encode(Object v, Type t, int format) {
    if (format == 0) {
      return Values.toText(v, t).getBytes(StandardCharsets.UTF_8);
    }
    return switch (t) {
      case BOOL -> new byte[] {(byte) ((Boolean) v ? 1 : 0)};
      case INT -> ByteBuffer.allocate(8).putLong((Long) v).array();
      case FLOAT -> ByteBuffer.allocate(8).putDouble((Double) v).array();
      case NUMERIC -> numericBinary((BigDecimal) v);
      case TEXT, UNKNOWN -> ((String) v).getBytes(StandardCharsets.UTF_8);
    };
  }

  /**
   * A parameter sent as {@code bytes} in {@code format}, declared by the client as {@code oid} (0:
   * not declared), as a value of DistDB type {@code t}.
   */
  static Object decode(byte[] bytes, int format, int oid, Type t) {
    if (format == 0) {
      String text = new String(bytes, StandardCharsets.UTF_8);
      return Values.cast(text, Type.UNKNOWN, TypeSpec.of(t));
    }
    int wire = oid != 0 ? oid : t.oid();
    ByteBuffer b = ByteBuffer.wrap(bytes);
    Object v;
    Type vt;
    switch (wire) {
      case INT2 -> {
        v = (long) b.getShort();
        vt = Type.INT;
      }
      case INT4 -> {
        v = (long) b.getInt();
        vt = Type.INT;
      }
      case INT8 -> {
        v = b.getLong();
        vt = Type.INT;
      }
      case FLOAT4 -> {
        v = (double) b.getFloat();
        vt = Type.FLOAT;
      }
      case FLOAT8 -> {
        v = b.getDouble();
        vt = Type.FLOAT;
      }
      case BOOL -> {
        v = bytes[0] != 0;
        vt = Type.BOOL;
      }
      case NUMERIC -> {
        v = numericFromBinary(b);
        vt = Type.NUMERIC;
      }
      case TEXT, VARCHAR, 1042, 19, 705 -> {
        v = new String(bytes, StandardCharsets.UTF_8);
        vt = Type.TEXT;
      }
      default ->
          throw new SqlException(
              SqlState.FEATURE_NOT_SUPPORTED, "binary parameters of type " + wire);
    }
    return vt == t ? v : Values.cast(v, vt, TypeSpec.of(t));
  }

  /**
   * NUMERIC's binary form: base-10000 digits with a weight (the power of the first digit), a sign
   * and a display scale.
   */
  static byte[] numericBinary(BigDecimal d) {
    int dscale = Math.max(d.scale(), 0);
    BigDecimal abs = d.abs();
    // Digits around the decimal point, in groups of four.
    String plain = abs.setScale(dscale, RoundingMode.UNNECESSARY).toPlainString();
    int point = plain.indexOf('.');
    String whole = point < 0 ? plain : plain.substring(0, point);
    String frac = point < 0 ? "" : plain.substring(point + 1);
    while (whole.length() % 4 != 0) {
      whole = "0" + whole;
    }
    while (frac.length() % 4 != 0) {
      frac = frac + "0";
    }
    List<Short> digits = new ArrayList<>();
    for (int k = 0; k < whole.length(); k += 4) {
      digits.add(Short.parseShort(whole.substring(k, k + 4)));
    }
    int weight = digits.size() - 1;
    for (int k = 0; k < frac.length(); k += 4) {
      digits.add(Short.parseShort(frac.substring(k, k + 4)));
    }
    // Strip leading and trailing zero groups.
    int first = 0;
    while (first < digits.size() && digits.get(first) == 0) {
      first++;
      weight--;
    }
    int last = digits.size();
    while (last > first && digits.get(last - 1) == 0) {
      last--;
    }
    List<Short> kept = digits.subList(first, last);
    if (kept.isEmpty()) {
      weight = 0;
    }
    ByteBuffer out = ByteBuffer.allocate(8 + 2 * kept.size());
    out.putShort((short) kept.size());
    out.putShort((short) weight);
    out.putShort((short) (d.signum() < 0 ? 0x4000 : 0));
    out.putShort((short) dscale);
    for (short s : kept) {
      out.putShort(s);
    }
    return out.array();
  }

  static BigDecimal numericFromBinary(ByteBuffer b) {
    int n = b.getShort();
    int weight = b.getShort();
    int sign = b.getShort() & 0xFFFF;
    int dscale = b.getShort();
    if (sign == 0xC000) {
      throw new SqlException(SqlState.FEATURE_NOT_SUPPORTED, "NaN is not a supported NUMERIC");
    }
    BigInteger unscaled = BigInteger.ZERO;
    for (int k = 0; k < n; k++) {
      unscaled = unscaled.multiply(BigInteger.valueOf(10000)).add(BigInteger.valueOf(b.getShort()));
    }
    // value = unscaled * 10000^(weight - n + 1)
    BigDecimal v = new BigDecimal(unscaled).scaleByPowerOfTen(4 * (weight - n + 1));
    v = v.setScale(dscale, RoundingMode.HALF_UP);
    return sign == 0x4000 ? v.negate() : v;
  }
}

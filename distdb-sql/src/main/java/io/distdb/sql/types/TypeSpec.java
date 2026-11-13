package io.distdb.sql.types;

/**
 * A type as declared for a column or a cast, with its modifiers: {@code VARCHAR(n)} limits the
 * length, {@code NUMERIC(p, s)} fixes the scale and bounds the precision. A modifier of -1 means
 * none.
 */
public record TypeSpec(Type type, int length, int precision, int scale) {

  public static TypeSpec of(Type type) {
    return new TypeSpec(type, -1, -1, -1);
  }

  public static TypeSpec varchar(int length) {
    return new TypeSpec(Type.TEXT, length, -1, -1);
  }

  public static TypeSpec numeric(int precision, int scale) {
    return new TypeSpec(Type.NUMERIC, -1, precision, scale);
  }

  public boolean hasModifier() {
    return length >= 0 || precision >= 0;
  }

  /**
   * The name PostgreSQL would print, e.g. {@code character varying(20)} or {@code numeric(10,2)}.
   */
  public String sqlName() {
    if (length >= 0) {
      return "character varying(" + length + ")";
    }
    if (precision >= 0) {
      return "numeric(" + precision + "," + Math.max(scale, 0) + ")";
    }
    return type.sqlName();
  }
}

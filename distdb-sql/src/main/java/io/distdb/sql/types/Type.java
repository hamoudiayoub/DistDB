package io.distdb.sql.types;

/**
 * The SQL types DistDB has, each with the PostgreSQL type a client sees: its OID, name and size, as
 * reported in a RowDescription. Every integer type is 64-bit ({@code bigint}).
 *
 * <p>Values are plain Java objects: {@code Boolean}, {@code Long}, {@code Double}, {@code
 * BigDecimal}, {@code String}; SQL NULL is Java {@code null}.
 */
public enum Type {
  BOOL(16, "boolean", 1),
  INT(20, "bigint", 8),
  FLOAT(701, "double precision", 8),
  NUMERIC(1700, "numeric", -1),
  TEXT(25, "text", -1),
  /** A quoted literal or NULL whose type its context has not decided yet. */
  UNKNOWN(705, "unknown", -2);

  private final int oid;
  private final String sqlName;
  private final int size;

  Type(int oid, String sqlName, int size) {
    this.oid = oid;
    this.sqlName = sqlName;
    this.size = size;
  }

  /** PostgreSQL's type OID. */
  public int oid() {
    return oid;
  }

  /** The name PostgreSQL gives the type in messages, e.g. {@code double precision}. */
  public String sqlName() {
    return sqlName;
  }

  /** The type's size in bytes, or -1 for variable length, as in {@code pg_type.typlen}. */
  public int size() {
    return size;
  }

  public boolean isNumeric() {
    return this == INT || this == FLOAT || this == NUMERIC;
  }

  /** The type for an OID a client sent, or null when DistDB has no such type. */
  public static Type ofOid(int oid) {
    return switch (oid) {
      case 16 -> BOOL;
      case 20, 21, 23 -> INT; // int8, int2, int4
      case 700, 701 -> FLOAT; // float4, float8
      case 1700 -> NUMERIC;
      case 25, 1043, 1042, 19 -> TEXT; // text, varchar, bpchar, name
      case 0, 705 -> UNKNOWN;
      default -> null;
    };
  }
}

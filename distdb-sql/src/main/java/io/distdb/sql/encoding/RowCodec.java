package io.distdb.sql.encoding;

import io.distdb.common.encoding.KeyReader;
import io.distdb.common.encoding.KeyWriter;
import io.distdb.common.encoding.Order;
import io.distdb.common.encoding.ValueType;
import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.catalog.TableDesc.Index;
import io.distdb.sql.types.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

/**
 * Rows and index entries as keys and values, in Phase 1's order-preserving tuple encoding, so that
 * key order is SQL order:
 *
 * <pre>
 *   /{table}/1/{pk columns}                    → (column id, value) for the other columns
 *   /{table}/{index}/{columns}/{pk columns}    → (empty)              a secondary index entry
 *   /{table}/{index}/{columns}                 → {pk columns}         a unique index entry
 * </pre>
 *
 * A unique index entry with a NULL column is stored like a non-unique one: SQL allows many NULLs in
 * a unique index. NUMERIC sorts by value ({@code 1.0} and {@code 1.00} are one key); a NUMERIC
 * primary-key column is also kept in the row's value, so its scale survives.
 */
public final class RowCodec {

  private RowCodec() {}

  // -----------------------------------------------------------------------------------------------
  // Values inside keys

  /** Appends one SQL value of type {@code t}, in an order-preserving way. */
  public static void writeKey(KeyWriter w, Type t, Object v) {
    if (v == null) {
      w.writeNull();
      return;
    }
    switch (t) {
      case BOOL -> w.writeBoolean((Boolean) v);
      case INT -> w.writeLong((Long) v);
      case FLOAT -> {
        double d = (Double) v;
        w.writeDouble(d == 0.0 ? 0.0 : d); // -0.0 is 0.0 in SQL
      }
      case TEXT, UNKNOWN -> w.writeString((String) v);
      case NUMERIC -> writeDecimal(w, (BigDecimal) v);
    }
  }

  /**
   * A decimal as {@code 0.d1d2... × 10^e}: a class (negative, zero, positive), then the exponent
   * and the digits; for negatives both descending, so that a larger magnitude sorts first.
   */
  private static void writeDecimal(KeyWriter w, BigDecimal v) {
    int sign = v.signum();
    w.writeLong(sign + 1);
    if (sign == 0) {
      return;
    }
    BigDecimal s = v.abs().stripTrailingZeros();
    String digits = s.unscaledValue().toString();
    long exponent = (long) digits.length() - s.scale();
    Order order = sign > 0 ? Order.ASC : Order.DESC;
    w.writeLong(exponent, order);
    w.writeString(digits, order);
  }

  /** Reads a value written by {@link #writeKey}. */
  public static Object readKey(KeyReader r, Type t) {
    if (r.peekType() == ValueType.NULL) {
      r.readNull();
      return null;
    }
    return switch (t) {
      case BOOL -> r.readBoolean();
      case INT -> r.readLong();
      case FLOAT -> r.readDouble();
      case TEXT, UNKNOWN -> r.readString();
      case NUMERIC -> {
        long cls = r.readLong();
        if (cls == 1) {
          yield BigDecimal.ZERO;
        }
        long exponent = r.readLong();
        String digits = r.readString();
        BigDecimal d = new BigDecimal(new BigInteger(digits), (int) (digits.length() - exponent));
        d = d.scale() < 0 ? d.setScale(0) : d;
        yield cls == 0 ? d.negate() : d;
      }
    };
  }

  /** A value in a row: as in a key, except that a NUMERIC keeps its scale ({@code 7.00}). */
  private static void writeValue(KeyWriter w, Type t, Object v) {
    if (t == Type.NUMERIC && v != null) {
      w.writeString(((BigDecimal) v).toPlainString());
    } else {
      writeKey(w, t, v);
    }
  }

  private static Object readValue(KeyReader r, Type t) {
    if (t == Type.NUMERIC && r.peekType() == ValueType.STRING) {
      return new BigDecimal(r.readString());
    }
    return readKey(r, t);
  }

  private static void skipKey(KeyReader r, Type t) {
    readKey(r, t);
  }

  // -----------------------------------------------------------------------------------------------
  // Spans

  /** {@code /{table}}: every key of the table, data and indexes. */
  public static byte[] tablePrefix(long tableId) {
    return new KeyWriter().writeLong(tableId).toByteArray();
  }

  /** {@code /{table}/{index}}: every entry of one index (1: the rows). */
  public static byte[] indexPrefix(long tableId, int indexId) {
    return new KeyWriter().writeLong(tableId).writeLong(indexId).toByteArray();
  }

  /**
   * {@code /{table}/{index}/{values...}}: the start of the entries whose first columns equal {@code
   * values} (typed as the index's columns).
   */
  public static byte[] indexPrefix(TableDesc t, Index index, List<Object> values) {
    KeyWriter w = new KeyWriter().writeLong(t.id()).writeLong(index.id());
    int[] positions = t.positions(index);
    for (int k = 0; k < values.size(); k++) {
      writeKey(w, t.columns().get(positions[k]).type().type(), values.get(k));
    }
    return w.toByteArray();
  }

  // -----------------------------------------------------------------------------------------------
  // Rows

  public static byte[] primaryKey(TableDesc t, Object[] row) {
    KeyWriter w = new KeyWriter().writeLong(t.id()).writeLong(TableDesc.PRIMARY_INDEX_ID);
    writeColumns(w, t, t.primary(), row);
    return w.toByteArray();
  }

  private static void writeColumns(KeyWriter w, TableDesc t, Index index, Object[] row) {
    for (int p : t.positions(index)) {
      writeKey(w, t.columns().get(p).type().type(), row[p]);
    }
  }

  /** The row's value: {@code (column id, value)} for every non-NULL column not in the key. */
  public static byte[] rowValue(TableDesc t, Object[] row) {
    KeyWriter w = new KeyWriter();
    int[] pk = t.positions(t.primary());
    for (int p = 0; p < row.length; p++) {
      TableDesc.Column c = t.columns().get(p);
      if (row[p] == null || (contains(pk, p) && c.type().type() != Type.NUMERIC)) {
        continue;
      }
      w.writeLong(c.id());
      writeValue(w, c.type().type(), row[p]);
    }
    return w.toByteArray();
  }

  /** The row stored at {@code key} with {@code value}. */
  public static Object[] decodeRow(TableDesc t, byte[] key, byte[] value) {
    Object[] row = new Object[t.columns().size()];
    KeyReader k = new KeyReader(key);
    k.readLong();
    k.readLong();
    for (int p : t.positions(t.primary())) {
      row[p] = readKey(k, t.columns().get(p).type().type());
    }
    KeyReader v = new KeyReader(value);
    while (v.hasRemaining()) {
      int id = (int) v.readLong();
      int p = positionOrMinus(t, id);
      if (p < 0) {
        v.skip(); // a dropped column (not possible yet: there is no ALTER TABLE)
      } else {
        row[p] = readValue(v, t.columns().get(p).type().type());
      }
    }
    return row;
  }

  private static int positionOrMinus(TableDesc t, int columnId) {
    for (int p = 0; p < t.columns().size(); p++) {
      if (t.columns().get(p).id() == columnId) {
        return p;
      }
    }
    return -1;
  }

  // -----------------------------------------------------------------------------------------------
  // Secondary indexes

  /** Whether {@code row}'s entry in {@code index} is a unique one (no NULL among its columns). */
  public static boolean uniqueEntry(TableDesc t, Index index, Object[] row) {
    if (!index.unique()) {
      return false;
    }
    for (int p : t.positions(index)) {
      if (row[p] == null) {
        return false;
      }
    }
    return true;
  }

  public static byte[] indexKey(TableDesc t, Index index, Object[] row) {
    KeyWriter w = new KeyWriter().writeLong(t.id()).writeLong(index.id());
    writeColumns(w, t, index, row);
    if (!uniqueEntry(t, index, row)) {
      writeColumns(w, t, t.primary(), row);
    }
    return w.toByteArray();
  }

  public static byte[] indexValue(TableDesc t, Index index, Object[] row) {
    if (!uniqueEntry(t, index, row)) {
      return new byte[0];
    }
    KeyWriter w = new KeyWriter();
    writeColumns(w, t, t.primary(), row);
    return w.toByteArray();
  }

  /** The primary key of the row an index entry points to. */
  public static byte[] primaryKeyOf(TableDesc t, Index index, byte[] key, byte[] value) {
    byte[] prefix = indexPrefix(t.id(), TableDesc.PRIMARY_INDEX_ID);
    byte[] suffix;
    if (value.length > 0) {
      suffix = value;
    } else {
      KeyReader r = new KeyReader(key);
      r.readLong();
      r.readLong();
      for (int p : t.positions(index)) {
        skipKey(r, t.columns().get(p).type().type());
      }
      int consumed = key.length - r.remaining();
      suffix = Arrays.copyOfRange(key, consumed, key.length);
    }
    byte[] out = Arrays.copyOf(prefix, prefix.length + suffix.length);
    System.arraycopy(suffix, 0, out, prefix.length, suffix.length);
    return out;
  }

  /** Raised when an index entry points to no row: the index and the table disagree. */
  public static SqlException corrupt(TableDesc t, Index index) {
    return new SqlException(
        SqlState.DATA_CORRUPTED,
        "index \"" + index.name() + "\" of table \"" + t.name() + "\" has an entry with no row");
  }

  private static boolean contains(int[] a, int v) {
    for (int x : a) {
      if (x == v) {
        return true;
      }
    }
    return false;
  }
}

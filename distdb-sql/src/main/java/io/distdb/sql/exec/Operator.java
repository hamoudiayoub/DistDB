package io.distdb.sql.exec;

/**
 * A Volcano iterator: {@link #open()}, then {@link #next()} until it returns null, then {@link
 * #close()}. Each row is an {@code Object[]} laid out as its plan's columns.
 */
public interface Operator {

  void open();

  /** The next row, or null when there are no more. */
  Object[] next();

  void close();
}

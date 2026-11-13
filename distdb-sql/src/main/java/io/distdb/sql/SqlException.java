package io.distdb.sql;

import java.util.Objects;

/**
 * A SQL error, with PostgreSQL's five-character SQLSTATE code ({@link SqlState}), so that drivers
 * and clients can react to it as they would to PostgreSQL's: retry on {@code 40001}, report {@code
 * 23505} as a duplicate key, and so on.
 */
public final class SqlException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final String state;
  private final boolean nothingWritten;

  public SqlException(String state, String message) {
    this(state, message, null, false);
  }

  public SqlException(String state, String message, Throwable cause) {
    this(state, message, cause, false);
  }

  /**
   * @param nothingWritten the transaction certainly wrote nothing (it timed out before its commit
   *     point), so running it again is safe even though this is not a serialization failure
   */
  public SqlException(String state, String message, Throwable cause, boolean nothingWritten) {
    super(message, cause);
    this.state = Objects.requireNonNull(state);
    this.nothingWritten = nothingWritten;
  }

  /** The SQLSTATE code, e.g. {@code "23505"}. */
  public String state() {
    return state;
  }

  /**
   * Whether running the whole transaction again may succeed and is safe: a serialization failure,
   * or a timeout before anything was written.
   */
  public boolean retryable() {
    return state.equals(SqlState.SERIALIZATION_FAILURE) || nothingWritten;
  }

  @Override
  public String toString() {
    return "SqlException[" + state + "]: " + getMessage();
  }
}

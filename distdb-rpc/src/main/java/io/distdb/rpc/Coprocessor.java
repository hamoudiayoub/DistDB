package io.distdb.rpc;

/**
 * A program a range runs over its part of a span, next to the data: the pushdown of a query
 * fragment (filter, projection, partial aggregation, limit). The storage layer supplies the entries
 * visible at the request's snapshot; the program decides what goes back. It lives here, below both
 * the storage layer that runs it and the SQL layer that implements it, so that storage never
 * depends on SQL.
 */
public interface Coprocessor {

  /** One run over one range's part of the span. Not thread-safe. */
  interface Run {
    /**
     * Takes one visible entry (in key order).
     *
     * @return false to stop after this entry: the response then resumes at the next key
     */
    boolean accept(byte[] key, byte[] value);

    /** The result to send back for the entries accepted. */
    byte[] finish();

    /** Whether the program needs no more entries from this span (a limit reached): no resume. */
    default boolean satisfied() {
      return false;
    }
  }

  /**
   * Starts a run of {@code program}.
   *
   * @throws IllegalArgumentException if the program cannot be decoded
   */
  Run start(byte[] program);
}

package io.distdb.txn;

/** Why a transaction did not commit, or did not finish. */
public final class TxnException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** What happened, and what the caller can do about it. */
  public enum Code {
    /** Another transaction wrote a key this one wrote, or holds a lock on it. Retry. */
    CONFLICT,
    /** It was rolled back (a reader found it dead) before its commit point. Retry. */
    ABORTED,
    /** It is older than the GC lifetime allows. Retry with a new transaction. */
    TOO_OLD,
    /**
     * Its commit point may or may not have been reached: the primary's commit got no answer. Either
     * all of its writes happen or none do; read to find out.
     */
    UNKNOWN,
    /** A test made the client stop on purpose, as if it had died; see {@link TxnClient.Crash}. */
    ABANDONED,
    /** Nothing answered in time; no write happened. */
    FAILED
  }

  private final Code code;

  public TxnException(Code code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public Code code() {
    return code;
  }

  /** Whether running the transaction again may succeed. */
  public boolean retryable() {
    return code == Code.CONFLICT || code == Code.ABORTED || code == Code.TOO_OLD;
  }
}

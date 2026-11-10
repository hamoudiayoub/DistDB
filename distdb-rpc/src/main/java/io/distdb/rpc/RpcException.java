package io.distdb.rpc;

/**
 * A call that got no response from the server.
 *
 * <p>Usually the client cannot tell whether the request arrived: the failure may have happened
 * before it was sent or after the reply was lost. {@link #maybeDelivered()} is {@code false} only
 * when the channel knows for certain that nothing was sent, such as when the connection was
 * refused.
 */
public final class RpcException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** Why no response arrived. */
  public enum Kind {
    /** No answer before the call's timeout. Worth retrying while the overall deadline allows. */
    TIMEOUT(true),
    /** The server could not be reached, or the connection broke. Worth retrying. */
    UNAVAILABLE(true),
    /** Anything else: the server failed on the request, or the call was malformed. */
    FAILED(false);

    private final boolean retryable;

    Kind(boolean retryable) {
      this.retryable = retryable;
    }

    /** Whether trying again can succeed. */
    public boolean retryable() {
      return retryable;
    }
  }

  private final Kind kind;
  private final boolean maybeDelivered;

  public RpcException(Kind kind, boolean maybeDelivered, String message, Throwable cause) {
    super(kind + ": " + message, cause);
    this.kind = kind;
    this.maybeDelivered = maybeDelivered;
  }

  /** A failure after which the request may have been delivered. */
  public RpcException(Kind kind, String message) {
    this(kind, true, message, null);
  }

  public Kind kind() {
    return kind;
  }

  /** Whether the server may have received, and acted on, the request. */
  public boolean maybeDelivered() {
    return maybeDelivered;
  }
}

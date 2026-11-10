package io.distdb.client;

/**
 * A KV call that did not succeed.
 *
 * <p>For writes, check {@link #outcomeUnknown()}. If it is {@code true}, at least one attempt may
 * have reached the disk (it went unanswered after it may have been sent, or the server failed while
 * writing it), so the write <em>may</em> have been applied; reading the key is the only way to
 * know. If it is {@code false}, every attempt was refused by the server or never sent, and the
 * write was certainly not applied.
 */
public final class KvException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** Why the call failed. */
  public enum Code {
    /** The call's deadline passed before any attempt succeeded. */
    DEADLINE_EXCEEDED,
    /** The request is wrong and retrying cannot help (for example, a key that is too long). */
    INVALID_ARGUMENT,
    /** Something unexpected: a server bug, or a protocol mismatch. */
    INTERNAL
  }

  private final Code code;
  private final boolean outcomeUnknown;

  KvException(Code code, boolean outcomeUnknown, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
    this.outcomeUnknown = outcomeUnknown;
  }

  public Code code() {
    return code;
  }

  /** Whether a write may have been applied despite the failure. Always false for reads. */
  public boolean outcomeUnknown() {
    return outcomeUnknown;
  }
}

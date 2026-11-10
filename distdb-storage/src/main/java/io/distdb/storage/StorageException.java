package io.distdb.storage;

/** A storage engine failure: an I/O error, corrupt data, or use of a failed or closed engine. */
public class StorageException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public StorageException(String message) {
    super(message);
  }

  public StorageException(String message, Throwable cause) {
    super(message, cause);
  }
}

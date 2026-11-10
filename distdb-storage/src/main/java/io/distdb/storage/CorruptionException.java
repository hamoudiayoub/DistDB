package io.distdb.storage;

/**
 * Data on disk failed a checksum or could not be parsed. Unlike a torn tail of an unsynced log
 * (which recovery expects and discards), this means bytes the engine relied on are wrong.
 */
public final class CorruptionException extends StorageException {

  private static final long serialVersionUID = 1L;

  public CorruptionException(String message) {
    super(message);
  }
}

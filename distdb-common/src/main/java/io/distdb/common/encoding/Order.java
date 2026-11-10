package io.distdb.common.encoding;

/** The direction one key component sorts in. */
public enum Order {
  /** Smallest value first; {@code NULL} sorts before every other value. */
  ASC,
  /**
   * Largest value first; {@code NULL} sorts after every other value. A descending component is the
   * ascending encoding with every byte inverted.
   */
  DESC
}

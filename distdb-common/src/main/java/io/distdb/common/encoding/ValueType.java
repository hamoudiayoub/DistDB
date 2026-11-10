package io.distdb.common.encoding;

/**
 * The types a key component can hold. When components of different types meet at the same position
 * (which a well-typed schema never produces) they sort in declaration order.
 */
public enum ValueType {
  NULL,
  BOOLEAN,
  LONG,
  DOUBLE,
  BYTES,
  STRING
}

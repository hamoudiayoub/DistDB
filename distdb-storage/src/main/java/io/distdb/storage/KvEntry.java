package io.distdb.storage;

/**
 * One key and its value. The arrays are owned by the caller once returned; the engine never
 * modifies them.
 */
public record KvEntry(byte[] key, byte[] value) {}

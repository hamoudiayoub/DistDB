package io.distdb.client;

/** One key and its value, as returned by a scan. The arrays belong to the caller. */
public record Entry(byte[] key, byte[] value) {}

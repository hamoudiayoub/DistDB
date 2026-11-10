package io.distdb.storage.lsm;

/**
 * The outcome of looking a key up in one source (memtable or SSTable) when that source has an entry
 * for it: either a value, or a tombstone meaning "deleted; stop looking". A source with no entry
 * returns {@code null} instead, and the search moves on to older sources.
 */
record Hit(byte[] value) {

  static final Hit DELETED = new Hit(null);

  boolean deleted() {
    return value == null;
  }
}

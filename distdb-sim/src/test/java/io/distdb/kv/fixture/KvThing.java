package io.distdb.kv.fixture;

/** A stand-in kv class that the storage fixture illegally depends on. */
public final class KvThing {
  private KvThing() {}

  public static int value() {
    return 1;
  }
}

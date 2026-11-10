package io.distdb.storage.fixture;

import io.distdb.kv.fixture.KvThing;

/** A deliberate upward dependency (storage -> kv), used only by ArchitectureRulesTest. */
public final class StorageReachingIntoKvFixture {
  private StorageReachingIntoKvFixture() {}

  static int peek() {
    return KvThing.value();
  }
}

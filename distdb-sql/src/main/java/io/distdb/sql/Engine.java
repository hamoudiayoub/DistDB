package io.distdb.sql;

import io.distdb.sql.catalog.Catalog;
import io.distdb.sql.kv.KvStore;
import java.util.Objects;

/**
 * The SQL layer of one node: sessions over a transactional store, sharing a catalog cache. Any
 * number of nodes may run one over the same cluster; each sees every other's tables, since the
 * catalog is in the store.
 */
public final class Engine {

  private final KvStore store;
  private final Catalog catalog = new Catalog();
  private final io.distdb.env.Clock clock;

  public Engine(KvStore store) {
    this(store, io.distdb.env.SystemClock.INSTANCE);
  }

  /** {@code clock} times EXPLAIN ANALYZE (the simulated clock, in a simulation). */
  public Engine(KvStore store, io.distdb.env.Clock clock) {
    this.store = Objects.requireNonNull(store);
    this.clock = Objects.requireNonNull(clock);
  }

  io.distdb.env.Clock clock() {
    return clock;
  }

  /** A new session (a client connection's state). Sessions are not thread-safe; the engine is. */
  public Session session() {
    return new Session(this);
  }

  KvStore store() {
    return store;
  }

  Catalog catalog() {
    return catalog;
  }
}

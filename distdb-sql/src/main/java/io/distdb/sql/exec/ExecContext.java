package io.distdb.sql.exec;

import io.distdb.sql.kv.KvStore;
import io.distdb.sql.kv.KvTxn;
import io.distdb.sql.plan.Scalar;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * What a running statement evaluates against: its transaction, its parameters, and, inside a
 * correlated subquery, the rows of the enclosing queries ({@link #withOuter}). Uncorrelated
 * subqueries' results are cached for the whole statement.
 */
public final class ExecContext {

  final KvTxn txn;
  final KvStore store;
  final Object[] params;
  private final ExecContext outerContext;
  private final Object[] outerRow;
  final Map<Scalar.Subquery, Object> cache;
  private final Map<io.distdb.sql.plan.Plan, RemoteStats> remote;
  private final Map<io.distdb.sql.plan.Plan, OpStats> ops;
  private final io.distdb.env.Clock clock;

  /** What a remote scan did: requests, bytes received, rows read on the ranges and returned. */
  public static final class RemoteStats {
    public long requests;
    public long bytes;
    public long rowsScanned;
    public long rowsReturned;
    public boolean local;
    public final java.util.Set<Integer> nodes = new java.util.TreeSet<>();
  }

  /** What an operator did, for EXPLAIN ANALYZE: rows produced, and time spent in it. */
  public static final class OpStats {
    public long rows;
    public long nanos;
    public long opens;
  }

  public ExecContext(KvTxn txn, KvStore store, Object[] params) {
    this(
        txn,
        store,
        params,
        null,
        null,
        new IdentityHashMap<>(),
        new IdentityHashMap<>(),
        null,
        null);
  }

  /** A context that records what each operator does (EXPLAIN ANALYZE), timed with {@code clock}. */
  public static ExecContext analyzing(
      KvTxn txn, KvStore store, Object[] params, io.distdb.env.Clock clock) {
    return new ExecContext(
        txn,
        store,
        params,
        null,
        null,
        new IdentityHashMap<>(),
        new IdentityHashMap<>(),
        new IdentityHashMap<>(),
        clock);
  }

  private ExecContext(
      KvTxn txn,
      KvStore store,
      Object[] params,
      ExecContext outerContext,
      Object[] outerRow,
      Map<Scalar.Subquery, Object> cache,
      Map<io.distdb.sql.plan.Plan, RemoteStats> remote,
      Map<io.distdb.sql.plan.Plan, OpStats> ops,
      io.distdb.env.Clock clock) {
    this.txn = txn;
    this.store = store;
    this.params = params;
    this.outerContext = outerContext;
    this.outerRow = outerRow;
    this.cache = cache;
    this.remote = remote;
    this.ops = ops;
    this.clock = clock;
  }

  /** The context of a subquery evaluated for {@code row} of this one. */
  ExecContext withOuter(Object[] row) {
    return new ExecContext(txn, store, params, this, row, cache, remote, ops, clock);
  }

  /** The value of an outer reference: its expression on the row {@code depth} levels out. */
  Object outer(Scalar.OuterRef ref) {
    ExecContext c = this;
    for (int k = 1; k < ref.depth(); k++) {
      c = c.outerContext;
    }
    if (c.outerRow == null) {
      throw new IllegalStateException("an outer reference with no outer row");
    }
    return Eval.eval(ref.inner(), c.outerRow, c.outerContext);
  }

  RemoteStats remoteStats(io.distdb.sql.plan.Plan p) {
    return remote.computeIfAbsent(p, x -> new RemoteStats());
  }

  /** The remote scans' statistics, by plan node. */
  public Map<io.distdb.sql.plan.Plan, RemoteStats> remoteStats() {
    return remote;
  }

  /** The operators' statistics (EXPLAIN ANALYZE), or null when not analyzing. */
  public Map<io.distdb.sql.plan.Plan, OpStats> opStats() {
    return ops;
  }

  io.distdb.env.Clock clock() {
    return clock;
  }

  private boolean pushdown = true;

  /** Whether statements push work to the ranges (the session's {@code distdb_pushdown}). */
  public ExecContext pushdown(boolean on) {
    this.pushdown = on;
    return this;
  }

  public boolean pushdown() {
    return pushdown;
  }

  public KvTxn txn() {
    return txn;
  }
}

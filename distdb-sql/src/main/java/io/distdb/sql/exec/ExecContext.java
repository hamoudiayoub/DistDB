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

  public ExecContext(KvTxn txn, KvStore store, Object[] params) {
    this(txn, store, params, null, null, new IdentityHashMap<>());
  }

  private ExecContext(
      KvTxn txn,
      KvStore store,
      Object[] params,
      ExecContext outerContext,
      Object[] outerRow,
      Map<Scalar.Subquery, Object> cache) {
    this.txn = txn;
    this.store = store;
    this.params = params;
    this.outerContext = outerContext;
    this.outerRow = outerRow;
    this.cache = cache;
  }

  /** The context of a subquery evaluated for {@code row} of this one. */
  ExecContext withOuter(Object[] row) {
    return new ExecContext(txn, store, params, this, row, cache);
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

  public KvTxn txn() {
    return txn;
  }
}

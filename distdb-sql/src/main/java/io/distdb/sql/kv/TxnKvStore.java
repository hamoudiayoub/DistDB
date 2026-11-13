package io.distdb.sql.kv;

import io.distdb.common.encoding.KeyReader;
import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.txn.Txn;
import io.distdb.txn.TxnClient;
import io.distdb.txn.TxnException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The SQL layer's store in a cluster: Phase 7's Percolator transactions, waited on through an
 * {@link Awaiter}. Transaction failures become SQLSTATEs: a conflict, an abort or a fence is {@code
 * 40001} (retry the transaction), an unknown outcome {@code 40003}.
 */
public final class TxnKvStore implements KvStore {

  private final TxnClient client;
  private final Awaiter awaiter;

  public TxnKvStore(TxnClient client, Awaiter awaiter) {
    this.client = Objects.requireNonNull(client);
    this.awaiter = Objects.requireNonNull(awaiter);
  }

  @Override
  public KvTxn begin() {
    return new TxnAdapter(await(client.begin()));
  }

  @Override
  public long[] timestamps(int n) {
    List<CompletableFuture<Long>> all = new ArrayList<>(n);
    for (int k = 0; k < n; k++) {
      all.add(client.timestamp());
    }
    long[] out = new long[n];
    for (int k = 0; k < n; k++) {
      out[k] = await(all.get(k));
    }
    java.util.Arrays.sort(out);
    return out;
  }

  @Override
  public void fence(long ts) {
    await(client.fence(ts));
  }

  @Override
  public int rangeCount(byte[] start, byte[] end) {
    return client.rangeSplits(start, end).size() + 1;
  }

  @Override
  public ClusterView cluster() {
    io.distdb.rpc.proto.ListRangesResponse list = await(client.listRanges());
    java.util.List<RangeRow> ranges = new java.util.ArrayList<>();
    for (io.distdb.rpc.proto.RangeInfo r : list.getRangesList()) {
      io.distdb.rpc.proto.RangeDescriptor d = r.getDesc();
      ranges.add(
          new RangeRow(
              d.getRangeId(),
              d.getStartKey().toByteArray(),
              d.hasEndKey() ? d.getEndKey().toByteArray() : null,
              r.getLeader(),
              java.util.List.copyOf(d.getVotersList()),
              java.util.List.copyOf(d.getLearnersList())));
    }
    java.util.List<NodeRow> nodes = new java.util.ArrayList<>();
    list.getStoresMap()
        .forEach(
            (id, address) -> nodes.add(new NodeRow(id, address, list.getAliveOrDefault(id, true))));
    return new ClusterView(ranges, nodes);
  }

  private <T> T await(CompletableFuture<T> f) {
    try {
      return awaiter.await(f);
    } catch (TxnException e) {
      throw translate(e);
    } catch (SqlException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new SqlException(
          SqlState.SYSTEM_ERROR, "the cluster did not answer: " + e.getMessage(), e);
    }
  }

  static SqlException translate(TxnException e) {
    return switch (e.code()) {
      case CONFLICT ->
          new SqlException(
              SqlState.SERIALIZATION_FAILURE,
              "could not serialize access due to concurrent update (" + e.getMessage() + ")",
              e);
      case ABORTED ->
          new SqlException(
              SqlState.SERIALIZATION_FAILURE,
              "the transaction was aborted by another one (" + e.getMessage() + ")",
              e);
      case TOO_OLD ->
          new SqlException(
              SqlState.SERIALIZATION_FAILURE,
              "the transaction is too old: a schema change or GC passed it ("
                  + e.getMessage()
                  + ")",
              e);
      case UNKNOWN ->
          new SqlException(
              SqlState.STATEMENT_COMPLETION_UNKNOWN,
              "the commit may or may not have happened (" + e.getMessage() + ")",
              e);
      // FAILED: nothing answered in time, and nothing was written; a retry is safe.
      case FAILED ->
          new SqlException(
              SqlState.SYSTEM_ERROR,
              "the cluster did not answer in time: " + e.getMessage(),
              e,
              true);
      case ABANDONED ->
          new SqlException(SqlState.SYSTEM_ERROR, "the transaction failed: " + e.getMessage(), e);
    };
  }

  /** {@code e}, saying which read failed. */
  private static SqlException where(SqlException e, String what) {
    return new SqlException(
        e.state(), e.getMessage() + " (" + what + ")", e.getCause(), e.retryable());
  }

  private final class TxnAdapter implements KvTxn {
    private final Txn txn;
    private boolean finished;

    TxnAdapter(Txn txn) {
      this.txn = txn;
    }

    @Override
    public long startTs() {
      return txn.startTs();
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
      try {
        return await(txn.get(key));
      } catch (SqlException e) {
        throw where(e, "get " + KeyReader.toDebugString(key));
      }
    }

    @Override
    public List<Entry> scan(byte[] start, byte[] end, int limit) {
      // Here 0 means no limit; Txn.scan wants a count.
      List<Txn.Entry> entries;
      try {
        entries = await(txn.scan(start, end, limit <= 0 ? Integer.MAX_VALUE : limit));
      } catch (SqlException e) {
        throw where(e, "scan from " + KeyReader.toDebugString(start));
      }
      List<Entry> out = new ArrayList<>(entries.size());
      for (Txn.Entry e : entries) {
        out.add(new Entry(e.key(), e.value()));
      }
      return out;
    }

    @Override
    public boolean hasWritesIn(byte[] start, byte[] end) {
      return txn.hasWritesIn(start, end);
    }

    @Override
    public List<CopResult> coprocessor(byte[] start, byte[] end, byte[] program) {
      List<io.distdb.rpc.proto.CoprocessorResponse> responses;
      try {
        responses = await(txn.coprocessor(start, end, program));
      } catch (SqlException e) {
        throw where(e, "coprocessor from " + KeyReader.toDebugString(start));
      }
      List<CopResult> out = new ArrayList<>(responses.size());
      for (io.distdb.rpc.proto.CoprocessorResponse r : responses) {
        out.add(
            new CopResult(
                r.getResult().toByteArray(),
                r.getRowsScanned(),
                r.getBytesScanned(),
                r.getServedBy()));
      }
      return out;
    }

    @Override
    public void put(byte[] key, byte[] value) {
      txn.put(key, value);
    }

    @Override
    public void delete(byte[] key) {
      txn.delete(key);
    }

    @Override
    public long commit() {
      finished = true;
      return await(txn.commit());
    }

    @Override
    public void rollback() {
      if (!finished) {
        finished = true;
        await(txn.rollback());
      }
    }
  }
}

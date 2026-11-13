package io.distdb.sim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.txn.Txn;
import io.distdb.txn.TxnClient;
import io.distdb.txn.TxnException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Snapshot isolation, anomaly by anomaly, on a real (simulated, fault-free) cluster: the ones it
 * forbids (dirty read, lost update, read skew, a stale snapshot) and the one it allows (write
 * skew). Each test interleaves transactions by hand.
 */
class TxnAnomalyTest {

  private final ShardedKvSimulation.Cluster cluster = ShardedKvSimulation.Cluster.start(1);
  private final TxnClient client = cluster.txnClient(t -> TxnClient.CrashPoint.NONE);

  @Test
  void dirtyReadsAreImpossible() {
    set("x", "old");
    // T1 prewrites x, and its client dies: a lock with an uncommitted value stays on x.
    TxnClient dying = cluster.txnClient(t -> TxnClient.CrashPoint.AFTER_PREWRITE);
    Txn t1 = cluster.await(dying.begin());
    t1.put(utf8("x"), utf8("dirty"));
    assertThatThrownBy(() -> cluster.await(t1.commit()))
        .isInstanceOfSatisfying(
            TxnException.class, e -> assertThat(e.code()).isEqualTo(TxnException.Code.ABANDONED));
    // T2 waits out the lock's TTL, rolls T1 back, and reads the committed value.
    Txn t2 = cluster.await(client.begin());
    assertThat(read(t2, "x")).isEqualTo("old");
  }

  @Test
  void lostUpdatesAreImpossible() {
    set("x", "0");
    Txn t1 = cluster.await(client.begin());
    Txn t2 = cluster.await(client.begin());
    assertThat(read(t1, "x")).isEqualTo("0");
    assertThat(read(t2, "x")).isEqualTo("0");
    t1.put(utf8("x"), utf8("t1"));
    t2.put(utf8("x"), utf8("t2"));
    cluster.await(t1.commit());
    assertThatThrownBy(() -> cluster.await(t2.commit()))
        .as("first committer wins")
        .isInstanceOfSatisfying(
            TxnException.class, e -> assertThat(e.code()).isEqualTo(TxnException.Code.CONFLICT));
    assertThat(get("x")).isEqualTo("t1");
  }

  @Test
  void readSkewIsImpossible() {
    set("x", "50");
    set("y", "50");
    Txn t1 = cluster.await(client.begin());
    assertThat(read(t1, "x")).isEqualTo("50");
    // T2 moves 10 from x to y and commits while T1 is between its reads.
    Txn t2 = cluster.await(client.begin());
    t2.put(utf8("x"), utf8("40"));
    t2.put(utf8("y"), utf8("60"));
    cluster.await(t2.commit());
    assertThat(read(t1, "y")).as("T1 still reads its own snapshot").isEqualTo("50");
  }

  @Test
  void writeSkewIsAllowed() {
    // The invariant x + y >= 1 holds, and each transaction keeps it on its own snapshot.
    set("x", "1");
    set("y", "1");
    Txn t1 = cluster.await(client.begin());
    Txn t2 = cluster.await(client.begin());
    assertThat(read(t1, "x") + read(t1, "y")).isEqualTo("11");
    assertThat(read(t2, "x") + read(t2, "y")).isEqualTo("11");
    t1.put(utf8("x"), utf8("0"));
    t2.put(utf8("y"), utf8("0"));
    cluster.await(t1.commit());
    cluster.await(t2.commit()); // they wrote different keys: snapshot isolation lets both commit
    assertThat(get("x") + get("y")).as("x + y is now 0: not serializable").isEqualTo("00");
  }

  @Test
  void aTransactionSeesEverythingThatCommittedBeforeItBegan() {
    for (int i = 0; i < 20; i++) {
      set("x", "v" + i);
      assertThat(get("x")).isEqualTo("v" + i);
    }
  }

  @Test
  void aReaderFinishesATransactionWhoseClientDiedAfterItsCommitPoint() {
    TxnClient dying = cluster.txnClient(t -> TxnClient.CrashPoint.AFTER_PRIMARY);
    Txn t1 = cluster.await(dying.begin());
    t1.put(utf8("a"), utf8("1")); // the primary: the smallest key
    t1.put(utf8("b"), utf8("2")); // a secondary, left locked
    assertThatThrownBy(() -> cluster.await(t1.commit())).isInstanceOf(TxnException.class);
    // The reader meets b's lock, finds the primary committed, and rolls b forward.
    assertThat(get("b")).isEqualTo("2");
    assertThat(get("a")).isEqualTo("1");
  }

  // ---------------------------------------------------------------------------------------------

  private void set(String key, String value) {
    cluster.await(
        client.run(
            t -> {
              t.put(utf8(key), utf8(value));
              return java.util.concurrent.CompletableFuture.completedFuture(null);
            },
            3));
  }

  private String get(String key) {
    return read(cluster.await(client.begin()), key);
  }

  private String read(Txn t, String key) {
    Optional<byte[]> v = cluster.await(t.get(utf8(key)));
    return v.map(b -> new String(b, StandardCharsets.UTF_8)).orElse(null);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}

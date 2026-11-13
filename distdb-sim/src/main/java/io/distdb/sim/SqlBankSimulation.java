package io.distdb.sim;

import io.distdb.common.Bytes;
import io.distdb.common.encoding.KeyWriter;
import io.distdb.env.RandomSource;
import io.distdb.env.SplittableRandomSource;
import io.distdb.sql.Engine;
import io.distdb.sql.Result;
import io.distdb.sql.Session;
import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.catalog.Catalog;
import io.distdb.sql.catalog.TableDesc;
import io.distdb.sql.catalog.TableDesc.Index;
import io.distdb.sql.catalog.TableDesc.IndexState;
import io.distdb.sql.encoding.RowCodec;
import io.distdb.sql.kv.KvTxn;
import io.distdb.sql.kv.TxnKvStore;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The Phase 8 gate: a bank in SQL, on the faulty sharded cluster of a seed.
 *
 * <p>Several gateways (SQL engines, each on its own transaction client) serve sessions that run
 * transfers (multi-statement transactions and single statements), owner changes, audits and index
 * audits, while partitions, power cuts, disk failures, splits, moves and GC go on, and admin
 * sessions build, drop and rebuild a secondary index under the load (and try a unique index that
 * must fail). Each session is a {@link SimFibers fiber}: their statements interleave at every wait,
 * deterministically.
 *
 * <p>Checks: every audit finds the opening total and no negative balance; an index lookup and a
 * full scan always agree; after the faults, every public index has exactly one entry per row, at
 * the row's values; no error other than those the faults explain; and the cluster's own checks
 * (replicas converge, one entry per index everywhere, no overlapping replicas).
 */
public final class SqlBankSimulation {

  public record Options(
      int accounts,
      int sessions,
      int gateways,
      int owners,
      Duration faultPeriod,
      ShardedKvSimulation.Options cluster) {

    public static Options of(long seed) {
      RandomSource r = SplittableRandomSource.seeded(seed * 0x5DEECE66DL + 17);
      ShardedKvSimulation.Options bank = ShardedKvSimulation.Options.bank(seed);
      ShardedKvSimulation.Options cluster =
          new ShardedKvSimulation.Options(
              bank.dataNodes(),
              0,
              bank.keys(),
              bank.splitKeys(),
              bank.forcedOps(),
              bank.faultPeriod(),
              // A fault at most every 2 s: a SQL statement is several round trips in a row.
              Duration.ofSeconds(2),
              bank.compactEvery(),
              bank.heartbeat(),
              true,
              Duration.ofMillis(r.nextInt(3000, 8001)));
      return new Options(
          r.nextInt(6, 25),
          r.nextInt(3, 9),
          r.nextInt(1, 4),
          r.nextInt(2, 6),
          Duration.ofSeconds(20),
          cluster);
    }
  }

  public record Report(
      long seed,
      long transfers,
      long declined,
      long conflicts,
      long unknown,
      long failed,
      long audits,
      long indexAudits,
      long ownerChanges,
      long opened,
      long closed,
      long indexesBuilt,
      long indexBuildsFailed,
      long indexesDropped,
      long statements,
      ShardedKvSimulation.Report cluster) {}

  private static final int OPENING = 100;
  private static final String INDEX = "accounts_owner";

  private final long seed;
  private final Options options;
  private final ShardedKvSimulation.Chaos chaos;
  private final SimFibers fibers;
  private final RandomSource random;
  private final List<Engine> gateways = new ArrayList<>();
  private final List<TxnKvStore> stores = new ArrayList<>();
  private final long total;
  private boolean stopped;
  private boolean indexBuilt;
  private long transfers;
  private long declined;
  private long conflicts;
  private long unknown;
  private long failed;
  private long audits;
  private long indexAudits;
  private long ownerChanges;
  private long indexesBuilt;
  private long indexBuildsFailed;
  private long indexesDropped;
  private long statements;
  private long opened;
  private long closed;

  private SqlBankSimulation(long seed, Options options) {
    this.seed = seed;
    this.options = Objects.requireNonNull(options);
    this.chaos = ShardedKvSimulation.Chaos.start(seed, options.cluster());
    this.fibers = new SimFibers(chaos::schedule, () -> chaos.run(Duration.ofMillis(10)));
    this.random = chaos.random();
    this.total = (long) OPENING * options.accounts();
    for (int g = 0; g < options.gateways(); g++) {
      TxnKvStore store = new TxnKvStore(chaos.txnClient(), fibers.awaiter());
      stores.add(store);
      gateways.add(new Engine(store));
    }
  }

  /** Runs one seed; throws an AssertionError on any violation. */
  public static Report run(long seed, Options options) {
    return new SqlBankSimulation(seed, options).execute();
  }

  private Report execute() {
    open(gateways.get(0).session());
    chaos.faults(true);
    for (int s = 0; s < options.sessions(); s++) {
      Session session = gateways.get(s % gateways.size()).session();
      fibers.spawn(
          "sql-" + seed + "-" + s, Duration.ofMillis(random.nextInt(1, 50)), () -> work(session));
    }
    scheduleSchemaChanges();
    chaos.run(options.faultPeriod());
    // Stop starting work; let the operations in flight finish.
    stopped = true;
    // A statement can retry through faults for minutes (a schema change has several steps).
    for (int i = 0; i < 6000 && !fibers.allDone(); i++) {
      chaos.run(Duration.ofMillis(100));
    }
    if (!fibers.allDone()) {
      throw new AssertionError(
          "seed "
              + seed
              + " at "
              + chaos.elapsed()
              + ": sessions never finished: "
              + fibers.running()
              + ", running "
              + doing.values());
    }
    chaos.heal();
    // After the faults: the index built (if the admin left it dropped), a clean audit, and every
    // public index exact.
    if (!indexBuilt) {
      Session admin = gateways.get(0).session();
      for (int k = 0; k < 20 && !buildIndex(admin); k++) {
        chaos.run(Duration.ofMillis(500));
      }
    }
    audit(gateways.get(gateways.size() - 1).session());
    checkIndexes();
    ShardedKvSimulation.Report cluster = chaos.finish();
    return new Report(
        seed,
        transfers,
        declined,
        conflicts,
        unknown,
        failed,
        audits,
        indexAudits,
        ownerChanges,
        opened,
        closed,
        indexesBuilt,
        indexBuildsFailed,
        indexesDropped,
        statements,
        cluster);
  }

  /** Creates and fills the table, retrying through the faults; sets the split points. */
  private void open(Session admin) {
    StringBuilder rows = new StringBuilder();
    for (int a = 0; a < options.accounts(); a++) {
      if (a > 0) {
        rows.append(", ");
      }
      rows.append("(")
          .append(a)
          .append(", 'o")
          .append(a % options.owners())
          .append("', ")
          .append(OPENING)
          .append(")");
    }
    retryUntilDone(
        admin,
        "CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, owner TEXT NOT NULL, balance"
            + " INT NOT NULL)");
    retryUntilDone(admin, "INSERT INTO accounts VALUES " + rows + " ON CONFLICT DO NOTHING");
    // Splits between accounts, and between the rows and the index.
    List<byte[]> points = new ArrayList<>();
    long table = Catalog.FIRST_TABLE_ID;
    for (int a = 1; a < options.accounts(); a++) {
      points.add(new KeyWriter().writeLong(table).writeLong(1).writeLong(a).toByteArray());
    }
    points.add(new KeyWriter().writeLong(table).writeLong(2).toByteArray());
    points.add(new KeyWriter().writeLong(table).writeLong(3).toByteArray());
    chaos.splitPoints(points);
  }

  private void retryUntilDone(Session s, String sql) {
    for (int attempt = 0; ; attempt++) {
      try {
        s.execute(sql);
        return;
      } catch (SqlException e) {
        if (attempt > 200) {
          throw new AssertionError("could not run " + sql + " in 200 attempts", e);
        }
        chaos.run(Duration.ofMillis(200));
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Sessions

  private void work(Session s) {
    while (!stopped) {
      int dice = random.nextInt(100);
      try {
        if (dice < 35) {
          transfer(s);
        } else if (dice < 48) {
          oneStatementTransfer(s);
        } else if (dice < 55) {
          // Open an empty account: an INSERT, which must maintain every index, even one being
          // built.
          run(
              s,
              "INSERT INTO accounts VALUES ("
                  + spareId()
                  + ", 'o"
                  + random.nextInt(options.owners())
                  + "', 0) ON CONFLICT DO NOTHING");
          opened++;
        } else if (dice < 60) {
          // Close one, if it is still empty (only spare accounts: the first ones always stay).
          run(s, "DELETE FROM accounts WHERE id = " + spareId() + " AND balance = 0");
          closed++;
        } else if (dice < 70) {
          run(
              s,
              "UPDATE accounts SET owner = 'o"
                  + random.nextInt(options.owners())
                  + "' WHERE id = "
                  + random.nextInt(options.accounts()));
          ownerChanges++;
        } else if (dice < 85) {
          checkAudit(run(s, "SELECT count(*), sum(balance), min(balance) FROM accounts"));
        } else {
          indexAudit(s);
        }
      } catch (SqlException e) {
        expected(e, "a session's operation");
        switch (e.state()) {
          case SqlState.SERIALIZATION_FAILURE -> conflicts++;
          case SqlState.STATEMENT_COMPLETION_UNKNOWN -> unknown++;
          default -> failed++;
        }
        // Abandon it: the money is safe whatever happened, the transaction is all-or-nothing.
        if (s.state() != Session.TxnState.IDLE) {
          try {
            s.execute("ROLLBACK");
          } catch (SqlException again) {
            expected(again, "ROLLBACK");
          }
        }
      }
      fibers.sleep(Duration.ofMillis(random.nextInt(1, 60)));
    }
  }

  private Result run(Session s, String sql) {
    statements++;
    doing.put(s, chaos.elapsed() + " " + sql);
    try {
      return s.executeOne(sql);
    } finally {
      doing.remove(s);
    }
  }

  /** What each session is running, and since when, for a stuck run's report. */
  private final Map<Session, String> doing = new java.util.IdentityHashMap<>();

  /** A pause inside a transaction, so that others' statements come between its own. */
  private void pause() {
    fibers.sleep(Duration.ofMillis(random.nextInt(0, 40)));
  }

  /** An id for opening or closing a spare account: past the first, permanent ones. */
  private int spareId() {
    return options.accounts() + random.nextInt(30);
  }

  private int[] pair() {
    int n = options.accounts();
    int from = random.nextInt(n);
    int to = (from + 1 + random.nextInt(n - 1)) % n;
    return new int[] {from, to};
  }

  private void transfer(Session s) {
    int[] p = pair();
    int amount = random.nextInt(1, 40);
    run(s, "BEGIN");
    pause();
    long balance =
        (Long) run(s, "SELECT balance FROM accounts WHERE id = " + p[0]).rows().get(0)[0];
    pause();
    if (balance < amount) {
      run(s, "ROLLBACK");
      declined++;
      return;
    }
    run(s, "UPDATE accounts SET balance = balance - " + amount + " WHERE id = " + p[0]);
    pause();
    run(s, "UPDATE accounts SET balance = balance + " + amount + " WHERE id = " + p[1]);
    pause();
    run(s, "COMMIT");
    transfers++;
  }

  private void oneStatementTransfer(Session s) {
    int[] p = pair();
    int amount = random.nextInt(1, 40);
    String sql =
        "UPDATE accounts SET balance = balance + CASE WHEN id = "
            + p[0]
            + " THEN -"
            + amount
            + " ELSE "
            + amount
            + " END WHERE id IN ("
            + p[0]
            + ", "
            + p[1]
            + ") AND (SELECT balance FROM accounts WHERE id = "
            + p[0]
            + ") >= "
            + amount;
    Result r = run(s, sql);
    switch (r.tag()) {
      case "UPDATE 2" -> transfers++;
      case "UPDATE 0" -> declined++;
      default -> throw new AssertionError("seed " + seed + ": " + sql + " changed " + r.tag());
    }
  }

  /** An index lookup and a full scan of the same snapshot find the same rows. */
  private void indexAudit(Session s) {
    String owner = "o" + random.nextInt(options.owners());
    run(s, "BEGIN");
    long byIndex =
        (Long)
            run(s, "SELECT count(*) FROM accounts WHERE owner = '" + owner + "'").rows().get(0)[0];
    pause();
    long byScan =
        (Long)
            run(s, "SELECT count(*) FROM accounts WHERE owner || '' = '" + owner + "'")
                .rows()
                .get(0)[0];
    run(s, "COMMIT");
    if (byIndex != byScan) {
      throw new AssertionError(
          "seed "
              + seed
              + ": for owner "
              + owner
              + " the index found "
              + byIndex
              + " rows, a full scan "
              + byScan);
    }
    indexAudits++;
  }

  private void checkAudit(Result r) {
    Object[] row = r.rows().get(0);
    long count = (Long) row[0];
    long sum = row[1] instanceof Long l ? l : ((BigDecimal) row[1]).longValueExact();
    long min = (Long) row[2];
    if (count < options.accounts() || sum != total || min < 0) {
      throw new AssertionError(
          "seed "
              + seed
              + ": an audit found "
              + count
              + " accounts holding "
              + sum
              + " (min "
              + min
              + "), not "
              + options.accounts()
              + " holding "
              + total);
    }
    audits++;
  }

  // -----------------------------------------------------------------------------------------------
  // Schema changes under load

  /**
   * One admin session changes the schema while the others work: it builds the index, drops it,
   * builds it again, and so on, sometimes trying a unique index that must fail; and it leaves the
   * index built at the end, so the final check covers an index built under load.
   */
  private void scheduleSchemaChanges() {
    Session admin = gateways.get(gateways.size() - 1).session();
    fibers.spawn("ddl-" + seed, Duration.ofMillis(random.nextLong(200, 1500)), () -> admin(admin));
  }

  private void admin(Session s) {
    boolean built = false;
    while (!stopped) {
      if (random.nextInt(5) == 0) {
        tryUniqueIndex(s);
      } else if (!built) {
        built = buildIndex(s);
      } else if (attempt(s, "DROP INDEX " + INDEX, SqlState.UNDEFINED_OBJECT)) {
        indexesDropped++;
        built = false;
      }
      fibers.sleep(Duration.ofMillis(random.nextInt(300, 2000)));
    }
    indexBuilt = built;
  }

  /** Builds the index; after a failure, drops what a cut-short build left. */
  private boolean buildIndex(Session s) {
    if (attempt(s, "CREATE INDEX " + INDEX + " ON accounts (owner)", null)) {
      indexesBuilt++;
      return true;
    }
    indexBuildsFailed++;
    attempt(s, "DROP INDEX IF EXISTS " + INDEX, null);
    return false;
  }

  /** The owners are not unique, so this must fail with 23505 (or a fault). */
  private void tryUniqueIndex(Session s) {
    try {
      run(s, "CREATE UNIQUE INDEX accounts_owner_unique ON accounts (owner)");
      throw new AssertionError("seed " + seed + ": a unique index on non-unique owners was built");
    } catch (SqlException e) {
      if (!e.state().equals(SqlState.UNIQUE_VIOLATION)) {
        expected(e, "CREATE UNIQUE INDEX");
        attempt(s, "DROP INDEX IF EXISTS accounts_owner_unique", null);
      }
    }
  }

  /** Runs {@code sql}; true if it succeeded or failed with {@code okState}. */
  private boolean attempt(Session s, String sql, String okState) {
    try {
      run(s, sql);
      return true;
    } catch (SqlException e) {
      if (e.state().equals(okState)) {
        return true;
      }
      expected(e, sql);
      return false;
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Checks

  /** Failures the faults explain; anything else (a wrong result, corruption, a bug) fails. */
  private void expected(SqlException e, String sql) {
    switch (e.state()) {
      case SqlState.SERIALIZATION_FAILURE,
          SqlState.STATEMENT_COMPLETION_UNKNOWN,
          SqlState.SYSTEM_ERROR,
          SqlState.IN_FAILED_SQL_TRANSACTION -> {
        // a conflict, a fence, a timeout under partition, an unknown commit
      }
      case SqlState.DUPLICATE_TABLE, SqlState.UNDEFINED_OBJECT -> {
        // a schema change retried after an unknown outcome, or an index another change dropped
      }
      case SqlState.UNIQUE_VIOLATION -> {
        // A unique index being built (and bound to fail: owners repeat) already enforces
        // uniqueness on the entries it has, as PostgreSQL's unfinished concurrent builds do.
        if (!String.valueOf(e.getMessage()).contains("accounts_owner_unique")) {
          throw new AssertionError("seed " + seed + ": unexpected error for " + sql + ": " + e, e);
        }
      }
      default ->
          throw new AssertionError("seed " + seed + ": unexpected error for " + sql + ": " + e, e);
    }
  }

  /** An audit that must succeed, retried until the healed cluster answers. */
  private void audit(Session s) {
    for (int attempt = 0; ; attempt++) {
      try {
        checkAudit(s.executeOne("SELECT count(*), sum(balance), min(balance) FROM accounts"));
        return;
      } catch (SqlException e) {
        expected(e, "the final audit");
        if (attempt > 100) {
          throw new AssertionError("seed " + seed + ": the final audit never succeeded", e);
        }
        chaos.run(Duration.ofMillis(500));
      }
    }
  }

  /**
   * Every public index has exactly the entries of the rows: one per row, at the row's values,
   * pointing at its primary key. Read raw, in one snapshot.
   */
  private void checkIndexes() {
    for (int attempt = 0; ; attempt++) {
      KvTxn txn = stores.get(0).begin();
      try {
        TableDesc t = new Catalog().load(txn).table("accounts");
        byte[] rowsStart = RowCodec.indexPrefix(t.id(), TableDesc.PRIMARY_INDEX_ID);
        List<KvTxn.Entry> rows = txn.scan(rowsStart, Bytes.prefixEnd(rowsStart).orElseThrow(), 0);
        if (rows.size() < options.accounts()) {
          throw new AssertionError(
              "seed " + seed + ": " + rows.size() + " rows, fewer than " + options.accounts());
        }
        for (Index i : t.indexes()) {
          if (i.state() != IndexState.PUBLIC) {
            continue;
          }
          Map<String, byte[]> expected = new HashMap<>();
          for (KvTxn.Entry e : rows) {
            Object[] row = RowCodec.decodeRow(t, e.key(), e.value());
            expected.put(Bytes.toHex(RowCodec.indexKey(t, i, row)), e.key());
          }
          byte[] start = RowCodec.indexPrefix(t.id(), i.id());
          for (KvTxn.Entry e : txn.scan(start, Bytes.prefixEnd(start).orElseThrow(), 0)) {
            byte[] pk = expected.remove(Bytes.toHex(e.key()));
            if (pk == null) {
              throw new AssertionError(
                  "seed " + seed + ": index " + i.name() + " has an entry for no row");
            }
            if (!Arrays.equals(pk, RowCodec.primaryKeyOf(t, i, e.key(), e.value()))) {
              throw new AssertionError(
                  "seed " + seed + ": index " + i.name() + " points at the wrong row");
            }
          }
          if (!expected.isEmpty()) {
            throw new AssertionError(
                "seed "
                    + seed
                    + ": index "
                    + i.name()
                    + " is missing "
                    + expected.size()
                    + " of "
                    + rows.size()
                    + " rows");
          }
        }
        txn.rollback();
        return;
      } catch (SqlException e) {
        txn.rollback();
        expected(e, "the index check");
        if (attempt > 100) {
          throw new AssertionError("seed " + seed + ": the index check never ran", e);
        }
        chaos.run(Duration.ofMillis(500));
      }
    }
  }
}

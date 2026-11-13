package io.distdb.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.sql.testing.MemoryKvStore;
import io.distdb.sql.types.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The session: plans, prepared statements, transactions across sessions, schema changes. */
class SessionTest {

  private MemoryKvStore store;
  private Engine engine;
  private Session s;

  @BeforeEach
  void setUp() {
    store = new MemoryKvStore();
    engine = new Engine(store);
    s = engine.session();
    s.execute(
        "CREATE TABLE acct (id INT PRIMARY KEY, owner TEXT, branch INT, balance INT);"
            + "CREATE TABLE branch (id INT PRIMARY KEY, name TEXT UNIQUE);"
            + "INSERT INTO acct VALUES (1, 'ann', 1, 100), (2, 'bob', 1, 50), (3, 'cy', 2, 7);"
            + "INSERT INTO branch VALUES (1, 'north'), (2, 'south')");
  }

  private List<String> plan(String sql) {
    List<String> lines = new ArrayList<>();
    for (Object[] row : s.executeOne("EXPLAIN " + sql).rows()) {
      lines.add((String) row[0]);
    }
    return lines;
  }

  private List<List<Object>> rows(Session session, String sql) {
    List<List<Object>> out = new ArrayList<>();
    for (Object[] r : session.executeOne(sql).rows()) {
      out.add(java.util.Arrays.asList(r));
    }
    return out;
  }

  // -----------------------------------------------------------------------------------------------
  // Plans

  @Test
  void anEqualityOnThePrimaryKeyIsAPointGet() {
    assertThat(String.join("\n", plan("SELECT * FROM acct WHERE id = 2")))
        .contains("point get acct on acct_pkey (2)");
  }

  @Test
  void aRangeOnThePrimaryKeyIsANarrowScan() {
    assertThat(String.join("\n", plan("SELECT * FROM acct WHERE id > 1 AND id <= 3")))
        .contains("index scan acct on acct_pkey after 1 through 3");
  }

  @Test
  void aSecondaryIndexIsUsedOnlyOnceItIsPublic() {
    assertThat(String.join("\n", plan("SELECT * FROM acct WHERE owner = 'bob'")))
        .contains("full scan acct");
    s.execute("CREATE INDEX acct_owner ON acct (owner)");
    assertThat(String.join("\n", plan("SELECT * FROM acct WHERE owner = 'bob'")))
        .contains("index scan acct on acct_owner = ('bob')");
    assertThat(rows(s, "SELECT id FROM acct WHERE owner = 'bob'")).containsExactly(List.of(2L));
  }

  @Test
  void anEquiJoinIsAHashJoinAndAKeyedJoinFromAPointIsALookup() {
    assertThat(String.join("\n", plan("SELECT * FROM acct a JOIN branch b ON a.branch = b.id")))
        .contains("hash join on a.branch = b.id");
    String lookup =
        String.join(
            "\n",
            plan("SELECT * FROM acct a JOIN branch b ON a.branch = b.id WHERE a.id" + " = 1"));
    assertThat(lookup).contains("lookup join branch b on branch_pkey").contains("point get acct");
    assertThat(rows(s, "SELECT b.name FROM acct a JOIN branch b ON a.branch = b.id WHERE a.id = 3"))
        .containsExactly(List.of("south"));
  }

  @Test
  void aNonEquiJoinIsANestedLoopAndFiltersArePushedBelowJoins() {
    String p =
        String.join(
            "\n",
            plan(
                "SELECT * FROM acct a JOIN branch b ON a.branch < b.id WHERE"
                    + " b.name = 'south' AND a.balance > 10"));
    assertThat(p).contains("nested loop join");
    // Each side's conjunct sits under the join, on its own side.
    assertThat(p).containsPattern("-> filter b.name = 'south'|-> filter a.balance > 10");
  }

  @Test
  void aUniqueIndexAnswersAPointLookup() {
    assertThat(String.join("\n", plan("SELECT * FROM branch WHERE name = 'north'")))
        .contains("index scan branch on branch_name_key = ('north')");
  }

  // -----------------------------------------------------------------------------------------------
  // Prepared statements

  @Test
  void parametersTakeTheirTypeFromContext() {
    Prepared p = s.prepare("SELECT owner FROM acct WHERE id = $1 AND balance > $2", List.of());
    assertThat(p.paramTypes()).containsExactly(Type.INT, Type.INT);
    assertThat(p.columns()).containsExactly(new Result.Column("owner", Type.TEXT));
    assertThat(s.execute(p, List.of(1L, 10L)).rows()).hasSize(1);
    assertThat(s.execute(p, List.of(3L, 10L)).rows()).isEmpty();
  }

  @Test
  void anInsertsParametersTakeTheColumnsTypes() {
    Prepared p = s.prepare("INSERT INTO acct VALUES ($1, $2, $3, $4)", List.of());
    assertThat(p.paramTypes()).containsExactly(Type.INT, Type.TEXT, Type.INT, Type.INT);
    assertThat(p.columns()).isNull();
    assertThat(s.execute(p, List.of(9L, "zed", 2L, 1L)).tag()).isEqualTo("INSERT 0 1");
    assertThat(rows(s, "SELECT owner FROM acct WHERE id = 9")).containsExactly(List.of("zed"));
  }

  @Test
  void aPreparedStatementSurvivesASchemaChange() {
    Prepared p = s.prepare("SELECT count(*) FROM acct WHERE owner = $1", List.of());
    assertThat(s.execute(p, List.of("ann")).rows().get(0)[0]).isEqualTo(1L);
    s.execute("CREATE INDEX acct_owner ON acct (owner)");
    s.execute("INSERT INTO acct VALUES (4, 'ann', 2, 1)");
    assertThat(s.execute(p, List.of("ann")).rows().get(0)[0]).isEqualTo(2L);
  }

  // -----------------------------------------------------------------------------------------------
  // Transactions across sessions

  @Test
  void transactionsReadFromTheirSnapshot() {
    Session other = engine.session();
    s.execute("BEGIN");
    assertThat(rows(s, "SELECT balance FROM acct WHERE id = 1")).containsExactly(List.of(100L));
    other.execute("UPDATE acct SET balance = 0 WHERE id = 1");
    // Repeatable: the committed update is not visible in the open transaction.
    assertThat(rows(s, "SELECT balance FROM acct WHERE id = 1")).containsExactly(List.of(100L));
    s.execute("COMMIT");
    assertThat(rows(s, "SELECT balance FROM acct WHERE id = 1")).containsExactly(List.of(0L));
  }

  @Test
  void theSecondOfTwoConflictingWritersFailsWith40001() {
    Session other = engine.session();
    s.execute("BEGIN");
    other.execute("BEGIN");
    s.execute("UPDATE acct SET balance = balance + 1 WHERE id = 1");
    other.execute("UPDATE acct SET balance = balance + 2 WHERE id = 1");
    s.execute("COMMIT");
    assertThatThrownBy(() -> other.execute("COMMIT"))
        .isInstanceOfSatisfying(
            SqlException.class,
            e -> assertThat(e.state()).isEqualTo(SqlState.SERIALIZATION_FAILURE));
    assertThat(other.state()).isEqualTo(Session.TxnState.IDLE);
    assertThat(rows(s, "SELECT balance FROM acct WHERE id = 1")).containsExactly(List.of(101L));
  }

  @Test
  void twoInsertsOfOneKeyCannotBothCommit() {
    Session other = engine.session();
    s.execute("BEGIN");
    other.execute("BEGIN");
    s.execute("INSERT INTO acct VALUES (10, 'x', 1, 1)");
    other.execute("INSERT INTO acct VALUES (10, 'y', 1, 1)");
    s.execute("COMMIT");
    assertThatThrownBy(() -> other.execute("COMMIT")).isInstanceOf(SqlException.class);
    assertThat(rows(s, "SELECT owner FROM acct WHERE id = 10")).containsExactly(List.of("x"));
  }

  @Test
  void anErrorAbortsTheBlockUntilRollback() {
    s.execute("BEGIN");
    assertThatThrownBy(() -> s.execute("INSERT INTO acct VALUES (1, 'dup', 1, 1)"))
        .isInstanceOf(SqlException.class);
    assertThat(s.state()).isEqualTo(Session.TxnState.FAILED);
    assertThatThrownBy(() -> s.execute("SELECT 1"))
        .isInstanceOfSatisfying(
            SqlException.class,
            e -> assertThat(e.state()).isEqualTo(SqlState.IN_FAILED_SQL_TRANSACTION));
    assertThat(s.executeOne("COMMIT").tag()).isEqualTo("ROLLBACK");
    assertThat(s.state()).isEqualTo(Session.TxnState.IDLE);
  }

  // -----------------------------------------------------------------------------------------------
  // Schema changes

  @Test
  void createIndexFencesOutAWriterThatStartedWithTheOldSchema() {
    Session writer = engine.session();
    writer.execute("BEGIN");
    // This transaction's schema has no index: its row would have no entry.
    writer.execute("INSERT INTO acct VALUES (20, 'late', 1, 1)");
    s.execute("CREATE INDEX acct_owner ON acct (owner)");
    assertThatThrownBy(() -> writer.execute("COMMIT"))
        .isInstanceOfSatisfying(
            SqlException.class,
            e -> assertThat(e.state()).isEqualTo(SqlState.SERIALIZATION_FAILURE));
    // Run again, it sees the index and maintains it.
    writer.execute("INSERT INTO acct VALUES (20, 'late', 1, 1)");
    assertThat(rows(s, "SELECT id FROM acct WHERE owner = 'late'")).containsExactly(List.of(20L));
  }

  @Test
  void schemaChangesThatFenceRefuseToRunInsideATransactionBlock() {
    s.execute("BEGIN");
    assertThatThrownBy(() -> s.execute("CREATE INDEX i ON acct (owner)"))
        .isInstanceOfSatisfying(
            SqlException.class,
            e -> assertThat(e.state()).isEqualTo(SqlState.ACTIVE_SQL_TRANSACTION));
    s.execute("ROLLBACK");
  }

  @Test
  void dropTableDeletesEveryKeyOfTheTable() {
    int before = store.liveKeys();
    s.execute("CREATE TABLE junk (id INT PRIMARY KEY, v TEXT UNIQUE)");
    s.execute("INSERT INTO junk VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    s.execute("CREATE INDEX ON junk (v)");
    assertThat(store.liveKeys()).isGreaterThan(before + 9);
    s.execute("DROP TABLE junk");
    // Only the catalog's version (and next id) keys changed; no row or entry is left.
    assertThat(store.liveKeys()).isLessThanOrEqualTo(before + 1);
  }

  @Test
  void numericKeysSortByValueAndKeepTheirScale() {
    s.execute("CREATE TABLE p (k NUMERIC PRIMARY KEY)");
    s.execute("INSERT INTO p VALUES (10.50), (-3), (0.001), (-0.25), (2), (100)");
    List<Object> keys = new ArrayList<>();
    for (Object[] r : s.executeOne("SELECT k FROM p WHERE k > -1 ORDER BY k").rows()) {
      keys.add(r[0]);
    }
    assertThat(keys)
        .containsExactly(
            new BigDecimal("-0.25"),
            new BigDecimal("0.001"),
            new BigDecimal("2"),
            new BigDecimal("10.50"),
            new BigDecimal("100"));
  }

  @Test
  void rowsAndIndexesAgreeAfterManyWrites() {
    s.execute("CREATE INDEX acct_owner ON acct (owner)");
    s.execute("CREATE INDEX acct_branch ON acct (branch, balance)");
    java.util.SplittableRandom r = new java.util.SplittableRandom(7);
    for (int i = 0; i < 300; i++) {
      int id = r.nextInt(30);
      String owner = "o" + r.nextInt(6);
      switch (r.nextInt(3)) {
        case 0 ->
            s.execute(
                "INSERT INTO acct VALUES ("
                    + id
                    + ", '"
                    + owner
                    + "', "
                    + r.nextInt(4)
                    + ", "
                    + r.nextInt(100)
                    + ") ON CONFLICT DO NOTHING");
        case 1 ->
            s.execute(
                "UPDATE acct SET owner = '"
                    + owner
                    + "', branch = "
                    + r.nextInt(4)
                    + " WHERE id = "
                    + id);
        default -> s.execute("DELETE FROM acct WHERE id = " + id);
      }
    }
    for (int o = 0; o < 6; o++) {
      String q = "SELECT id FROM acct WHERE owner = 'o" + o + "' ORDER BY id";
      List<List<Object>> viaIndex = rows(s, q);
      List<List<Object>> viaScan =
          rows(s, "SELECT id FROM acct WHERE owner || '' = 'o" + o + "' ORDER BY id");
      assertThat(viaIndex).isEqualTo(viaScan);
    }
    for (int b = 0; b < 4; b++) {
      assertThat(rows(s, "SELECT id FROM acct WHERE branch = " + b + " ORDER BY id"))
          .isEqualTo(rows(s, "SELECT id FROM acct WHERE branch + 0 = " + b + " ORDER BY id"));
    }
  }
}

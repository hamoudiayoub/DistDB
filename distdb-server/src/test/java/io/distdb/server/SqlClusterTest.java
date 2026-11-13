package io.distdb.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.server.DistdbProcesses.NodeProcess;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Phase 8 milestone on real processes: a placement driver of three JVMs and three data nodes,
 * each serving SQL. PostgreSQL's JDBC driver and {@code psql} connect to different nodes: a table
 * created through one is used through another, joins and transactions work, and two nodes'
 * conflicting transactions cannot both commit.
 */
class SqlClusterTest {

  @TempDir Path tmp;

  @Test
  void psqlAndJdbcRunJoinsAndTransactionsOnAnyNode() throws Exception {
    DistdbProcesses procs = new DistdbProcesses(tmp);
    int[] ports = DistdbProcesses.freePorts(9);
    List<String> pdAddresses = new ArrayList<>();
    StringBuilder pdPeers = new StringBuilder();
    for (int i = 0; i < 3; i++) {
      pdAddresses.add("127.0.0.1:" + ports[i]);
      pdPeers.append(i == 0 ? "" : ",").append(i + 1).append('=').append(pdAddresses.get(i));
    }
    List<NodeProcess> all = new ArrayList<>();
    int[] sql = new int[3];
    try {
      for (int i = 0; i < 3; i++) {
        all.add(
            procs.pd(
                "--data-dir",
                tmp.resolve("pd" + (i + 1)).toString(),
                "--id",
                "" + (i + 1),
                "--peers",
                pdPeers.toString()));
      }
      for (int i = 0; i < 3; i++) {
        sql[i] = ports[6 + i];
        all.add(
            procs.node(
                "--data-dir",
                tmp.resolve("n" + (i + 1)).toString(),
                "--id",
                "" + (i + 1),
                "--listen",
                "127.0.0.1:" + ports[3 + i],
                "--pd",
                String.join(",", pdAddresses),
                "--founders",
                "1,2,3",
                "--sql",
                "127.0.0.1:" + sql[i]));
      }
      try (Connection a = connect(sql[0]);
          Connection b = connect(sql[1]);
          Connection c = connect(sql[2])) {
        // The cluster forms: retry the first statement until a range leader serves it.
        untilItWorks(a, "CREATE TABLE IF NOT EXISTS dept (id INT PRIMARY KEY, name TEXT)");
        exec(b, "CREATE TABLE person (id INT PRIMARY KEY, name TEXT NOT NULL, dept INT)");
        exec(c, "INSERT INTO dept VALUES (1, 'eng'), (2, 'sales')");
        exec(
            a,
            "INSERT INTO person VALUES (10, 'ann', 1), (11, 'bob', 1), (12, 'cy', 2),"
                + " (13, 'dee', NULL)");
        // A join through the third node, of tables made through the other two.
        assertThat(
                query(
                    c,
                    "SELECT p.name, d.name FROM person p LEFT JOIN dept d ON p.dept ="
                        + " d.id ORDER BY p.id"))
            .containsExactly("ann eng", "bob eng", "cy sales", "dee null");
        assertThat(
                query(
                    b,
                    "SELECT d.name, count(*) FROM person p JOIN dept d ON p.dept = d.id"
                        + " GROUP BY d.name ORDER BY 1"))
            .containsExactly("eng 2", "sales 1");
        // An index built through one node is used, and maintained, through another. (Unique: on
        // four rows a non-unique index loses to one filtered scan on the table's range, by cost.)
        exec(c, "CREATE UNIQUE INDEX person_name ON person (name)");
        assertThat(String.join("\n", query(a, "EXPLAIN SELECT id FROM person WHERE name = 'cy'")))
            .contains("index scan person on person_name");
        assertThat(query(a, "SELECT id FROM person WHERE name = 'cy'")).containsExactly("12");
        assertThatThrownBy(() -> exec(a, "INSERT INTO person VALUES (14, 'cy', 1)"))
            .isInstanceOfSatisfying(
                SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("23505"));
        // A transaction across nodes' sessions: a conflict, then the retry.
        a.setAutoCommit(false);
        b.setAutoCommit(false);
        exec(a, "UPDATE person SET dept = 2 WHERE id = 10");
        exec(b, "UPDATE person SET dept = 1 WHERE id = 10");
        a.commit();
        assertThatThrownBy(b::commit)
            .isInstanceOfSatisfying(
                SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("40001"));
        b.rollback();
        a.setAutoCommit(true);
        b.setAutoCommit(true);
        assertThat(query(c, "SELECT dept FROM person WHERE id = 10")).containsExactly("2");
      }
      psql(sql[1]);
    } finally {
      for (NodeProcess p : all) {
        p.close();
      }
    }
  }

  /** psql, if this machine has it, runs a script through the simple query protocol. */
  private void psql(int port) throws Exception {
    Optional<Path> psql = findPsql();
    Assumptions.assumeTrue(psql.isPresent(), "psql is not installed");
    Path script = tmp.resolve("script.sql");
    Files.writeString(
        script,
        """
        CREATE TABLE note (id INT PRIMARY KEY, body TEXT);
        BEGIN;
        INSERT INTO note VALUES (1, 'first'), (2, 'second');
        UPDATE note SET body = upper(body) WHERE id = 2;
        COMMIT;
        BEGIN;
        DELETE FROM note;
        ROLLBACK;
        SELECT n.id, n.body, p.name FROM note n JOIN person p ON p.id = n.id + 9 ORDER BY n.id;
        SELECT count(*) FROM person;
        """);
    Process p =
        new ProcessBuilder(
                psql.get().toString(),
                "-h",
                "127.0.0.1",
                "-p",
                "" + port,
                "-U",
                "distdb",
                "-d",
                "distdb",
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
                "-F",
                " ",
                "-f",
                script.toString())
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(p.waitFor(60, TimeUnit.SECONDS)).isTrue();
    assertThat(p.exitValue()).as(out).isZero();
    assertThat(out.replace("\r\n", "\n").lines().toList())
        .containsSubsequence("1 first ann", "2 SECOND bob", "4");
  }

  private static Optional<Path> findPsql() {
    String env = System.getenv("DISTDB_PSQL");
    List<Path> candidates = new ArrayList<>();
    if (env != null) {
      candidates.add(Path.of(env));
    }
    for (String dir : System.getenv().getOrDefault("PATH", "").split(java.io.File.pathSeparator)) {
      candidates.add(Path.of(dir, "psql"));
      candidates.add(Path.of(dir, "psql.exe"));
    }
    for (int v = 20; v >= 12; v--) {
      candidates.add(Path.of("C:\\Program Files\\PostgreSQL\\" + v + "\\bin\\psql.exe"));
    }
    return candidates.stream().filter(Files::isExecutable).findFirst();
  }

  private static Connection connect(int port) throws SQLException {
    Properties p = new Properties();
    p.setProperty("user", "distdb");
    p.setProperty("connectTimeout", "30");
    return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + port + "/distdb", p);
  }

  private static void exec(Connection c, String sql) throws SQLException {
    try (Statement s = c.createStatement()) {
      s.execute(sql);
    }
  }

  private static void untilItWorks(Connection c, String sql) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
    while (true) {
      try {
        exec(c, sql);
        return;
      } catch (SQLException e) {
        if (System.nanoTime() > deadline) {
          throw e;
        }
        Thread.sleep(500);
      }
    }
  }

  private static List<String> query(Connection c, String sql) throws SQLException {
    List<String> out = new ArrayList<>();
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(sql)) {
      int n = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        List<String> row = new ArrayList<>();
        for (int k = 1; k <= n; k++) {
          row.add(String.valueOf(rs.getString(k)));
        }
        out.add(String.join(" ", row));
      }
    }
    return out;
  }
}

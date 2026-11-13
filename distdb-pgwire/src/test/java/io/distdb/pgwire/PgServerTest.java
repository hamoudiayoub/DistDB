package io.distdb.pgwire;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.sql.Engine;
import io.distdb.sql.testing.MemoryKvStore;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** PostgreSQL's own JDBC driver against DistDB's wire protocol, over an in-memory store. */
class PgServerTest {

  private PgServer server;

  @BeforeEach
  void start() throws Exception {
    server = PgServer.start(new InetSocketAddress("127.0.0.1", 0), new Engine(new MemoryKvStore()));
    try (Connection c = connect();
        Statement s = c.createStatement()) {
      s.execute(
          "CREATE TABLE acct (id INT PRIMARY KEY, owner TEXT NOT NULL, balance NUMERIC(10,"
              + " 2), rate DOUBLE PRECISION, active BOOLEAN)");
      s.execute("INSERT INTO acct VALUES (1, 'ann', 10.50, 0.1, true), (2, 'bob', 0, 1.5, false)");
    }
  }

  @AfterEach
  void stop() throws Exception {
    server.close();
  }

  private Connection connect(String... options) throws SQLException {
    Properties p = new Properties();
    p.setProperty("user", "distdb");
    for (int k = 0; k < options.length; k += 2) {
      p.setProperty(options[k], options[k + 1]);
    }
    return DriverManager.getConnection(
        "jdbc:postgresql://127.0.0.1:" + server.port() + "/distdb", p);
  }

  private static List<String> column(ResultSet rs, int k) throws SQLException {
    List<String> out = new ArrayList<>();
    while (rs.next()) {
      out.add(rs.getString(k));
    }
    return out;
  }

  @Test
  void queriesReturnTypedColumns() throws Exception {
    try (Connection c = connect();
        Statement s = c.createStatement();
        ResultSet rs =
            s.executeQuery("SELECT id, owner, balance, rate, active FROM acct ORDER BY" + " id")) {
      ResultSetMetaData md = rs.getMetaData();
      assertThat(md.getColumnCount()).isEqualTo(5);
      assertThat(md.getColumnName(2)).isEqualTo("owner");
      assertThat(md.getColumnType(1)).isEqualTo(Types.BIGINT);
      assertThat(md.getColumnType(3)).isEqualTo(Types.NUMERIC);
      assertThat(md.getColumnType(4)).isEqualTo(Types.DOUBLE);
      assertThat(md.getColumnType(5)).isEqualTo(Types.BIT);
      assertThat(rs.next()).isTrue();
      assertThat(rs.getLong(1)).isEqualTo(1);
      assertThat(rs.getString(2)).isEqualTo("ann");
      assertThat(rs.getBigDecimal(3)).isEqualTo(new BigDecimal("10.50"));
      assertThat(rs.getDouble(4)).isEqualTo(0.1);
      assertThat(rs.getBoolean(5)).isTrue();
      assertThat(rs.next()).isTrue();
      assertThat(rs.getBigDecimal(3)).isEqualTo(new BigDecimal("0.00"));
      assertThat(rs.next()).isFalse();
    }
  }

  @Test
  void preparedStatementsBindParametersInTextAndBinary() throws Exception {
    // prepareThreshold=1: server-side prepared statements from the first use, with binary values.
    for (String threshold : List.of("5", "1")) {
      try (Connection c = connect("prepareThreshold", threshold, "binaryTransfer", "true")) {
        try (PreparedStatement ins =
            c.prepareStatement("INSERT INTO acct VALUES (?, ?, ?, ?, ?)")) {
          for (int i = 0; i < 3; i++) {
            int id = 100 * Integer.parseInt(threshold) + i;
            ins.setInt(1, id);
            ins.setString(2, "p" + id);
            ins.setBigDecimal(3, new BigDecimal("12.345"));
            ins.setDouble(4, -2.5);
            ins.setBoolean(5, i % 2 == 0);
            assertThat(ins.executeUpdate()).isEqualTo(1);
          }
        }
        try (PreparedStatement q =
            c.prepareStatement(
                "SELECT owner, balance, rate, active FROM acct WHERE id = ? AND balance > ?")) {
          for (int i = 0; i < 3; i++) {
            q.setLong(1, 100L * Integer.parseInt(threshold) + i);
            q.setBigDecimal(2, BigDecimal.ONE);
            try (ResultSet rs = q.executeQuery()) {
              assertThat(rs.next()).isTrue();
              assertThat(rs.getBigDecimal(2)).isEqualTo(new BigDecimal("12.35"));
              assertThat(rs.getDouble(3)).isEqualTo(-2.5);
              assertThat(rs.getBoolean(4)).isEqualTo(i % 2 == 0);
              assertThat(rs.next()).isFalse();
            }
          }
          q.setNull(1, Types.BIGINT);
          q.setBigDecimal(2, BigDecimal.ONE);
          try (ResultSet rs = q.executeQuery()) {
            assertThat(rs.next()).isFalse();
          }
        }
      }
    }
  }

  @Test
  void transactionsCommitRollBackAndConflict() throws Exception {
    try (Connection a = connect();
        Connection b = connect()) {
      a.setAutoCommit(false);
      b.setAutoCommit(false);
      try (Statement sa = a.createStatement();
          Statement sb = b.createStatement()) {
        sa.executeUpdate("UPDATE acct SET balance = balance + 1 WHERE id = 1");
        sb.executeUpdate("UPDATE acct SET balance = balance + 2 WHERE id = 1");
        a.commit();
        assertThatThrownBy(b::commit)
            .isInstanceOfSatisfying(
                SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("40001"));
        b.rollback();
        sb.executeUpdate("DELETE FROM acct WHERE id = 2");
        b.rollback();
        try (ResultSet rs = sb.executeQuery("SELECT balance FROM acct ORDER BY id")) {
          assertThat(column(rs, 1)).containsExactly("11.50", "0.00");
        }
        b.commit();
      }
    }
  }

  @Test
  void errorsCarryTheirSqlStateAndTheConnectionCarriesOn() throws Exception {
    try (Connection c = connect();
        Statement s = c.createStatement()) {
      assertThatThrownBy(() -> s.execute("INSERT INTO acct VALUES (1, 'dup', 0, 0, true)"))
          .isInstanceOfSatisfying(
              SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("23505"));
      assertThatThrownBy(() -> s.executeQuery("SELECT * FROM nope"))
          .isInstanceOfSatisfying(
              SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("42P01"));
      assertThatThrownBy(() -> s.executeQuery("SELEC 1"))
          .isInstanceOfSatisfying(
              SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("42601"));
      try (ResultSet rs = s.executeQuery("SELECT count(*) FROM acct")) {
        assertThat(column(rs, 1)).containsExactly("2");
      }
      c.setAutoCommit(false);
      assertThatThrownBy(() -> s.executeQuery("SELECT 1 / 0")).isInstanceOf(SQLException.class);
      assertThatThrownBy(() -> s.executeQuery("SELECT 1"))
          .isInstanceOfSatisfying(
              SQLException.class, e -> assertThat(e.getSQLState()).isEqualTo("25P02"));
      c.rollback();
      try (ResultSet rs = s.executeQuery("SELECT 1")) {
        assertThat(column(rs, 1)).containsExactly("1");
      }
    }
  }

  @Test
  void theSimpleQueryProtocolRunsScripts() throws Exception {
    try (Connection c = connect("preferQueryMode", "simple");
        Statement s = c.createStatement()) {
      s.execute(
          "INSERT INTO acct VALUES (3, 'cy', 1, 1, true); UPDATE acct SET owner = 'CY' WHERE"
              + " id = 3");
      try (ResultSet rs = s.executeQuery("SELECT owner FROM acct WHERE id = 3")) {
        assertThat(column(rs, 1)).containsExactly("CY");
      }
      try (ResultSet rs = s.executeQuery("SHOW transaction_isolation")) {
        assertThat(column(rs, 1)).containsExactly("repeatable read");
      }
    }
  }

  @Test
  void aSmallFetchSizeStreamsThroughSuspendedPortals() throws Exception {
    try (Connection c = connect();
        Statement s = c.createStatement()) {
      for (int i = 10; i < 60; i++) {
        s.execute("INSERT INTO acct VALUES (" + i + ", 'x', 0, 0, true)");
      }
      c.setAutoCommit(false); // the driver uses a cursor only inside a transaction
      s.setFetchSize(7);
      try (ResultSet rs = s.executeQuery("SELECT id FROM acct WHERE id >= 10 ORDER BY id")) {
        assertThat(column(rs, 1)).hasSize(50);
      }
      c.commit();
    }
  }

  @Test
  void generatedKeysComeFromReturning() throws Exception {
    try (Connection c = connect();
        PreparedStatement p =
            c.prepareStatement("INSERT INTO acct (id, owner) VALUES (?, ?) RETURNING id, owner")) {
      p.setInt(1, 77);
      p.setString(2, "ret");
      try (ResultSet rs = p.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(77);
        assertThat(rs.getString(2)).isEqualTo("ret");
      }
    }
  }
}

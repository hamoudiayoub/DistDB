package io.distdb.shell;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.pgwire.PgServer;
import io.distdb.sql.Engine;
import io.distdb.sql.testing.MemoryKvStore;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The shell against a real wire-protocol server (the SQL layer over an in-memory store). */
class ShellTest {

  private PgServer server;
  private String address;

  @BeforeEach
  void start() throws Exception {
    server = PgServer.start(new InetSocketAddress("127.0.0.1", 0), new Engine(new MemoryKvStore()));
    address = "127.0.0.1:" + server.port();
  }

  @AfterEach
  void stop() throws Exception {
    server.close();
  }

  private record Run(int status, String out, String err) {}

  private Run shell(String stdin, String... args) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    List<String> all = new ArrayList<>();
    all.add(address);
    all.addAll(List.of(args));
    int status =
        Shell.run(
            all,
            new ByteArrayInputStream(stdin.getBytes(StandardCharsets.UTF_8)),
            new PrintStream(out, true, StandardCharsets.UTF_8),
            new PrintStream(err, true, StandardCharsets.UTF_8));
    // println ends lines with the platform's separator; compare with \n everywhere.
    return new Run(
        status,
        out.toString(StandardCharsets.UTF_8).replace("\r\n", "\n"),
        err.toString(StandardCharsets.UTF_8).replace("\r\n", "\n"));
  }

  @Test
  void aCommandPrintsPsqlsPlainOutput() {
    Run r =
        shell(
            "",
            "-c",
            "CREATE TABLE t (id INT PRIMARY KEY, name TEXT); INSERT INTO t VALUES (1, 'a'), (2,"
                + " NULL); SELECT * FROM t ORDER BY id");
    assertThat(r.status()).isZero();
    assertThat(r.out())
        .isEqualTo(
            String.join(
                "\n",
                "CREATE TABLE",
                "INSERT 0 2",
                " id | name",
                "----+------",
                "  1 | a",
                "  2 |",
                "(2 rows)",
                "",
                ""));
  }

  @Test
  void aScriptRunsStatementsAndBackslashCommandsAndStopsAtTheFirstError(@TempDir Path dir)
      throws Exception {
    Path f = dir.resolve("s.sql");
    Files.writeString(
        f,
        """
        CREATE TABLE acct (id INT PRIMARY KEY, owner TEXT NOT NULL, balance INT);
        CREATE INDEX acct_owner ON acct (owner);
        INSERT INTO acct VALUES
          (1, 'ann', 100),  -- a comment; with a semicolon
          (2, 'bob', 50);
        \\dt
        \\d acct
        SELECT nope FROM acct;
        SELECT 'never reached';
        """);
    Run r = shell("", "-f", f.toString());
    assertThat(r.status()).isEqualTo(Shell.FAILED);
    assertThat(r.out())
        .contains("INSERT 0 2")
        .contains(" acct       |       3 |       2 |")
        .contains(" owner       | text      | f        |         | f")
        .contains(" acct_owner | owner   | f      | f       | public")
        .doesNotContain("never reached");
    assertThat(r.err()).contains("ERROR:  column \"nope\" does not exist");
  }

  @Test
  void pipedInputIsAScript() {
    Run r = shell("SELECT 1 + 1 AS two;\nSELECT 'x' AS s;\n");
    assertThat(r.status()).isZero();
    assertThat(r.out()).contains(" two\n-----\n   2\n(1 row)").contains(" s\n---\n x\n(1 row)");
  }

  @Test
  void prettyOutputFramesTablesAndKeepsGoingAfterErrors() {
    Run r =
        shell(
            "",
            "--pretty",
            "--no-color",
            "-c",
            "CREATE TABLE t (id INT PRIMARY KEY); SELECT * FROM missing; INSERT INTO t VALUES (7);"
                + " SELECT * FROM t");
    assertThat(r.out())
        .contains("✔ CREATE TABLE")
        .contains("✖ ERROR 42P01  relation \"missing\" does not exist")
        .contains("hint:   \\dt lists the tables")
        .contains("╭────╮\n│ id │\n├────┤\n│  7 │\n╰────╯\n1 row · ");
  }

  @Test
  void transactionsShowInTheClientsStatus() throws Exception {
    try (PgClient c =
        PgClient.connect(
            "127.0.0.1", server.port(), "distdb", "distdb", null, Duration.ofSeconds(5))) {
      assertThat(c.transactionStatus()).isEqualTo('I');
      c.query("BEGIN");
      assertThat(c.transactionStatus()).isEqualTo('T');
      List<PgClient.Outcome> o = c.query("SELECT * FROM missing");
      assertThat(o.getFirst()).isInstanceOf(PgClient.Failed.class);
      assertThat(((PgClient.Failed) o.getFirst()).error().code()).isEqualTo("42P01");
      assertThat(c.transactionStatus()).isEqualTo('E');
      c.query("ROLLBACK");
      assertThat(c.transactionStatus()).isEqualTo('I');
      assertThat(c.parameters()).containsKey("server_version");
    }
  }

  @Test
  void anUnreachableServerIsExplained() {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int status =
        Shell.run(
            List.of("127.0.0.1:1", "-c", "SELECT 1"),
            new ByteArrayInputStream(new byte[0]),
            new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
            new PrintStream(err, true, StandardCharsets.UTF_8));
    assertThat(status).isEqualTo(Shell.CANNOT_CONNECT);
    assertThat(err.toString(StandardCharsets.UTF_8)).contains("cannot connect to 127.0.0.1:1");
  }
}

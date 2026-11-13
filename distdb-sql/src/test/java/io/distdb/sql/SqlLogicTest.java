package io.distdb.sql;

import io.distdb.sql.testing.MemoryKvStore;
import io.distdb.sql.testing.SqlFuzz;
import io.distdb.sql.testing.SqlLogic;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * The sqllogictest files, run on the SQL layer over an in-memory store. Their expected results come
 * from PostgreSQL: {@code -Ddistdb.slt.complete=jdbc:postgresql://host:port/db?user=...} runs every
 * file against that PostgreSQL and rewrites the expected results (use a scratch database: it drops
 * and recreates schema {@code public}).
 */
class SqlLogicTest {

  @Test
  void everyFileMatchesPostgres() {
    int statements = 0;
    int queries = 0;
    for (String file : SqlLogic.FILES) {
      Engine engine = new Engine(new MemoryKvStore());
      SqlLogic.Counts c = SqlLogic.run(file, SqlLogic.load(file), SqlLogic.of(engine.session()));
      statements += c.statements();
      queries += c.queries();
    }
    System.out.println(
        "sqllogictest: "
            + SqlLogic.FILES.size()
            + " files, "
            + statements
            + " statements, "
            + queries
            + " queries, all as PostgreSQL answered");
  }

  @Test
  @EnabledIfSystemProperty(named = "distdb.slt.complete", matches = ".+")
  void recordExpectedResultsFromPostgres() throws Exception {
    String url = System.getProperty("distdb.slt.complete");
    Path dir = Path.of("src/testFixtures/resources/slt");
    // -Ddistdb.slt.fuzz=N writes fuzz.test afresh: N random queries (seed 1), for PostgreSQL to
    // answer.
    Integer fuzz = Integer.getInteger("distdb.slt.fuzz");
    if (fuzz != null) {
      Files.writeString(
          dir.resolve("fuzz.test"), SqlFuzz.generate(1, fuzz), StandardCharsets.UTF_8);
    }
    try (Connection pg = DriverManager.getConnection(url)) {
      for (String file : SqlLogic.FILES) {
        try (Statement s = pg.createStatement()) {
          s.execute("DROP SCHEMA IF EXISTS public CASCADE");
          s.execute("CREATE SCHEMA public");
        }
        Path path = dir.resolve(file);
        String text = Files.readString(path, StandardCharsets.UTF_8);
        String completed = SqlLogic.complete(file, text, jdbc(pg));
        Files.writeString(path, completed, StandardCharsets.UTF_8);
        System.out.println(
            "recorded "
                + file
                + " from "
                + pg.getMetaData().getDatabaseProductName()
                + " "
                + pg.getMetaData().getDatabaseProductVersion());
      }
    }
  }

  /** PostgreSQL through JDBC, in autocommit mode (BEGIN and COMMIT go through as statements). */
  static SqlLogic.Database jdbc(Connection c) {
    return new SqlLogic.Database() {
      @Override
      public void statement(String sql) {
        try (Statement s = c.createStatement()) {
          s.execute(sql);
        } catch (SQLException e) {
          throw new SqlLogic.Failure(e.getSQLState(), e.getMessage());
        }
      }

      @Override
      public List<List<String>> query(String sql) {
        try (Statement s = c.createStatement();
            ResultSet rs = s.executeQuery(sql)) {
          int n = rs.getMetaData().getColumnCount();
          List<List<String>> rows = new ArrayList<>();
          while (rs.next()) {
            List<String> row = new ArrayList<>();
            for (int k = 1; k <= n; k++) {
              row.add(rs.getString(k));
            }
            rows.add(row);
          }
          return rows;
        } catch (SQLException e) {
          throw new SqlLogic.Failure(e.getSQLState(), e.getMessage());
        }
      }
    };
  }
}

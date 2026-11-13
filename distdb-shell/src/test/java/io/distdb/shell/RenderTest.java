package io.distdb.shell;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The shell's layouts, compared as plain text (colors stripped). */
class RenderTest {

  private static PgClient.Result result(List<PgClient.Column> cols, String[]... rows) {
    return new PgClient.Result("SELECT " + rows.length, cols, List.of(rows));
  }

  private static final List<PgClient.Column> ACCT =
      List.of(
          new PgClient.Column("id", 20),
          new PgClient.Column("owner", 25),
          new PgClient.Column("balance", 1700));

  private static List<String> render(PgClient.Result r, int width, Render.Expanded x) {
    List<String> out = new ArrayList<>();
    for (String line : Render.rows(r, new Render.Options(Theme.COLOR, width, x, true))) {
      out.add(Theme.strip(line));
    }
    return out;
  }

  @Test
  void aTableIsFramedWithNumbersOnTheRightAndNullsShown() {
    List<String> lines =
        render(
            result(ACCT, new String[] {"1", "ann", "100.50"}, new String[] {"22", "bob", null}),
            80,
            Render.Expanded.AUTO);
    assertThat(lines)
        .containsExactly(
            "╭────┬───────┬─────────╮",
            "│ id │ owner │ balance │",
            "├────┼───────┼─────────┤",
            "│  1 │ ann   │  100.50 │",
            "│ 22 │ bob   │    NULL │",
            "╰────┴───────┴─────────╯");
  }

  @Test
  void colorsNeverChangeTheLayout() {
    PgClient.Result r = result(ACCT, new String[] {"1", "ann", null});
    List<String> colored =
        Render.rows(r, new Render.Options(Theme.COLOR, 80, Render.Expanded.OFF, true));
    List<String> plain =
        Render.rows(r, new Render.Options(Theme.PLAIN, 80, Render.Expanded.OFF, true));
    assertThat(colored.getFirst()).contains("\u001b[");
    assertThat(colored.stream().map(Theme::strip).toList()).isEqualTo(plain);
  }

  @Test
  void aLongValueIsCutToTheTerminal() {
    String note = "x".repeat(200);
    List<String> lines =
        render(
            result(
                List.of(new PgClient.Column("id", 20), new PgClient.Column("note", 25)),
                new String[] {"1", note}),
            40,
            Render.Expanded.OFF);
    for (String l : lines) {
      assertThat(Text.width(l)).isLessThanOrEqualTo(40);
    }
    assertThat(lines.get(3)).contains("…");
  }

  @Test
  void aTooWideTableBecomesRecords() {
    List<PgClient.Column> cols = new ArrayList<>();
    String[] row = new String[12];
    for (int i = 0; i < 12; i++) {
      cols.add(new PgClient.Column("column_" + i, 25));
      row[i] = "value " + i;
    }
    List<String> lines = render(result(cols, row), 60, Render.Expanded.AUTO);
    assertThat(lines.getFirst()).startsWith("─ row 1 ");
    assertThat(lines.get(1)).isEqualTo(" column_0  │ value 0");
    assertThat(lines).hasSize(13);
  }

  @Test
  void scriptsGetPsqlsAlignedFormat() {
    List<String> lines =
        Render.rows(
            result(ACCT, new String[] {"1", "ann", "100.50"}, new String[] {"22", "bob", null}),
            new Render.Options(Theme.PLAIN, 80, Render.Expanded.AUTO, false));
    assertThat(lines)
        .containsExactly(
            " id | owner | balance",
            "----+-------+---------",
            "  1 | ann   |  100.50",
            " 22 | bob   |");
    assertThat(Render.rowCount(1)).isEqualTo("(1 row)");
    assertThat(Render.rowCount(2)).isEqualTo("(2 rows)");
  }

  @Test
  void aPlanIsATreeWithEachOperatorsShareOfTheTime() {
    List<String> plan =
        Render.plan(
            List.of(
                "project a  (rows=2 time=10.000 ms)",
                "  -> hash join on a.x = b.y  (rows=2 time=9.000 ms)",
                "    -> remote scan a filter v > 1  (rows=5 time=6.000 ms requests=2 nodes=[1, 2]"
                    + " read on ranges=100 rows received=5 rows, 80 bytes)",
                "    -> point get b on b_pkey (1)  (rows=1 time=1.000 ms)",
                "SELECT 2 in 10.500 ms"),
            Theme.PLAIN);
    assertThat(plan.get(0)).startsWith("project a  (rows=2 time=10.000 ms)").endsWith(" 10%");
    assertThat(plan.get(1)).startsWith("└─ hash join on a.x = b.y").endsWith(" 20%");
    assertThat(plan.get(2)).startsWith("   ├─ remote scan a filter v > 1").endsWith(" 60%");
    assertThat(plan.get(3)).startsWith("   └─ point get b on b_pkey (1)").endsWith(" 10%");
    assertThat(plan.get(4)).isEqualTo("⏱  SELECT 2 in 10.500 ms");
  }

  @Test
  void aPlanTooWideForTheTerminalPutsItsNumbersUnderEachOperator() {
    List<String> plan =
        Render.plan(
            List.of(
                "merge aggregate  (rows=2 time=10.000 ms)",
                "  -> remote scan sales filter qty > 6 partial aggregate by region: sum(qty)  (rows=2"
                    + " time=9.000 ms requests=4 nodes=[1, 2, 3] read on ranges=20000 rows received=8"
                    + " rows, 160 bytes)"),
            Theme.PLAIN,
            80);
    for (String line : plan) {
      assertThat(Text.width(line)).isLessThanOrEqualTo(110);
    }
    assertThat(plan.get(0)).startsWith("merge aggregate  (rows=2 time=10.000 ms)");
    assertThat(plan.get(1))
        .startsWith("└─ remote scan sales filter qty > 6 partial aggregate by region: sum(qty)")
        .endsWith(" 90%");
    assertThat(plan.get(2)).startsWith("      (rows=2 time=9.000 ms requests=4");
  }

  @Test
  void barsFillByEighths() {
    assertThat(Render.bar(0)).isEmpty();
    assertThat(Render.bar(1)).isEqualTo("████████");
    assertThat(Render.bar(0.5)).isEqualTo("████");
    assertThat(Render.bar(1.0 / 64)).isEqualTo("▏");
  }

  @Test
  void wideCharactersCountTwoColumns() {
    assertThat(Text.width("東京")).isEqualTo(4);
    assertThat(Text.pad("東京", 6, false)).isEqualTo("東京  ");
    assertThat(Text.cut("東京大阪", 5)).isEqualTo("東京…");
    assertThat(Text.oneLine("a\nb\u001b[31m")).isEqualTo("a↵b?[31m");
  }
}

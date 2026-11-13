package io.distdb.sql.exec;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.sql.plan.Scalar;
import io.distdb.sql.types.Type;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The gateway's merge of the partial aggregates that ranges send back. */
class FragmentMergeTest {

  private static final Scalar ARG = new Scalar.ColRef(0, Type.INT);

  private static Scalar.AggCall agg(String function, Type type) {
    return new Scalar.AggCall(function, ARG, false, type);
  }

  @Test
  void anAverageMergesFromSumsAndCountsNotFromAverages() {
    // Range A saw {10}, range B saw {1, 2, 3}: the average is 16 / 4 = 4. Averaging the ranges'
    // averages would answer (10 + 2) / 2 = 6.
    List<Object[]> out =
        Fragment.merge(
            0,
            List.of(agg("avg", Type.NUMERIC)),
            List.of(
                new Object[] {BigDecimal.valueOf(10), 1L},
                new Object[] {BigDecimal.valueOf(6), 3L}));
    assertThat(out).hasSize(1);
    assertThat((BigDecimal) out.get(0)[0]).isEqualByComparingTo("4");
  }

  @Test
  void groupsMeetAcrossRangesAndIntegerSumsStayIntegers() {
    List<Scalar.AggCall> aggs =
        List.of(agg("count", Type.INT), agg("sum", Type.INT), agg("max", Type.INT));
    List<Object[]> out =
        Fragment.merge(
            1,
            aggs,
            List.of(
                new Object[] {"a", 2L, BigDecimal.valueOf(5), 4L},
                new Object[] {"b", 1L, BigDecimal.valueOf(7), 7L},
                new Object[] {"a", 3L, BigDecimal.valueOf(10), 9L},
                new Object[] {"b", 1L, null, null}));
    assertThat(out).hasSize(2);
    assertThat(out.get(0)).containsExactly("a", 5L, 15L, 9L);
    assertThat(out.get(1)).containsExactly("b", 2L, 7L, 7L);
  }

  @Test
  void withNoRowsAnywhereCountIsZeroAndTheRestNull() {
    List<Object[]> out =
        Fragment.merge(
            0,
            List.of(agg("count", Type.INT), agg("sum", Type.INT), agg("avg", Type.NUMERIC)),
            List.of());
    assertThat(out).hasSize(1);
    assertThat(out.get(0)).containsExactly(0L, null, null);
  }

  @Test
  void aRangeWithOnlyNullsDoesNotMaskAnother() {
    List<Object[]> out =
        Fragment.merge(
            0,
            List.of(agg("min", Type.INT), agg("sum", Type.INT)),
            List.of(new Object[] {null, null}, new Object[] {3L, BigDecimal.valueOf(3)}));
    assertThat(out.get(0)).containsExactly(3L, 3L);
  }
}

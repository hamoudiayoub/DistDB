package io.distdb.sim;

import static io.distdb.sim.Linearizability.Kind.DELETE;
import static io.distdb.sim.Linearizability.Kind.GET;
import static io.distdb.sim.Linearizability.Kind.PUT;
import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.env.RandomSource;
import io.distdb.env.SplittableRandomSource;
import io.distdb.sim.Linearizability.Op;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class LinearizabilityTest {

  private static final long NEVER = Long.MAX_VALUE;

  @Test
  void aSequentialHistoryThatFollowsTheRegisterIsLinearizable() {
    assertThat(
            ok(
                op(1, PUT, "1", 0, 10),
                op(2, GET, "1", 20, 30),
                op(1, DELETE, null, 40, 50),
                op(2, GET, null, 60, 70)))
        .isTrue();
  }

  @Test
  void aReadThatMissesACompletedWriteIsNot() {
    assertThat(ok(op(1, PUT, "1", 0, 10), op(2, GET, null, 20, 30))).isFalse();
  }

  @Test
  void concurrentOperationsMayTakeEffectInEitherOrder() {
    assertThat(ok(op(1, PUT, "1", 0, 10), op(2, GET, "1", 5, 15))).isTrue();
    assertThat(ok(op(1, PUT, "1", 0, 10), op(2, GET, null, 5, 15))).isTrue();
  }

  @Test
  void readsMustNotSeeWritesGoBackInTime() {
    // Once a reader has seen 2, nobody may later read the older 1.
    assertThat(
            ok(
                op(1, PUT, "1", 0, 10),
                op(1, PUT, "2", 20, 30),
                op(2, GET, "2", 40, 50),
                op(3, GET, "1", 60, 70)))
        .isFalse();
    // While the second write is still running, a read may see either value...
    assertThat(ok(op(1, PUT, "1", 0, 10), op(1, PUT, "2", 20, 100), op(2, GET, "1", 40, 50)))
        .isTrue();
    // ...but two reads in sequence cannot see the new value and then the old.
    assertThat(
            ok(
                op(1, PUT, "1", 0, 10),
                op(1, PUT, "2", 20, 100),
                op(2, GET, "2", 40, 50),
                op(3, GET, "1", 60, 70)))
        .isFalse();
  }

  @Test
  void aWriteWithAnUnknownOutcomeMayOrMayNotHaveHappened() {
    assertThat(ok(op(1, PUT, "1", 0, NEVER), op(2, GET, "1", 20, 30))).isTrue();
    assertThat(ok(op(1, PUT, "1", 0, NEVER), op(2, GET, null, 20, 30))).isTrue();
    // But not both ways: once it was seen, it cannot be undone.
    assertThat(ok(op(1, PUT, "1", 0, NEVER), op(2, GET, "1", 20, 30), op(2, GET, null, 40, 50)))
        .isFalse();
  }

  @Test
  void aValueNobodyWroteIsNeverLinearizable() {
    assertThat(ok(op(1, PUT, "1", 0, 10), op(2, GET, "7", 20, 30))).isFalse();
  }

  @Test
  void keysAreCheckedIndependently() {
    List<Op> h = new ArrayList<>();
    h.add(new Op(1, "a", PUT, "1", 0, 10));
    h.add(new Op(2, "b", GET, null, 20, 30));
    h.add(new Op(2, "a", GET, "1", 40, 50));
    assertThat(Linearizability.check(h).linearizable()).isTrue();
    h.add(new Op(3, "b", GET, "1", 60, 70)); // b was never written
    Linearizability.Result r = Linearizability.check(h);
    assertThat(r.linearizable()).isFalse();
    assertThat(r.key()).isEqualTo("b");
  }

  @Test
  void aLongConcurrentHistoryFromARealRegisterIsCheckedQuickly() {
    // Simulate a real register: each operation takes effect at a random instant inside its
    // interval, so the history is linearizable by construction, with plenty of overlap.
    RandomSource r = SplittableRandomSource.seeded(3);
    List<Op> h = new ArrayList<>();
    String value = null;
    long t = 0;
    int clients = 5;
    long[] busyUntil = new long[clients];
    for (int i = 0; i < 2_000; i++) {
      t += r.nextInt(1, 4);
      int c = r.nextInt(clients);
      long call = Math.max(t, busyUntil[c]);
      long effect = call + r.nextInt(0, 10);
      long ret = effect + r.nextInt(0, 10);
      busyUntil[c] = ret + 1;
      t = effect; // effects happen in order of t, so the register below is the truth
      switch (r.nextInt(3)) {
        case 0 -> {
          value = "v" + i;
          h.add(new Op(c, "k", PUT, value, call, ret));
        }
        case 1 -> {
          value = null;
          h.add(new Op(c, "k", DELETE, null, call, ret));
        }
        default -> h.add(new Op(c, "k", GET, value, call, ret));
      }
    }
    long start = System.nanoTime();
    assertThat(Linearizability.check(h).linearizable()).isTrue();
    assertThat(System.nanoTime() - start).isLessThan(10_000_000_000L);
  }

  private static boolean ok(Op... ops) {
    return Linearizability.check(List.of(ops)).linearizable();
  }

  private static Op op(int client, Linearizability.Kind kind, String value, long call, long ret) {
    return new Op(client, "k", kind, value, call, ret);
  }
}

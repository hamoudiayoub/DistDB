package io.distdb.storage.mvcc;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.common.Bytes;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.storage.WriteBatch;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;

class MvccKeyTest {

  @Property(tries = 3000)
  void ordersByUserKeyThenNewestTimestampFirst(
      @ForAll("keys") byte[] k1,
      @ForAll("timestamps") long t1,
      @ForAll("keys") byte[] k2,
      @ForAll("timestamps") long t2) {
    int byKey = Arrays.compareUnsigned(k1, k2);
    int expected = byKey != 0 ? byKey : Long.compare(t2, t1);
    int actual = Bytes.compare(MvccKey.encode(k1, t1), MvccKey.encode(k2, t2));
    assertThat(Integer.signum(actual)).isEqualTo(Integer.signum(expected));
  }

  @Property
  void versionsOfAKeyShareAPrefixNoOtherKeyHas(
      @ForAll("keys") byte[] k1, @ForAll("timestamps") long t, @ForAll("keys") byte[] k2) {
    assertThat(Bytes.startsWith(MvccKey.encode(k1, t), MvccKey.prefix(k1))).isTrue();
    if (!Arrays.equals(k1, k2)) {
      assertThat(Bytes.startsWith(MvccKey.encode(k2, t), MvccKey.prefix(k1))).isFalse();
    }
    assertThat(MvccKey.userKeyOf(MvccKey.encode(k1, t))).isEqualTo(k1);
    assertThat(MvccKey.timestampOf(MvccKey.encode(k1, t))).isEqualTo(t);
  }

  @Provide
  Arbitrary<byte[]> keys() {
    return Arbitraries.of((byte) 0, (byte) 1, (byte) 0xFF).array(byte[].class).ofMaxSize(3);
  }

  @Provide
  Arbitrary<Long> timestamps() {
    return Arbitraries.oneOf(
        Arbitraries.longs().between(0, 5),
        Arbitraries.of(0L, Long.MAX_VALUE),
        Arbitraries.longs().greaterOrEqual(0));
  }

  @Test
  void readAtReturnsTheNewestVersionAtOrBeforeTheTimestamp() {
    SimFileSystem fs = new SimFileSystem(SplittableRandomSource.seeded(1));
    try (LsmEngine db =
        LsmEngine.open(
            fs, new SimScheduler(new SimClock()), Path.of("db"), EngineOptions.defaults())) {
      byte[] k = "account/7".getBytes(StandardCharsets.UTF_8);
      byte[] other = "account/70".getBytes(StandardCharsets.UTF_8);
      db.write(
          new WriteBatch()
              .put(MvccKey.encode(k, 10), "v10".getBytes(StandardCharsets.UTF_8))
              .put(MvccKey.encode(k, 20), "v20".getBytes(StandardCharsets.UTF_8))
              .put(MvccKey.encode(other, 5), "other".getBytes(StandardCharsets.UTF_8)),
          true);
      assertThat(MvccKey.readAt(db, k, 9)).isEmpty();
      assertThat(MvccKey.readAt(db, k, 10))
          .hasValueSatisfying(v -> assertThat(v.timestamp()).isEqualTo(10));
      assertThat(MvccKey.readAt(db, k, 19))
          .hasValueSatisfying(v -> assertThat(v.timestamp()).isEqualTo(10));
      assertThat(MvccKey.readAt(db, k, 99))
          .hasValueSatisfying(
              v -> assertThat(new String(v.value(), StandardCharsets.UTF_8)).isEqualTo("v20"));
      assertThat(MvccKey.readAt(db, other, 4))
          .as("does not leak into the neighbouring key")
          .isEmpty();
    }
  }
}

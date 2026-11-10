package io.distdb.storage.lsm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.env.FileHandle;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.storage.CorruptionException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/** Unit tests for the pieces the engine is built from. */
class ComponentsTest {

  private static final Path DIR = Path.of("t");

  private static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static SimFileSystem fs() throws IOException {
    SimFileSystem fs = new SimFileSystem(SplittableRandomSource.seeded(1));
    fs.createDirectories(DIR);
    return fs;
  }

  // ---- internal keys ----

  @Test
  void internalKeysOrderByUserKeyThenNewestFirst() {
    byte[] a5 = InternalKey.encode(b("a"), 5, InternalKey.PUT);
    byte[] a9 = InternalKey.encode(b("a"), 9, InternalKey.DELETE);
    byte[] ab1 = InternalKey.encode(b("ab"), 1, InternalKey.PUT);
    List<byte[]> keys = new ArrayList<>(List.of(ab1, a5, a9));
    keys.sort(InternalKey.COMPARATOR);
    assertThat(keys).containsExactly(a9, a5, ab1);
    assertThat(InternalKey.sequence(a9)).isEqualTo(9);
    assertThat(InternalKey.kind(a9)).isEqualTo(InternalKey.DELETE);
    assertThat(InternalKey.userKey(ab1)).isEqualTo(b("ab"));
    // Seeking at sequence 7 skips the version at 9 and lands on the one at 5.
    assertThat(InternalKey.COMPARATOR.compare(InternalKey.seekKey(b("a"), 7), a9)).isPositive();
    assertThat(InternalKey.COMPARATOR.compare(InternalKey.seekKey(b("a"), 7), a5)).isNegative();
  }

  // ---- blocks ----

  @Test
  void blockRoundTripsAndSeeks() {
    BlockBuilder builder = new BlockBuilder(4);
    TreeMap<byte[], byte[]> expected = new TreeMap<>(InternalKey.COMPARATOR);
    for (int i = 0; i < 200; i++) {
      byte[] k = InternalKey.encode(b("key%04d".formatted(i * 2)), i, InternalKey.PUT);
      expected.put(k, b("v" + i));
    }
    expected.forEach(builder::add);
    Block block = new Block(builder.finish());

    InternalIterator it = block.iterator(InternalKey.COMPARATOR);
    List<byte[]> seen = new ArrayList<>();
    for (it.seekToFirst(); it.valid(); it.next()) {
      seen.add(it.key());
      assertThat(it.value()).isEqualTo(expected.get(it.key()));
    }
    assertThat(seen).hasSize(200);

    for (byte[] target : List.of(b("key0000"), b("key0101"), b("key0398"), b("a"), b("zzz"))) {
      byte[] ik = InternalKey.seekKey(target, InternalKey.MAX_SEQUENCE);
      it.seek(ik);
      byte[] want = expected.ceilingKey(ik);
      if (want == null) {
        assertThat(it.valid()).isFalse();
      } else {
        assertThat(it.key()).isEqualTo(want);
      }
    }
  }

  // ---- bloom filter ----

  @Test
  void bloomFilterHasNoFalseNegativesAndFewFalsePositives() {
    int n = 10_000;
    long[] hashes = new long[n];
    for (int i = 0; i < n; i++) {
      byte[] k = b("present-" + i);
      hashes[i] = BloomFilter.hash(k, 0, k.length);
    }
    byte[] filter = BloomFilter.build(hashes, n, 10);
    for (long h : hashes) {
      assertThat(BloomFilter.mayContain(filter, h)).isTrue();
    }
    int falsePositives = 0;
    for (int i = 0; i < n; i++) {
      byte[] k = b("absent-" + i);
      if (BloomFilter.mayContain(filter, BloomFilter.hash(k, 0, k.length))) {
        falsePositives++;
      }
    }
    assertThat(falsePositives / (double) n).as("false positive rate").isLessThan(0.02);
  }

  // ---- tables ----

  @Test
  void tableRoundTripsAndAnswersPointLookups() throws IOException {
    SimFileSystem fs = fs();
    EngineOptions opts = new EngineOptions(1 << 20, 128, 10, 1 << 20, 4, 8, 1 << 20, 10);
    Path p = DIR.resolve("000001.sst");
    try (FileHandle f = fs.create(p)) {
      TableBuilder t = new TableBuilder(f, opts);
      for (int i = 0; i < 500; i++) {
        byte[] uk = b("k%04d".formatted(i));
        t.add(InternalKey.encode(uk, 1000 + i, InternalKey.PUT), b("new" + i));
        if (i % 3 == 0) {
          t.add(InternalKey.encode(uk, i, InternalKey.DELETE), new byte[0]);
        }
      }
      t.finish();
      f.sync();
    }
    try (TableReader r = TableReader.open(fs.openReadOnly(p), 1)) {
      assertThat(r.get(b("k0007"), Long.MAX_VALUE >>> 8).value()).isEqualTo(b("new7"));
      assertThat(r.get(b("k0006"), 500).deleted()).as("older tombstone at seq 6").isTrue();
      assertThat(r.get(b("k0007"), 500)).as("nothing visible that old").isNull();
      assertThat(r.get(b("nope"), InternalKey.MAX_SEQUENCE)).isNull();
      int count = 0;
      InternalIterator it = r.iterator();
      for (it.seekToFirst(); it.valid(); it.next()) {
        count++;
      }
      assertThat(count).isEqualTo(500 + 167);
    }
  }

  @Test
  void corruptTableBlockIsDetected() throws IOException {
    SimFileSystem fs = fs();
    EngineOptions opts = new EngineOptions(1 << 20, 128, 10, 1 << 20, 4, 8, 1 << 20, 10);
    Path p = DIR.resolve("000002.sst");
    try (FileHandle f = fs.create(p)) {
      TableBuilder t = new TableBuilder(f, opts);
      for (int i = 0; i < 100; i++) {
        t.add(InternalKey.encode(b("k%04d".formatted(i)), i + 1, InternalKey.PUT), b("v"));
      }
      t.finish();
      f.write(ByteBuffer.wrap(new byte[] {(byte) 0xAB}), 10); // flip a byte in block 1
    }
    try (TableReader r = TableReader.open(fs.openReadOnly(p), 2)) {
      InternalIterator it = r.iterator();
      assertThatThrownBy(it::seekToFirst).isInstanceOf(CorruptionException.class);
    }
  }

  // ---- logs ----

  @Test
  void logReaderStopsAtTheFirstDamagedRecord() throws IOException {
    SimFileSystem fs = fs();
    Path p = DIR.resolve("000003.log");
    try (FileHandle f = fs.create(p)) {
      LogWriter w = new LogWriter(f, 0);
      w.add(b("one"));
      w.add(b("two"));
      long damageAt = w.size() + LogWriter.HEADER_BYTES + 1;
      w.add(b("three"));
      w.add(b("four"));
      f.write(ByteBuffer.wrap(b("X")), damageAt); // corrupt "three"
    }
    try (FileHandle f = fs.openReadOnly(p)) {
      LogReader.Result r = LogReader.readAll(f);
      assertThat(r.records()).map(String::new).containsExactly("one", "two");
      assertThat(r.damagedTail()).isTrue();
    }
  }

  @Test
  void versionEditRoundTrips() {
    VersionEdit e =
        new VersionEdit()
            .setLogNumber(7)
            .setNextFileNumber(12)
            .setLastSequence(99)
            .deleteFile(1, 4)
            .addFile(
                2,
                new FileMetadata(
                    11,
                    4096,
                    InternalKey.encode(b("a"), 1, InternalKey.PUT),
                    InternalKey.encode(b("m"), 2, InternalKey.DELETE)));
    VersionEdit d = VersionEdit.decode(e.encode());
    assertThat(d.logNumber()).isEqualTo(7);
    assertThat(d.nextFileNumber()).isEqualTo(12);
    assertThat(d.lastSequence()).isEqualTo(99);
    assertThat(d.deleted()).containsExactly(new VersionEdit.DeletedFile(1, 4));
    assertThat(d.added()).containsExactlyElementsOf(e.added());
  }

  @Test
  void versionRejectsOverlappingFilesInADeepLevel() {
    FileMetadata f1 =
        new FileMetadata(
            1,
            10,
            InternalKey.encode(b("a"), 1, InternalKey.PUT),
            InternalKey.encode(b("m"), 1, InternalKey.PUT));
    FileMetadata f2 =
        new FileMetadata(
            2,
            10,
            InternalKey.encode(b("k"), 2, InternalKey.PUT),
            InternalKey.encode(b("z"), 2, InternalKey.PUT));
    Version v = Version.empty().apply(new VersionEdit().addFile(0, f1).addFile(0, f2));
    assertThat(v.files(0)).containsExactly(f2, f1); // level 0: newest first, overlap allowed
    assertThatThrownBy(() -> Version.empty().apply(new VersionEdit().addFile(1, f1).addFile(1, f2)))
        .isInstanceOf(CorruptionException.class)
        .hasMessageContaining("overlapping");
  }
}

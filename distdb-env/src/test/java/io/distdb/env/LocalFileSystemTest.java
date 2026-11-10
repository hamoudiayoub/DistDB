package io.distdb.env;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFileSystemTest {

  private final FileSystem fs = LocalFileSystem.INSTANCE;

  @TempDir Path dir;

  private static ByteBuffer utf8(String s) {
    return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
  }

  private static String readAll(FileHandle f) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate((int) f.size());
    f.readFully(buf, 0);
    return new String(buf.array(), StandardCharsets.UTF_8);
  }

  @Test
  void writeSyncAndReadBack() throws IOException {
    Path p = dir.resolve("wal-000001.log");
    try (FileHandle f = fs.create(p)) {
      f.write(utf8("hello "), 0);
      f.write(utf8("world"), 6);
      f.sync();
      assertThat(f.size()).isEqualTo(11);
      assertThat(readAll(f)).isEqualTo("hello world");
    }
    try (FileHandle f = fs.openReadOnly(p)) {
      assertThat(readAll(f)).isEqualTo("hello world");
    }
  }

  @Test
  void positionalReadPastEndReturnsMinusOne() throws IOException {
    try (FileHandle f = fs.create(dir.resolve("f"))) {
      f.write(utf8("abc"), 0);
      assertThat(f.read(ByteBuffer.allocate(4), 3)).isEqualTo(-1);
      assertThatThrownBy(() -> f.readFully(ByteBuffer.allocate(4), 1))
          .isInstanceOf(EOFException.class);
    }
  }

  @Test
  void createFailsIfTheFileExists() throws IOException {
    Path p = dir.resolve("f");
    fs.create(p).close();
    assertThatThrownBy(() -> fs.create(p)).isInstanceOf(FileAlreadyExistsException.class);
  }

  @Test
  void openFailsIfTheFileIsMissing() {
    assertThatThrownBy(() -> fs.open(dir.resolve("missing")))
        .isInstanceOf(NoSuchFileException.class);
  }

  @Test
  void readOnlyHandleRejectsWrites() throws IOException {
    Path p = dir.resolve("f");
    fs.create(p).close();
    try (FileHandle f = fs.openReadOnly(p)) {
      assertThatThrownBy(() -> f.write(utf8("x"), 0))
          .isInstanceOf(NonWritableChannelException.class);
    }
  }

  @Test
  void openExistingAndTruncateATornTail() throws IOException {
    Path p = dir.resolve("wal");
    try (FileHandle f = fs.create(p)) {
      f.write(utf8("record1record2-torn"), 0);
    }
    try (FileHandle f = fs.open(p)) {
      f.truncate(14);
      f.sync();
      assertThat(readAll(f)).isEqualTo("record1record2");
    }
  }

  @Test
  void renameReplacesTheTargetAtomically() throws IOException {
    Path current = dir.resolve("CURRENT");
    Path tmp = dir.resolve("CURRENT.tmp");
    try (FileHandle f = fs.create(current)) {
      f.write(utf8("MANIFEST-1"), 0);
    }
    try (FileHandle f = fs.create(tmp)) {
      f.write(utf8("MANIFEST-2"), 0);
      f.sync();
    }
    fs.rename(tmp, current);
    fs.syncDirectory(dir);
    assertThat(fs.exists(tmp)).isFalse();
    try (FileHandle f = fs.openReadOnly(current)) {
      assertThat(readAll(f)).isEqualTo("MANIFEST-2");
    }
  }

  @Test
  void listIsSortedAndDeleteIsIdempotent() throws IOException {
    Path sub = dir.resolve("a/b");
    fs.createDirectories(sub);
    fs.createDirectories(sub);
    for (String name : new String[] {"000003.sst", "000001.sst", "000002.sst"}) {
      fs.create(sub.resolve(name)).close();
    }
    assertThat(fs.list(sub))
        .containsExactly(
            sub.resolve("000001.sst"), sub.resolve("000002.sst"), sub.resolve("000003.sst"));
    assertThat(fs.deleteIfExists(sub.resolve("000002.sst"))).isTrue();
    assertThat(fs.deleteIfExists(sub.resolve("000002.sst"))).isFalse();
    assertThat(fs.list(sub)).hasSize(2);
  }
}

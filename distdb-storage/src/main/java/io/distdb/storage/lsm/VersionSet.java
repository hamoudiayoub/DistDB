package io.distdb.storage.lsm;

import io.distdb.env.FileHandle;
import io.distdb.env.FileSystem;
import io.distdb.storage.CorruptionException;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.OptionalLong;
import java.util.function.Consumer;

/**
 * Owns the manifest: the durable history of file-layout changes, and the counters that must survive
 * restarts (next file number, oldest needed log, last sequence number).
 *
 * <p>Crash safety rests on one rule: a change to the file layout takes effect only once its edit is
 * synced to the manifest. Files a crashed flush or compaction wrote but never recorded are simply
 * unreferenced, and recovery deletes them.
 */
final class VersionSet implements Closeable {

  private final FileSystem fs;
  private final Path dir;

  private Version current = Version.empty();
  private long nextFileNumber = 1;
  private long logNumber;
  private long lastSequence;
  private long manifestNumber;
  private LogWriter manifest;

  VersionSet(FileSystem fs, Path dir) {
    this.fs = fs;
    this.dir = dir;
  }

  synchronized Version current() {
    return current;
  }

  synchronized long newFileNumber() {
    return nextFileNumber++;
  }

  /** Makes sure future file numbers are above {@code number}, e.g. leftovers found on disk. */
  synchronized void markFileNumberUsed(long number) {
    nextFileNumber = Math.max(nextFileNumber, number + 1);
  }

  synchronized long logNumber() {
    return logNumber;
  }

  synchronized long lastSequence() {
    return lastSequence;
  }

  synchronized long manifestNumber() {
    return manifestNumber;
  }

  /** Reads CURRENT and replays the manifest it names. */
  synchronized void recover() throws IOException {
    String name = readCurrentFile().strip();
    OptionalLong number = LsmFiles.number(name);
    if (LsmFiles.kind(name) != LsmFiles.Kind.MANIFEST || number.isEmpty()) {
      throw new CorruptionException("CURRENT names " + name);
    }
    Version v = Version.empty();
    boolean sawLog = false;
    boolean sawNext = false;
    try (FileHandle f = fs.openReadOnly(dir.resolve(name))) {
      for (byte[] record : LogReader.readAll(f).records()) {
        VersionEdit e = VersionEdit.decode(record);
        v = v.apply(e);
        if (e.logNumber() != null) {
          logNumber = e.logNumber();
          sawLog = true;
        }
        if (e.nextFileNumber() != null) {
          nextFileNumber = Math.max(nextFileNumber, e.nextFileNumber());
          sawNext = true;
        }
        if (e.lastSequence() != null) {
          lastSequence = Math.max(lastSequence, e.lastSequence());
        }
      }
    }
    if (!sawLog || !sawNext) {
      throw new CorruptionException("manifest " + name + " lacks log or file number");
    }
    current = v;
    manifestNumber = number.getAsLong();
    markFileNumberUsed(manifestNumber);
  }

  /**
   * Records {@code edit} durably, then installs the resulting version. {@code install} runs inside
   * this object's lock, so versions are installed in exactly the order they were logged.
   */
  synchronized void logAndApply(VersionEdit edit, Consumer<Version> install) throws IOException {
    if (edit.logNumber() == null) {
      edit.setLogNumber(logNumber);
    }
    edit.setNextFileNumber(nextFileNumber);
    if (edit.lastSequence() == null || edit.lastSequence() < lastSequence) {
      edit.setLastSequence(lastSequence);
    }
    Version next = current.apply(edit);
    manifest.add(edit.encode());
    manifest.sync();
    logNumber = edit.logNumber();
    lastSequence = edit.lastSequence();
    current = next;
    install.accept(next);
  }

  /**
   * Starts a fresh manifest holding the whole current state as one edit, then points CURRENT at it.
   * Used on every open, so the manifest never grows without bound across restarts.
   */
  synchronized void writeSnapshotManifest() throws IOException {
    long number = nextFileNumber++;
    VersionEdit snapshot =
        new VersionEdit()
            .setLogNumber(logNumber)
            .setNextFileNumber(nextFileNumber)
            .setLastSequence(lastSequence);
    for (int level = 0; level < Version.NUM_LEVELS; level++) {
      for (FileMetadata f : current.files(level)) {
        snapshot.addFile(level, f);
      }
    }
    LogWriter writer = new LogWriter(fs.create(dir.resolve(LsmFiles.manifestName(number))), 0);
    try {
      writer.add(snapshot.encode());
      writer.sync();
      setCurrentFile(number);
    } catch (IOException | RuntimeException e) {
      writer.close();
      throw e;
    }
    if (manifest != null) {
      manifest.close();
    }
    manifest = writer;
    manifestNumber = number;
  }

  /** Atomically points CURRENT at manifest {@code number}: write a temp file, sync, rename. */
  private void setCurrentFile(long number) throws IOException {
    Path tmp = dir.resolve(LsmFiles.CURRENT_TMP);
    fs.deleteIfExists(tmp);
    byte[] contents = (LsmFiles.manifestName(number) + "\n").getBytes(StandardCharsets.UTF_8);
    try (FileHandle f = fs.create(tmp)) {
      f.write(ByteBuffer.wrap(contents), 0);
      f.sync();
    }
    fs.rename(tmp, dir.resolve(LsmFiles.CURRENT));
    fs.syncDirectory(dir);
  }

  private String readCurrentFile() throws IOException {
    try (FileHandle f = fs.openReadOnly(dir.resolve(LsmFiles.CURRENT))) {
      long size = f.size();
      if (size == 0 || size > 256) {
        throw new CorruptionException("CURRENT has " + size + " bytes");
      }
      ByteBuffer buf = ByteBuffer.allocate((int) size);
      f.readFully(buf, 0);
      return new String(buf.array(), StandardCharsets.UTF_8);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (manifest != null) {
      manifest.close();
      manifest = null;
    }
  }
}

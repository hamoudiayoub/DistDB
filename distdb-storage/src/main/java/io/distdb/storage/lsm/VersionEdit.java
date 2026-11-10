package io.distdb.storage.lsm;

import io.distdb.storage.CorruptionException;
import java.util.ArrayList;
import java.util.List;

/**
 * One manifest record: a change to the set of live files plus counters. Replaying every edit in the
 * manifest, in order, rebuilds the engine's file layout after a restart.
 *
 * <p>Format: a sequence of tagged fields. 1 = log number (8), 2 = next file number (8), 3 = last
 * sequence (8), 4 = deleted file (level 1, number 8), 5 = added file (level 1, number 8, size 8,
 * smallest and largest internal keys, length-prefixed).
 */
final class VersionEdit {

  record AddedFile(int level, FileMetadata file) {}

  record DeletedFile(int level, long number) {}

  private Long logNumber;
  private Long nextFileNumber;
  private Long lastSequence;
  private final List<AddedFile> added = new ArrayList<>();
  private final List<DeletedFile> deleted = new ArrayList<>();

  VersionEdit setLogNumber(long n) {
    logNumber = n;
    return this;
  }

  VersionEdit setNextFileNumber(long n) {
    nextFileNumber = n;
    return this;
  }

  VersionEdit setLastSequence(long s) {
    lastSequence = s;
    return this;
  }

  VersionEdit addFile(int level, FileMetadata f) {
    added.add(new AddedFile(level, f));
    return this;
  }

  VersionEdit deleteFile(int level, long number) {
    deleted.add(new DeletedFile(level, number));
    return this;
  }

  Long logNumber() {
    return logNumber;
  }

  Long nextFileNumber() {
    return nextFileNumber;
  }

  Long lastSequence() {
    return lastSequence;
  }

  List<AddedFile> added() {
    return added;
  }

  List<DeletedFile> deleted() {
    return deleted;
  }

  byte[] encode() {
    ByteSink out = new ByteSink(64);
    if (logNumber != null) {
      out.writeByte(1);
      out.writeLong(logNumber);
    }
    if (nextFileNumber != null) {
      out.writeByte(2);
      out.writeLong(nextFileNumber);
    }
    if (lastSequence != null) {
      out.writeByte(3);
      out.writeLong(lastSequence);
    }
    for (DeletedFile d : deleted) {
      out.writeByte(4);
      out.writeByte(d.level());
      out.writeLong(d.number());
    }
    for (AddedFile a : added) {
      out.writeByte(5);
      out.writeByte(a.level());
      out.writeLong(a.file().number());
      out.writeLong(a.file().size());
      out.writeLengthPrefixed(a.file().smallest());
      out.writeLengthPrefixed(a.file().largest());
    }
    return out.toByteArray();
  }

  static VersionEdit decode(byte[] record) {
    VersionEdit e = new VersionEdit();
    ByteSource in = new ByteSource(record);
    while (in.hasRemaining()) {
      int tag = in.readByte();
      switch (tag) {
        case 1 -> e.logNumber = in.readLong();
        case 2 -> e.nextFileNumber = in.readLong();
        case 3 -> e.lastSequence = in.readLong();
        case 4 -> e.deleteFile(level(in), in.readLong());
        case 5 -> {
          int level = level(in);
          long number = in.readLong();
          long size = in.readLong();
          byte[] smallest = in.readLengthPrefixed();
          byte[] largest = in.readLengthPrefixed();
          e.addFile(level, new FileMetadata(number, size, smallest, largest));
        }
        default -> throw new CorruptionException("unknown manifest field " + tag);
      }
    }
    return e;
  }

  private static int level(ByteSource in) {
    int level = in.readByte();
    if (level >= Version.NUM_LEVELS) {
      throw new CorruptionException("bad level " + level);
    }
    return level;
  }
}

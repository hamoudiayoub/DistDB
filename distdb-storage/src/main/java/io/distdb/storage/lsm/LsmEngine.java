package io.distdb.storage.lsm;

import io.distdb.env.FileHandle;
import io.distdb.env.FileSystem;
import io.distdb.env.Scheduler;
import io.distdb.storage.EngineSnapshot;
import io.distdb.storage.KvEngine;
import io.distdb.storage.KvIterator;
import io.distdb.storage.StorageException;
import io.distdb.storage.WriteBatch;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A log-structured merge-tree {@link KvEngine}.
 *
 * <p><b>Write path:</b> append the batch to the write-ahead log (fsync if asked), insert it into
 * the memtable, then publish its sequence numbers. When the memtable is full it is frozen and a new
 * one (with a new log) takes over; a background task flushes the frozen one to a level-0 SSTable.
 *
 * <p><b>Read path:</b> memtable, frozen memtable, then SSTables shallow to deep. Every read runs at
 * a sequence number and ignores newer entries, so readers never block writers.
 *
 * <p><b>Compaction:</b> leveled, as in LevelDB/RocksDB; see {@link Compaction}.
 *
 * <p><b>Crash safety</b> follows the {@link FileSystem} contract: a file is used only after it is
 * synced and its directory entry is synced; the file layout changes only through synced manifest
 * edits; a log is deleted only after the manifest says its data lives in an SSTable; and the old
 * log is synced before a new one is started, so recovery always restores a prefix of the writes.
 *
 * <p><b>Determinism:</b> all I/O goes through {@link FileSystem} and all background work through
 * {@link Scheduler}. When the writer would have to wait for background work (a full memtable with a
 * flush pending, or too many level-0 files) it does that work itself instead of blocking, so the
 * engine also runs correctly on a single-threaded simulated scheduler.
 *
 * <p>Lock order: writeLock, then flushLock or compactionLock, then the VersionSet monitor, then
 * viewLock. Readers take only viewLock, briefly.
 */
public final class LsmEngine implements KvEngine {

  private final FileSystem fs;
  private final Scheduler scheduler;
  private final Path dir;
  private final EngineOptions options;
  private final VersionSet versions;
  private final TableCache tables;

  private final ReentrantLock writeLock = new ReentrantLock();

  /**
   * Group commit: writers queue here, and the one at the head writes the whole queue's batches with
   * one log write and one fsync, while the others wait. Guarded by writeLock.
   */
  private final ArrayDeque<Writer> writers = new ArrayDeque<>();

  private final Condition writersTurn = writeLock.newCondition();

  /**
   * Held by the group's leader while it writes the log without writeLock (so that more writers can
   * queue meanwhile), and by close() before it touches the log. Order: writeLock, then logLock.
   */
  private final ReentrantLock logLock = new ReentrantLock();

  private static final int MAX_GROUP = 256;

  /** A write waiting in the queue; {@code done} once a leader wrote it (or failed to). */
  private static final class Writer {
    final WriteBatch batch;
    final boolean sync;
    boolean done;
    RuntimeException failure;

    Writer(WriteBatch batch, boolean sync) {
      this.batch = batch;
      this.sync = sync;
    }
  }

  /**
   * What this engine has done since it opened, for metrics. {@code groupCommits} counts the log
   * writes that carried {@code groupedWrites} batches between them.
   */
  public record Stats(
      long writes,
      long walSyncs,
      long groupCommits,
      long groupedWrites,
      long flushes,
      long flushBytes,
      long compactions,
      long compactionBytesRead,
      long compactionBytesWritten) {}

  private final LongAdder statWrites = new LongAdder();
  private final LongAdder statSyncs = new LongAdder();
  private final LongAdder statGroups = new LongAdder();
  private final LongAdder statGrouped = new LongAdder();
  private final LongAdder statFlushes = new LongAdder();
  private final LongAdder statFlushBytes = new LongAdder();
  private final LongAdder statCompactions = new LongAdder();
  private final LongAdder statCompactionRead = new LongAdder();
  private final LongAdder statCompactionWritten = new LongAdder();

  public Stats stats() {
    return new Stats(
        statWrites.sum(),
        statSyncs.sum(),
        statGroups.sum(),
        statGrouped.sum(),
        statFlushes.sum(),
        statFlushBytes.sum(),
        statCompactions.sum(),
        statCompactionRead.sum(),
        statCompactionWritten.sum());
  }

  private final ReentrantLock flushLock = new ReentrantLock();
  private final ReentrantLock compactionLock = new ReentrantLock();
  private final Object viewLock = new Object();
  private final Object liveLock = new Object();
  private final Object obsoleteLock = new Object();

  /** What readers see; replaced atomically. Guarded by viewLock. */
  private View view;

  /** Highest sequence number whose batch is fully in the memtable. Guarded by viewLock. */
  private long visibleSequence;

  /** Sequence numbers of open snapshots, with counts. Guarded by viewLock. */
  private final TreeMap<Long, Integer> snapshots = new TreeMap<>();

  // Writer state, guarded by writeLock.
  private LogWriter wal;
  private long lastSequence;

  /** Versions some reader or the view still references. Guarded by liveLock. */
  private final Set<Version> liveVersions = Collections.newSetFromMap(new IdentityHashMap<>());

  /** Numbers of files being written by a flush or compaction. Guarded by liveLock. */
  private final Set<Long> pendingOutputs = new HashSet<>();

  /** Per level, where the round-robin compaction picker left off. Guarded by compactionLock. */
  private final byte[][] compactPointers = new byte[Version.NUM_LEVELS][];

  private final AtomicBoolean backgroundScheduled = new AtomicBoolean();
  private volatile Throwable backgroundError;
  private volatile boolean closed;

  /** The memtables and SSTables a read must consult, captured together. */
  private record View(Memtable mem, Memtable imm, long immNextLog, Version version) {}

  private LsmEngine(FileSystem fs, Scheduler scheduler, Path dir, EngineOptions options) {
    this.fs = fs;
    this.scheduler = scheduler;
    this.dir = dir;
    this.options = options;
    this.versions = new VersionSet(fs, dir);
    this.tables = new TableCache(fs, dir);
  }

  /**
   * Opens (creating if needed) the engine in {@code dir}, recovering any state left by a previous
   * run or crash.
   */
  public static LsmEngine open(
      FileSystem fs, Scheduler scheduler, Path dir, EngineOptions options) {
    LsmEngine engine =
        new LsmEngine(
            Objects.requireNonNull(fs),
            Objects.requireNonNull(scheduler),
            Objects.requireNonNull(dir),
            Objects.requireNonNull(options));
    try {
      engine.recover();
    } catch (IOException | RuntimeException e) {
      engine.abandon();
      if (e instanceof StorageException se) {
        throw se;
      }
      throw new StorageException("failed to open engine in " + dir, e);
    }
    engine.maybeScheduleBackgroundWork();
    return engine;
  }

  // ---------------------------------------------------------------------------------------------
  // Recovery

  private void recover() throws IOException {
    fs.createDirectories(dir);
    // Leftovers of a crashed run may carry numbers the manifest never recorded; never reuse them.
    for (Path p : fs.list(dir)) {
      LsmFiles.number(p.getFileName().toString()).ifPresent(versions::markFileNumberUsed);
    }
    if (fs.exists(dir.resolve(LsmFiles.CURRENT))) {
      versions.recover();
    }
    versions.writeSnapshotManifest();
    Version base = versions.current();
    base.ref();
    synchronized (liveLock) {
      liveVersions.add(base);
    }
    view = new View(new Memtable(), null, 0, base);

    // Replay every log the manifest still needs, oldest first, stopping at the first damaged tail.
    TreeSet<Long> logs = new TreeSet<>();
    for (Path p : fs.list(dir)) {
      String name = p.getFileName().toString();
      OptionalLong n = LsmFiles.number(name);
      if (LsmFiles.kind(name) == LsmFiles.Kind.LOG && n.getAsLong() >= versions.logNumber()) {
        logs.add(n.getAsLong());
      }
    }
    Memtable replayed = new Memtable();
    long maxSequence = versions.lastSequence();
    for (long n : logs) {
      LogReader.Result r;
      try (FileHandle f = fs.openReadOnly(LsmFiles.log(dir, n))) {
        r = LogReader.readAll(f);
      }
      for (byte[] record : r.records()) {
        maxSequence = Math.max(maxSequence, BatchCodec.replay(record, replayed));
      }
      if (r.damagedTail()) {
        break;
      }
    }
    lastSequence = maxSequence;
    visibleSequence = maxSequence;

    // Move replayed data into a level-0 table and start a fresh log; only once the manifest
    // records both do the old logs become obsolete.
    VersionEdit edit = new VersionEdit();
    if (!replayed.isEmpty()) {
      long number = newOutputNumber();
      FileMetadata table = writeTable(replayed.iterator(), number);
      if (table != null) {
        edit.addFile(0, table);
      }
    }
    long logNumber = versions.newFileNumber();
    wal = new LogWriter(fs.create(LsmFiles.log(dir, logNumber)), 0);
    fs.syncDirectory(dir);
    edit.setLogNumber(logNumber).setLastSequence(lastSequence);
    versions.logAndApply(edit, v -> installVersion(v, false));
    clearPendingOutputs();
    deleteObsoleteFiles();
  }

  // ---------------------------------------------------------------------------------------------
  // Writes

  /**
   * {@inheritDoc}
   *
   * <p>With group commit, concurrent writers share one log write and one fsync: each queues its
   * batch, and the writer at the head of the queue writes every queued batch, syncs once if any of
   * them asked to, applies them in order, and wakes the others. No batch becomes visible, and no
   * writer returns, before the write (and the fsync) that covers it.
   */
  @Override
  public void write(WriteBatch batch, boolean sync) {
    Objects.requireNonNull(batch);
    if (batch.isEmpty()) {
      return;
    }
    statWrites.increment();
    if (!options.groupCommit()) {
      writeAlone(batch, sync);
      return;
    }
    Writer w = new Writer(batch, sync);
    writeLock.lock();
    try {
      writers.addLast(w);
      while (!w.done && writers.peekFirst() != w) {
        writersTurn.awaitUninterruptibly();
      }
      if (w.done) {
        if (w.failure != null) {
          throw w.failure;
        }
        return;
      }
      lead(w);
    } finally {
      writeLock.unlock();
    }
    maybeScheduleBackgroundWork();
  }

  /** Called with writeLock held by the writer at the head of the queue: writes a group. */
  private void lead(Writer head) {
    List<Writer> group = new ArrayList<>();
    RuntimeException failure = null;
    try {
      checkWritable();
      makeRoomForWrite();
      boolean sync = false;
      long count = 0;
      for (Writer x : writers) {
        if (group.size() == MAX_GROUP) {
          break;
        }
        group.add(x);
        sync |= x.sync;
        count += x.batch.count();
      }
      long first = lastSequence + 1;
      if (first + count - 1 > InternalKey.MAX_SEQUENCE) {
        throw new StorageException("sequence numbers exhausted");
      }
      List<byte[]> records = new ArrayList<>(group.size());
      long seq = first;
      for (Writer x : group) {
        records.add(BatchCodec.encode(x.batch, seq));
        seq += x.batch.count();
      }
      LogWriter log = wal;
      // Let more writers queue (for the next group) while this one is written.
      writeLock.unlock();
      logLock.lock();
      try {
        for (byte[] r : records) {
          log.add(r);
        }
        if (sync) {
          log.sync();
          statSyncs.increment();
        }
      } catch (IOException e) {
        throw fail("write-ahead log write failed", e);
      } finally {
        logLock.unlock();
        writeLock.lock();
      }
      checkWritable();
      seq = first;
      Memtable mem = currentView().mem();
      for (Writer x : group) {
        BatchCodec.apply(x.batch, seq, mem);
        seq += x.batch.count();
      }
      lastSequence = first + count - 1;
      synchronized (viewLock) {
        visibleSequence = lastSequence;
      }
      statGroups.increment();
      statGrouped.add(group.size());
    } catch (RuntimeException e) {
      failure = e;
      throw e;
    } finally {
      if (group.isEmpty()) {
        group.add(head);
      }
      for (Writer x : group) {
        writers.remove(x);
        x.done = true;
        if (x != head) {
          x.failure = failure;
        }
      }
      writersTurn.signalAll();
    }
  }

  /** The write path without group commit: one log write (and fsync) per batch, in turn. */
  private void writeAlone(WriteBatch batch, boolean sync) {
    writeLock.lock();
    try {
      checkWritable();
      makeRoomForWrite();
      long first = lastSequence + 1;
      if (first + batch.count() - 1 > InternalKey.MAX_SEQUENCE) {
        throw new StorageException("sequence numbers exhausted");
      }
      try {
        logLock.lock();
        try {
          wal.add(BatchCodec.encode(batch, first));
          if (sync) {
            wal.sync();
            statSyncs.increment();
          }
        } finally {
          logLock.unlock();
        }
      } catch (IOException e) {
        throw fail("write-ahead log write failed", e);
      }
      BatchCodec.apply(batch, first, currentView().mem());
      lastSequence = first + batch.count() - 1;
      synchronized (viewLock) {
        visibleSequence = lastSequence;
      }
    } finally {
      writeLock.unlock();
    }
    maybeScheduleBackgroundWork();
  }

  /** Called with writeLock held: ensures the memtable has room, doing background work if needed. */
  private void makeRoomForWrite() {
    while (true) {
      checkWritable();
      View v = currentView();
      if (v.version().numFiles(0) >= options.l0StopTrigger()) {
        runOneCompaction(); // stall: compact level 0 ourselves rather than wait
        continue;
      }
      if (v.mem().approximateBytes() < options.memtableBytes()) {
        return;
      }
      if (v.imm() != null) {
        flushImmutable(); // stall: flush the previous memtable ourselves rather than wait
        continue;
      }
      switchMemtable();
      return;
    }
  }

  /** Freezes the memtable and starts a new one with a new log. Called with writeLock held. */
  private void switchMemtable() {
    try {
      // Sync the old log before starting the new one: a later synced write must never survive a
      // crash while an earlier unsynced one in the old log is lost.
      wal.sync();
      wal.close();
      long number = versions.newFileNumber();
      FileHandle f = fs.create(LsmFiles.log(dir, number));
      fs.syncDirectory(dir);
      wal = new LogWriter(f, 0);
      synchronized (viewLock) {
        view = new View(new Memtable(), view.mem(), number, view.version());
      }
    } catch (IOException e) {
      throw fail("switching memtable failed", e);
    }
    maybeScheduleBackgroundWork();
  }

  // ---------------------------------------------------------------------------------------------
  // Reads

  @Override
  public Optional<byte[]> get(byte[] key) {
    Objects.requireNonNull(key);
    View v;
    long seq;
    synchronized (viewLock) {
      checkOpen();
      v = view;
      seq = visibleSequence;
      v.version().ref();
    }
    try {
      return lookup(v, key, seq);
    } finally {
      release(v.version());
    }
  }

  @Override
  public KvIterator scan(byte[] startInclusive, byte[] endExclusive) {
    Objects.requireNonNull(startInclusive);
    View v;
    long seq;
    synchronized (viewLock) {
      checkOpen();
      v = view;
      seq = visibleSequence;
      v.version().ref();
    }
    return newIterator(v, seq, startInclusive, endExclusive);
  }

  @Override
  public EngineSnapshot snapshot() {
    long seq;
    synchronized (viewLock) {
      checkOpen();
      seq = visibleSequence;
      snapshots.merge(seq, 1, Integer::sum);
    }
    return new Snapshot(seq);
  }

  /** The sequence number of the last write applied; used by tests and by recovery checks. */
  public long lastSequence() {
    synchronized (viewLock) {
      return visibleSequence;
    }
  }

  /** The number of SSTables at {@code level}, for tests and debugging. */
  public int numFilesAtLevel(int level) {
    return currentView().version().numFiles(level);
  }

  /** A one-line description of the file layout, for tests and debugging. */
  public String describeLevels() {
    return currentView().version().toString();
  }

  private Optional<byte[]> lookup(View v, byte[] key, long seq) {
    Hit hit = v.mem().get(key, seq);
    if (hit == null && v.imm() != null) {
      hit = v.imm().get(key, seq);
    }
    if (hit == null) {
      hit = v.version().get(key, seq, tables);
    }
    return hit == null || hit.deleted() ? Optional.empty() : Optional.of(hit.value().clone());
  }

  /** Builds an iterator over a referenced view; the reference is released when it closes. */
  private KvIterator newIterator(View v, long seq, byte[] start, byte[] end) {
    try {
      List<InternalIterator> children = new ArrayList<>();
      children.add(v.mem().iterator());
      if (v.imm() != null) {
        children.add(v.imm().iterator());
      }
      v.version().addIterators(children, tables, start, end);
      return new DbIterator(
          new MergingIterator(children), seq, start, end, () -> release(v.version()));
    } catch (RuntimeException e) {
      release(v.version());
      throw e;
    }
  }

  private final class Snapshot implements EngineSnapshot {
    private final long seq;
    private final AtomicBoolean released = new AtomicBoolean();

    Snapshot(long seq) {
      this.seq = seq;
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
      View v = acquireView();
      try {
        return lookup(v, key, seq);
      } finally {
        release(v.version());
      }
    }

    @Override
    public KvIterator scan(byte[] startInclusive, byte[] endExclusive) {
      return newIterator(acquireView(), seq, startInclusive, endExclusive);
    }

    @Override
    public void close() {
      if (released.compareAndSet(false, true)) {
        synchronized (viewLock) {
          snapshots.computeIfPresent(seq, (k, n) -> n == 1 ? null : n - 1);
        }
      }
    }
  }

  private View acquireView() {
    synchronized (viewLock) {
      checkOpen();
      view.version().ref();
      return view;
    }
  }

  private View currentView() {
    synchronized (viewLock) {
      return view;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Background work: flushes and compactions

  private void maybeScheduleBackgroundWork() {
    if (closed || backgroundError != null) {
      return;
    }
    View v = currentView();
    boolean work = v.imm() != null || Compaction.needed(v.version(), options);
    if (work && backgroundScheduled.compareAndSet(false, true)) {
      scheduler.execute(this::backgroundWork);
    }
  }

  /** One step of background work: flush if needed, then at most one compaction. */
  private void backgroundWork() {
    try {
      if (closed || backgroundError != null) {
        return;
      }
      if (currentView().imm() != null) {
        flushImmutable();
      }
      if (!closed) {
        runOneCompaction();
      }
    } catch (Throwable t) {
      if (backgroundError == null) {
        backgroundError = t;
      }
    } finally {
      backgroundScheduled.set(false);
    }
    maybeScheduleBackgroundWork();
  }

  /** Writes the frozen memtable to a level-0 table and drops its log. */
  private void flushImmutable() {
    flushLock.lock();
    try {
      View v = currentView();
      if (v.imm() == null || closed) {
        return;
      }
      long number = newOutputNumber();
      try {
        FileMetadata table = writeTable(v.imm().iterator(), number);
        VersionEdit edit = new VersionEdit();
        if (table != null) {
          edit.addFile(0, table);
          statFlushBytes.add(table.size());
        }
        statFlushes.increment();
        edit.setLogNumber(v.immNextLog()).setLastSequence(v.imm().maxSequence());
        versions.logAndApply(edit, nv -> installVersion(nv, true));
      } catch (IOException e) {
        throw fail("flush failed", e);
      } finally {
        removePendingOutput(number);
      }
      deleteObsoleteFiles();
    } finally {
      flushLock.unlock();
    }
  }

  /**
   * Runs the most urgent compaction, if any.
   *
   * @return whether a compaction ran
   */
  private boolean runOneCompaction() {
    compactionLock.lock();
    try {
      if (closed) {
        return false;
      }
      Version base = acquireView().version();
      try {
        Compaction c = Compaction.pick(base, options, compactPointers);
        if (c == null) {
          return false;
        }
        if (c.isTrivialMove()) {
          FileMetadata f = c.inputs().get(0);
          VersionEdit edit =
              new VersionEdit().deleteFile(c.level(), f.number()).addFile(c.outputLevel(), f);
          versions.logAndApply(edit, nv -> installVersion(nv, false));
          compactPointers[c.level()] = f.largestUserKey();
          return true;
        }
        List<FileMetadata> outputs = mergeInputs(c, base);
        statCompactions.increment();
        for (FileMetadata f : c.inputs()) {
          statCompactionRead.add(f.size());
        }
        for (FileMetadata f : c.nextLevelInputs()) {
          statCompactionRead.add(f.size());
        }
        for (FileMetadata f : outputs) {
          statCompactionWritten.add(f.size());
        }
        VersionEdit edit = new VersionEdit();
        for (FileMetadata f : c.inputs()) {
          edit.deleteFile(c.level(), f.number());
        }
        for (FileMetadata f : c.nextLevelInputs()) {
          edit.deleteFile(c.outputLevel(), f.number());
        }
        for (FileMetadata f : outputs) {
          edit.addFile(c.outputLevel(), f);
        }
        try {
          versions.logAndApply(edit, nv -> installVersion(nv, false));
        } finally {
          for (FileMetadata f : outputs) {
            removePendingOutput(f.number());
          }
        }
        if (c.level() > 0) {
          compactPointers[c.level()] = c.inputs().get(0).largestUserKey();
        }
        deleteObsoleteFiles();
        return true;
      } catch (IOException e) {
        throw fail("compaction failed", e);
      } finally {
        release(base);
      }
    } finally {
      compactionLock.unlock();
    }
  }

  /**
   * Merges a compaction's inputs into new tables at its output level, dropping what no reader can
   * ever see: versions shadowed by a newer version that every snapshot already sees, and tombstones
   * with nothing older beneath them.
   */
  private List<FileMetadata> mergeInputs(Compaction c, Version base) throws IOException {
    long smallestSnapshot;
    synchronized (viewLock) {
      smallestSnapshot = snapshots.isEmpty() ? visibleSequence : snapshots.firstKey();
    }
    List<InternalIterator> children = new ArrayList<>();
    for (FileMetadata f : c.allInputs()) {
      children.add(tables.get(f.number()).iterator());
    }
    List<FileMetadata> outputs = new ArrayList<>();
    Output out = null;
    try (MergingIterator it = new MergingIterator(children)) {
      it.seekToFirst();
      byte[] currentUserKey = null;
      long lastSequenceForKey = Long.MAX_VALUE;
      while (it.valid()) {
        if (closed) {
          throw new StorageException("engine closing; compaction abandoned");
        }
        byte[] ik = it.key();
        if (currentUserKey == null || InternalKey.compareUserKey(ik, currentUserKey) != 0) {
          currentUserKey = InternalKey.userKey(ik);
          lastSequenceForKey = Long.MAX_VALUE;
        }
        long seq = InternalKey.sequence(ik);
        boolean drop;
        if (lastSequenceForKey <= smallestSnapshot) {
          drop = true; // a newer version of this key is visible to every reader
        } else {
          drop =
              InternalKey.kind(ik) == InternalKey.DELETE
                  && seq <= smallestSnapshot
                  && base.isBaseLevelForKey(currentUserKey, c.outputLevel());
        }
        lastSequenceForKey = seq;
        if (!drop) {
          // Cut a new output file only between user keys, so a key never spans two files at a
          // level.
          if (out != null
              && out.builder.estimatedSize() >= options.targetFileBytes()
              && !InternalKey.sameUserKey(out.builder.largest(), ik)) {
            outputs.add(out.finish());
            out = null;
          }
          if (out == null) {
            out = new Output(newOutputNumber());
          }
          out.builder.add(ik, it.value());
        }
        it.next();
      }
      if (out != null) {
        outputs.add(out.finish());
        out = null;
      }
      fs.syncDirectory(dir);
      return outputs;
    } catch (IOException | RuntimeException e) {
      if (out != null) {
        out.abandon();
      }
      for (FileMetadata f : outputs) {
        removePendingOutput(f.number());
      }
      throw e;
    }
  }

  /** One compaction output file being written. */
  private final class Output {
    final long number;
    final FileHandle file;
    final TableBuilder builder;

    Output(long number) throws IOException {
      this.number = number;
      this.file = fs.create(LsmFiles.table(dir, number));
      this.builder = new TableBuilder(file, options);
    }

    FileMetadata finish() throws IOException {
      long size = builder.finish();
      file.sync();
      file.close();
      return new FileMetadata(number, size, builder.smallest(), builder.largest());
    }

    void abandon() {
      try {
        file.close();
      } catch (IOException ignored) {
        // Unreferenced; deleted as obsolete later.
      }
      removePendingOutput(number);
    }
  }

  /** Writes every entry of {@code it} to a new table; returns null (and no file) if it is empty. */
  private FileMetadata writeTable(InternalIterator it, long number) throws IOException {
    Path path = LsmFiles.table(dir, number);
    TableBuilder builder;
    long size;
    try (FileHandle f = fs.create(path)) {
      builder = new TableBuilder(f, options);
      for (it.seekToFirst(); it.valid(); it.next()) {
        builder.add(it.key(), it.value());
      }
      if (builder.entries() == 0) {
        size = -1;
      } else {
        size = builder.finish();
        f.sync();
      }
    }
    if (size < 0) {
      fs.deleteIfExists(path);
      return null;
    }
    fs.syncDirectory(dir);
    return new FileMetadata(number, size, builder.smallest(), builder.largest());
  }

  // ---------------------------------------------------------------------------------------------
  // Versions, references and obsolete files

  /** Installs a new version into the view. Called inside VersionSet's lock, in log order. */
  private void installVersion(Version next, boolean clearImmutable) {
    next.ref();
    synchronized (liveLock) {
      liveVersions.add(next);
    }
    Version old;
    synchronized (viewLock) {
      old = view.version();
      view =
          clearImmutable
              ? new View(view.mem(), null, 0, next)
              : new View(view.mem(), view.imm(), view.immNextLog(), next);
    }
    release(old);
  }

  private void release(Version v) {
    if (v.unref()) {
      synchronized (liveLock) {
        liveVersions.remove(v);
      }
    }
  }

  private long newOutputNumber() {
    long n = versions.newFileNumber();
    synchronized (liveLock) {
      pendingOutputs.add(n);
    }
    return n;
  }

  private void removePendingOutput(long n) {
    synchronized (liveLock) {
      pendingOutputs.remove(n);
    }
  }

  private void clearPendingOutputs() {
    synchronized (liveLock) {
      pendingOutputs.clear();
    }
  }

  /**
   * Deletes files nothing needs: tables in no live version and not being written, logs older than
   * the oldest log the manifest still needs, and old manifests.
   */
  private void deleteObsoleteFiles() {
    synchronized (obsoleteLock) {
      try {
        // List first, then take the live set. A table in the listing exists, so its number was
        // already pending (numbers are pending before their file is created) and stays pending
        // until a live version holds it. The other order deletes a table that a concurrent flush
        // or compaction creates between the two steps.
        List<Path> files = fs.list(dir);
        Set<Long> live = new HashSet<>();
        synchronized (liveLock) {
          for (Version v : liveVersions) {
            live.addAll(v.fileNumbers());
          }
          live.addAll(pendingOutputs);
        }
        long minLog = versions.logNumber();
        long manifest = versions.manifestNumber();
        for (Path p : files) {
          String name = p.getFileName().toString();
          OptionalLong n = LsmFiles.number(name);
          boolean delete =
              switch (LsmFiles.kind(name)) {
                case LOG -> n.getAsLong() < minLog;
                case TABLE -> !live.contains(n.getAsLong());
                case MANIFEST -> n.getAsLong() != manifest;
                case TEMP, CURRENT, OTHER -> false;
              };
          if (delete) {
            if (LsmFiles.kind(name) == LsmFiles.Kind.TABLE) {
              tables.evict(n.getAsLong());
            }
            fs.deleteIfExists(p);
          }
        }
      } catch (IOException e) {
        // Leftover files waste space but are harmless; the next pass will retry.
        if (backgroundError == null && !closed) {
          throw fail("deleting obsolete files failed", e);
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Failure and shutdown

  private StorageException fail(String what, IOException cause) {
    StorageException e = new StorageException(what + " in " + dir, cause);
    if (backgroundError == null) {
      backgroundError = e;
    }
    return e;
  }

  private void checkOpen() {
    if (closed) {
      throw new StorageException("engine is closed: " + dir);
    }
  }

  private void checkWritable() {
    checkOpen();
    Throwable err = backgroundError;
    if (err != null) {
      throw new StorageException("engine failed earlier; reopen to recover: " + dir, err);
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    writeLock.lock();
    logLock.lock();
    flushLock.lock();
    compactionLock.lock();
    try {
      if (wal != null && backgroundError == null) {
        try {
          wal.sync();
        } catch (IOException e) {
          throw new StorageException("syncing log on close failed in " + dir, e);
        }
      }
    } finally {
      abandon();
      compactionLock.unlock();
      flushLock.unlock();
      logLock.unlock();
      writeLock.unlock();
    }
  }

  /** Releases resources without making anything durable (failed open, or after a crash). */
  private void abandon() {
    closed = true;
    try {
      if (wal != null) {
        wal.close();
      }
    } catch (IOException ignored) {
      // Best effort.
    }
    try {
      versions.close();
    } catch (IOException ignored) {
      // Best effort.
    }
    tables.close();
  }

  @Override
  public String toString() {
    return "LsmEngine(" + dir + ", " + describeLevels() + ")";
  }
}

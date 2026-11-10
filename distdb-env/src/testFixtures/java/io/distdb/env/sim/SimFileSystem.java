package io.distdb.env.sim;

import io.distdb.env.FileHandle;
import io.distdb.env.FileSystem;
import io.distdb.env.RandomSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An in-memory {@link FileSystem} that enforces the durability contract to the letter, and no more.
 *
 * <p>It keeps two views of the world: what the running process sees, and what is on "disk". {@link
 * FileHandle#sync()} copies a file's bytes to disk; {@link #syncDirectory} copies a directory's
 * entries (creates, renames, deletes) to disk. {@link #crash()} throws the process view away and
 * rebuilds it from disk, and for every unsynced region it rolls dice: keep the new bytes, keep the
 * old bytes, or tear the page (a mix of new, old and zeroed sectors). File lengths that changed
 * since the last sync may land on the old length, the new one, or anywhere in between.
 *
 * <p>{@link #crashAfter(long)} arms a countdown of mutating operations; when it reaches zero the
 * operation fails and the file system plays dead (every call throws {@link
 * SimulatedCrashException}) until the test calls {@link #crash()}. That lets a test cut power in
 * the middle of a flush, a compaction or a recovery, at any of its I/O steps.
 *
 * <p>Simplification: directories themselves are durable as soon as they are created.
 *
 * <p>All randomness comes from the {@link RandomSource} given at construction, so a crash test is
 * reproducible from its seed.
 */
public final class SimFileSystem implements FileSystem {

  /** Granularity at which unsynced data survives or not. */
  public static final int PAGE_SIZE = 4096;

  /** Granularity of a torn page. */
  private static final int SECTOR_SIZE = 512;

  private final RandomSource random;
  private final Map<Path, Inode> namespace = new HashMap<>();
  private final Map<Path, Inode> durableNamespace = new HashMap<>();
  private final Set<Path> directories = new HashSet<>();
  private int generation;
  private long mutationsUntilCrash;
  private boolean dead;
  private long crashes;

  public SimFileSystem(RandomSource random) {
    this.random = Objects.requireNonNull(random);
  }

  /**
   * Arms a crash: the {@code mutations}-th mutating operation from now (create, write, truncate,
   * sync, rename, delete, directory create or sync) fails instead of happening, and the file system
   * then plays dead.
   */
  public synchronized void crashAfter(long mutations) {
    if (mutations <= 0) {
      throw new IllegalArgumentException("mutations must be positive: " + mutations);
    }
    mutationsUntilCrash = mutations;
  }

  /** Whether an armed crash has fired and the file system is waiting for {@link #crash()}. */
  public synchronized boolean isDead() {
    return dead;
  }

  /** How many times {@link #crash()} has been called. */
  public synchronized long crashes() {
    return crashes;
  }

  /**
   * Simulates power loss: everything not yet synced is resolved at random as described in the class
   * comment, every open handle becomes unusable, and the file system comes back alive.
   */
  public synchronized void crash() {
    generation++;
    crashes++;
    dead = false;
    mutationsUntilCrash = 0;
    namespace.clear();
    Map<Inode, Boolean> recovered = new IdentityHashMap<>();
    for (Map.Entry<Path, Inode> e : durableNamespace.entrySet()) {
      Inode inode = e.getValue();
      if (recovered.put(inode, Boolean.TRUE) == null) {
        inode.recover(random);
      }
      namespace.put(e.getKey(), inode);
    }
  }

  // ---- FileSystem ----

  @Override
  public synchronized FileHandle create(Path path) throws IOException {
    Path p = normalize(path);
    mutation();
    if (namespace.containsKey(p) || directories.contains(p)) {
      throw new FileAlreadyExistsException(p.toString());
    }
    requireDirectory(p.getParent(), p);
    Inode inode = new Inode();
    namespace.put(p, inode);
    return new Handle(inode, true, generation);
  }

  @Override
  public synchronized FileHandle open(Path path) throws IOException {
    checkAlive();
    return new Handle(existing(path), true, generation);
  }

  @Override
  public synchronized FileHandle openReadOnly(Path path) throws IOException {
    checkAlive();
    return new Handle(existing(path), false, generation);
  }

  @Override
  public synchronized boolean exists(Path path) {
    Path p = normalize(path);
    return namespace.containsKey(p) || directories.contains(p);
  }

  @Override
  public synchronized boolean deleteIfExists(Path path) throws IOException {
    Path p = normalize(path);
    mutation();
    return namespace.remove(p) != null;
  }

  @Override
  public synchronized void rename(Path from, Path to) throws IOException {
    Path src = normalize(from);
    Path dst = normalize(to);
    mutation();
    Inode inode = namespace.get(src);
    if (inode == null) {
      throw new NoSuchFileException(src.toString());
    }
    requireDirectory(dst.getParent(), dst);
    namespace.remove(src);
    namespace.put(dst, inode);
  }

  @Override
  public synchronized List<Path> list(Path dir) throws IOException {
    checkAlive();
    Path d = normalize(dir);
    if (!directories.contains(d)) {
      throw new NoSuchFileException(d.toString());
    }
    List<Path> out = new ArrayList<>();
    for (Path p : namespace.keySet()) {
      if (d.equals(p.getParent())) {
        out.add(p);
      }
    }
    for (Path p : directories) {
      if (d.equals(p.getParent())) {
        out.add(p);
      }
    }
    out.sort(null);
    return out;
  }

  @Override
  public synchronized void createDirectories(Path dir) throws IOException {
    mutation();
    for (Path p = normalize(dir); p != null; p = p.getParent()) {
      if (namespace.containsKey(p)) {
        throw new FileAlreadyExistsException(p + " is a file");
      }
      directories.add(p);
    }
  }

  @Override
  public synchronized void syncDirectory(Path dir) throws IOException {
    Path d = normalize(dir);
    mutation();
    Set<Path> entries = new HashSet<>();
    for (Path p : namespace.keySet()) {
      if (d.equals(p.getParent())) {
        entries.add(p);
      }
    }
    for (Path p : durableNamespace.keySet()) {
      if (d.equals(p.getParent())) {
        entries.add(p);
      }
    }
    for (Path p : entries) {
      Inode current = namespace.get(p);
      if (current == null) {
        durableNamespace.remove(p);
      } else {
        durableNamespace.put(p, current);
      }
    }
  }

  // ---- internals ----

  private static Path normalize(Path path) {
    return path.normalize();
  }

  private Inode existing(Path path) throws NoSuchFileException {
    Path p = normalize(path);
    Inode inode = namespace.get(p);
    if (inode == null) {
      throw new NoSuchFileException(p.toString());
    }
    return inode;
  }

  private void requireDirectory(Path parent, Path child) throws NoSuchFileException {
    if (parent != null && !directories.contains(parent)) {
      throw new NoSuchFileException(child.toString() + " (no such directory)");
    }
  }

  private void checkAlive() throws SimulatedCrashException {
    if (dead) {
      throw new SimulatedCrashException("simulated crash: file system is down");
    }
  }

  /** Called at the start of every mutating operation, before it has any effect. */
  private void mutation() throws SimulatedCrashException {
    checkAlive();
    if (mutationsUntilCrash > 0 && --mutationsUntilCrash == 0) {
      dead = true;
      throw new SimulatedCrashException("simulated crash at an armed mutation");
    }
  }

  /** One file's contents: what the process sees, and what is durably on disk. */
  private static final class Inode {
    private byte[] data = new byte[0];
    private int size;
    private byte[] durable = new byte[0];

    int read(ByteBuffer dst, long position) {
      if (position >= size) {
        return -1;
      }
      int pos = (int) position;
      int n = Math.min(dst.remaining(), size - pos);
      dst.put(data, pos, n);
      return n;
    }

    void write(ByteBuffer src, long position) {
      int pos = Math.toIntExact(position);
      int n = src.remaining();
      int end = Math.addExact(pos, n);
      if (end > data.length) {
        data = Arrays.copyOf(data, Math.max(end, data.length * 2));
      }
      if (pos > size) {
        Arrays.fill(data, size, pos, (byte) 0);
      }
      src.get(data, pos, n);
      size = Math.max(size, end);
    }

    void truncate(long newSize) {
      if (newSize < size) {
        size = (int) newSize;
      }
    }

    void sync() {
      durable = Arrays.copyOf(data, size);
    }

    /** Resolves unsynced state the way a power loss might. */
    void recover(RandomSource r) {
      int durLen = durable.length;
      int curLen = size;
      if (durLen == curLen && Arrays.equals(durable, 0, durLen, data, 0, curLen)) {
        data = Arrays.copyOf(durable, durLen);
        return;
      }
      int newLen =
          switch (r.nextInt(3)) {
            case 0 -> durLen;
            case 1 -> curLen;
            default -> {
              int lo = Math.min(durLen, curLen);
              int hi = Math.max(durLen, curLen);
              yield lo == hi ? lo : r.nextInt(lo, hi + 1);
            }
          };
      byte[] out = new byte[newLen];
      for (int start = 0; start < newLen; start += PAGE_SIZE) {
        int end = Math.min(start + PAGE_SIZE, newLen);
        if (regionEqual(start, end)) {
          copyView(out, start, end, false);
          continue;
        }
        int choice = r.nextInt(10);
        if (choice < 4) {
          copyView(out, start, end, false); // old contents survive
        } else if (choice < 8) {
          copyView(out, start, end, true); // new contents made it
        } else {
          // Torn page: each sector independently old, new, or (where nothing synced lives) zero.
          // Sector writes are atomic, and both the old and the new contents of a sector contain
          // its synced bytes, so synced bytes never turn to zeros; only never-synced space can.
          for (int s = start; s < end; s += SECTOR_SIZE) {
            int e = Math.min(s + SECTOR_SIZE, end);
            switch (r.nextInt(3)) {
              case 0 -> copyView(out, s, e, false);
              case 1 -> copyView(out, s, e, true);
              default -> {
                for (int i = s; i < e; i++) {
                  boolean syncedAndUnchanged = i < durLen && durableAt(i) == currentAt(i);
                  out[i] = syncedAndUnchanged ? durableAt(i) : 0;
                }
              }
            }
          }
        }
      }
      data = out;
      size = newLen;
      durable = Arrays.copyOf(out, newLen);
    }

    private boolean regionEqual(int start, int end) {
      for (int i = start; i < end; i++) {
        if (durableAt(i) != currentAt(i)) {
          return false;
        }
      }
      return true;
    }

    private void copyView(byte[] out, int start, int end, boolean current) {
      for (int i = start; i < end; i++) {
        out[i] = current ? currentAt(i) : durableAt(i);
      }
    }

    private byte durableAt(int i) {
      return i < durable.length ? durable[i] : 0;
    }

    private byte currentAt(int i) {
      return i < size ? data[i] : 0;
    }
  }

  private final class Handle implements FileHandle {
    private final Inode inode;
    private final boolean writable;
    private final int handleGeneration;
    private boolean closed;

    Handle(Inode inode, boolean writable, int handleGeneration) {
      this.inode = inode;
      this.writable = writable;
      this.handleGeneration = handleGeneration;
    }

    private void check() throws IOException {
      if (closed) {
        throw new ClosedChannelException();
      }
      if (handleGeneration != generation) {
        throw new SimulatedCrashException("handle opened before a crash");
      }
      checkAlive();
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
      synchronized (SimFileSystem.this) {
        check();
        return inode.read(dst, position);
      }
    }

    @Override
    public void write(ByteBuffer src, long position) throws IOException {
      synchronized (SimFileSystem.this) {
        check();
        if (!writable) {
          throw new NonWritableChannelException();
        }
        mutation();
        inode.write(src, position);
      }
    }

    @Override
    public long size() throws IOException {
      synchronized (SimFileSystem.this) {
        check();
        return inode.size;
      }
    }

    @Override
    public void truncate(long size) throws IOException {
      synchronized (SimFileSystem.this) {
        check();
        if (!writable) {
          throw new NonWritableChannelException();
        }
        mutation();
        inode.truncate(size);
      }
    }

    @Override
    public void sync() throws IOException {
      synchronized (SimFileSystem.this) {
        check();
        mutation();
        inode.sync();
      }
    }

    @Override
    public void close() {
      synchronized (SimFileSystem.this) {
        closed = true;
      }
    }
  }
}

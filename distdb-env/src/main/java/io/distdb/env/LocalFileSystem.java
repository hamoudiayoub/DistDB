package io.distdb.env;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/** The real {@link FileSystem}, backed by {@link FileChannel} and the operating system's fsync. */
public final class LocalFileSystem implements FileSystem {

  /** The single instance; the class holds no state. */
  public static final LocalFileSystem INSTANCE = new LocalFileSystem();

  // Windows cannot open a directory as a FileChannel. NTFS journals metadata, so creates and
  // renames are already durable once they return, and there is nothing to sync.
  private static final boolean DIRECTORY_SYNC_SUPPORTED =
      !System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");

  private LocalFileSystem() {}

  @Override
  public FileHandle create(Path path) throws IOException {
    return new ChannelHandle(
        FileChannel.open(
            path,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE));
  }

  @Override
  public FileHandle open(Path path) throws IOException {
    return new ChannelHandle(
        FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE));
  }

  @Override
  public FileHandle openReadOnly(Path path) throws IOException {
    return new ChannelHandle(FileChannel.open(path, StandardOpenOption.READ));
  }

  @Override
  public boolean exists(Path path) {
    return Files.exists(path);
  }

  @Override
  public boolean deleteIfExists(Path path) throws IOException {
    return Files.deleteIfExists(path);
  }

  @Override
  public void rename(Path from, Path to) throws IOException {
    Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
  }

  @Override
  public List<Path> list(Path dir) throws IOException {
    try (Stream<Path> entries = Files.list(dir)) {
      return entries.sorted().toList();
    }
  }

  @Override
  public void createDirectories(Path dir) throws IOException {
    Files.createDirectories(dir);
  }

  @Override
  public void syncDirectory(Path dir) throws IOException {
    if (!DIRECTORY_SYNC_SUPPORTED) {
      return;
    }
    try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  private record ChannelHandle(FileChannel channel) implements FileHandle {

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
      return channel.read(dst, position);
    }

    @Override
    public void write(ByteBuffer src, long position) throws IOException {
      long pos = position;
      while (src.hasRemaining()) {
        pos += channel.write(src, pos);
      }
    }

    @Override
    public long size() throws IOException {
      return channel.size();
    }

    @Override
    public void truncate(long size) throws IOException {
      channel.truncate(size);
    }

    @Override
    public void sync() throws IOException {
      // force(true) also flushes the metadata needed to read the data back (for example the
      // file length after an append), which force(false) may skip.
      channel.force(true);
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}

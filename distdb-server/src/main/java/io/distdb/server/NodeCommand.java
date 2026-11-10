package io.distdb.server;

import io.distdb.env.ExecutorScheduler;
import io.distdb.env.LocalFileSystem;
import io.distdb.env.SystemClock;
import io.distdb.kv.KvService;
import io.distdb.kv.KvServiceOptions;
import io.distdb.rpc.grpc.GrpcKvServer;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;

/** {@code distdb node}: serves one data directory over gRPC until the process is stopped. */
final class NodeCommand {

  static final String DEFAULT_LISTEN = "127.0.0.1:7700";

  /** Printed once the node accepts requests; scripts and tests wait for it. */
  static final String READY = "distdb node listening on ";

  /** Exit status when the storage engine fails: the process stops at once (crash-only). */
  static final int STORAGE_FAILURE_EXIT = 70;

  private NodeCommand() {}

  static int run(List<String> argv, PrintStream out, PrintStream err)
      throws Args.UsageException, IOException, InterruptedException {
    Args args = Args.parse(argv, Set.of("data-dir", "listen"));
    if (!args.positional().isEmpty()) {
      throw new Args.UsageException("unexpected argument " + args.positional().getFirst());
    }
    Path dataDir = Path.of(args.required("data-dir")).toAbsolutePath();
    Args.HostPort listen = Args.hostPort(args.option("listen", DEFAULT_LISTEN));

    Files.createDirectories(dataDir);
    FileChannel lockFile =
        FileChannel.open(
            dataDir.resolve("LOCK"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    FileLock lock;
    try {
      lock = lockFile.tryLock();
    } catch (OverlappingFileLockException e) {
      lock = null;
    }
    if (lock == null) {
      lockFile.close();
      err.println("error: another node is already using " + dataDir);
      return 3;
    }

    ExecutorScheduler scheduler = new ExecutorScheduler();
    LsmEngine engine =
        LsmEngine.open(
            LocalFileSystem.INSTANCE,
            scheduler,
            dataDir.resolve("engine"),
            EngineOptions.defaults());
    KvService service =
        new KvService(
            engine,
            SystemClock.INSTANCE,
            scheduler,
            KvServiceOptions.defaults(),
            failure -> {
              // Carrying on after an I/O error risks serving or writing garbage. Stop now; a
              // restart recovers from what is safely on disk.
              err.println("fatal: storage engine failed, stopping: " + failure);
              err.flush();
              Runtime.getRuntime().halt(STORAGE_FAILURE_EXIT);
            });
    GrpcKvServer server =
        GrpcKvServer.start(
            new InetSocketAddress(listen.host(), listen.port()), service, SystemClock.INSTANCE);

    FileLock heldLock = lock;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  server.close(); // let requests in flight finish first
                  service.close();
                  engine.close();
                  scheduler.close();
                  try {
                    heldLock.release();
                    lockFile.close();
                  } catch (IOException e) {
                    // The process is exiting; the OS drops the lock anyway.
                  }
                },
                "distdb-shutdown"));

    out.println(READY + listen.host() + ":" + server.port() + " (data in " + dataDir + ")");
    out.flush();
    server.awaitTermination();
    return 0;
  }
}

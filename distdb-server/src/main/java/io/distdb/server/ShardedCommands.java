package io.distdb.server;

import io.distdb.client.ClientOptions;
import io.distdb.client.KvClient;
import io.distdb.common.NodeId;
import io.distdb.env.ExecutorScheduler;
import io.distdb.env.LocalFileSystem;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.SystemClock;
import io.distdb.kv.Ranges;
import io.distdb.kv.ReplicatedKv;
import io.distdb.kv.Store;
import io.distdb.pd.PlacementDriver;
import io.distdb.pgwire.PgServer;
import io.distdb.rpc.PdClient;
import io.distdb.rpc.grpc.GrpcKvChannel;
import io.distdb.rpc.grpc.GrpcKvServer;
import io.distdb.rpc.grpc.GrpcTransport;
import io.distdb.rpc.proto.ListRangesRequest;
import io.distdb.rpc.proto.ListRangesResponse;
import io.distdb.rpc.proto.PdRequest;
import io.distdb.rpc.proto.RangeInfo;
import io.distdb.sql.Engine;
import io.distdb.sql.kv.Awaiter;
import io.distdb.sql.kv.TxnKvStore;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import io.distdb.txn.GcWorker;
import io.distdb.txn.TxnClient;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The commands of a sharded cluster: {@code distdb pd} (a placement driver node), {@code distdb
 * node --pd ...} (a data node hosting many ranges), and {@code distdb ranges} (print the PD's
 * directory).
 */
final class ShardedCommands {

  private ShardedCommands() {}

  /** {@code distdb node --data-dir DIR --id N --listen HOST:PORT --pd PD,... [--founders 1,2,3]} */
  static int node(Args args, PrintStream out, PrintStream err) throws Exception {
    Path dataDir = Path.of(args.required("data-dir")).toAbsolutePath();
    NodeId self = NodeCommand.nodeId(args.required("id"));
    String listenText = args.option("listen", NodeCommand.DEFAULT_LISTEN);
    Args.HostPort listen = Args.hostPort(listenText);
    List<String> pdAddresses = List.of(args.required("pd").split(","));
    List<NodeId> founders = new ArrayList<>();
    String foundersText = args.option("founders", "");
    for (String f : foundersText.isEmpty() ? new String[0] : foundersText.split(",")) {
      founders.add(NodeCommand.nodeId(f.trim()));
    }
    DataDirLock lock = DataDirLock.acquire(dataDir, err);
    if (lock == null) {
      return 3;
    }
    ExecutorScheduler scheduler = new ExecutorScheduler();
    LsmEngine engine =
        LsmEngine.open(
            LocalFileSystem.INSTANCE,
            scheduler,
            dataDir.resolve("engine"),
            EngineOptions.defaults());
    // Peers' addresses come from the placement driver, through the store.
    AtomicReference<Store> holder = new AtomicReference<>();
    GrpcTransport transport =
        new GrpcTransport(
            self,
            id -> {
              Store s = holder.get();
              return s == null ? null : s.addressOf(id);
            });
    PdClient pd = new PdClient(pdAddresses, GrpcKvChannel::connect);
    Store store =
        new Store(
            Store.Config.defaults(self, listenText, founders),
            engine,
            transport,
            pd,
            SystemClock.INSTANCE,
            scheduler,
            SplittableRandomSource.fromEntropy(),
            failure -> halt(err, failure),
            null);
    holder.set(store);
    GrpcKvServer server =
        GrpcKvServer.start(
            new InetSocketAddress(listen.host(), listen.port()),
            store,
            SystemClock.INSTANCE,
            transport.service());
    store.start();
    // SQL: a PostgreSQL wire server whose sessions run Phase 7 transactions through this node.
    String sqlText = args.option("sql", null);
    PgServer sql = null;
    if (sqlText != null) {
      Args.HostPort sqlAt = Args.hostPort(sqlText);
      KvClient kv =
          new KvClient(
              pd,
              GrpcKvChannel::connect,
              SystemClock.INSTANCE,
              scheduler,
              SplittableRandomSource.fromEntropy(),
              ClientOptions.defaults());
      TxnClient txn =
          new TxnClient(
              kv,
              pd,
              SystemClock.INSTANCE,
              scheduler,
              SplittableRandomSource.fromEntropy(),
              TxnClient.Options.defaults());
      sql =
          PgServer.start(
              new InetSocketAddress(sqlAt.host(), sqlAt.port()),
              new Engine(new TxnKvStore(txn, Awaiter.blocking())));
    }
    PgServer sqlServer = sql;
    onShutdown(() -> {
      if (sqlServer != null) {
        try {
          sqlServer.close();
        } catch (IOException ignored) {
          // shutting down anyway
        }
      }
      server.close();
      store.close();
      pd.close();
      transport.close();
      engine.close();
      scheduler.close();
      lock.release();
    });
    out.println(
        NodeCommand.READY
            + listen.host()
            + ":"
            + server.port()
            + " as data node "
            + self.value()
            + " (data in "
            + dataDir
            + ")"
            + (sqlServer == null
                ? ""
                : ", SQL on " + Args.hostPort(sqlText).host() + ":" + sqlServer.port()));
    out.flush();
    server.awaitTermination();
    return 0;
  }

  /** {@code distdb pd --data-dir DIR --id N --peers 1=HOST:PORT,... [--split-keys N]} */
  static int pd(List<String> argv, PrintStream out, PrintStream err) throws Exception {
    Args args =
        Args.parse(
            argv,
            java.util.Set.of("data-dir", "id", "peers", "split-keys", "dead-after", "gc-lifetime"));
    if (!args.positional().isEmpty()) {
      throw new Args.UsageException("unexpected argument " + args.positional().getFirst());
    }
    Path dataDir = Path.of(args.required("data-dir")).toAbsolutePath();
    NodeId self = NodeCommand.nodeId(args.required("id"));
    Map<NodeId, String> peers = NodeCommand.parsePeers(args.required("peers"));
    if (!peers.containsKey(self)) {
      throw new Args.UsageException("--peers must include this node, " + self.value());
    }
    Args.HostPort listen = Args.hostPort(peers.get(self));
    PlacementDriver.Config defaults = PlacementDriver.Config.defaults(self, peers);
    long splitKeys = Long.parseLong(args.option("split-keys", "" + defaults.splitKeys()));
    Duration deadAfter = Args.duration(args.option("dead-after", "30s"));
    Duration gcLifetime = Args.duration(args.option("gc-lifetime", "10m"));
    DataDirLock lock = DataDirLock.acquire(dataDir, err);
    if (lock == null) {
      return 3;
    }
    ExecutorScheduler scheduler = new ExecutorScheduler();
    LsmEngine engine =
        LsmEngine.open(
            LocalFileSystem.INSTANCE,
            scheduler,
            dataDir.resolve("engine"),
            EngineOptions.defaults());
    GrpcTransport transport = new GrpcTransport(self, peers);
    ReplicatedKv kv =
        new ReplicatedKv(
            ReplicatedKv.Config.defaults(self, peers),
            engine,
            transport,
            SystemClock.INSTANCE,
            scheduler,
            SplittableRandomSource.fromEntropy(),
            failure -> halt(err, failure),
            null);
    PlacementDriver driver =
        new PlacementDriver(
            new PlacementDriver.Config(
                self,
                peers,
                defaults.replicas(),
                splitKeys,
                deadAfter,
                defaults.operatorTimeout(),
                defaults.maxOperators(),
                true),
            kv,
            SystemClock.INSTANCE,
            scheduler,
            SplittableRandomSource.fromEntropy());
    GrpcKvServer server =
        GrpcKvServer.start(
            new InetSocketAddress(listen.host(), listen.port()),
            driver,
            SystemClock.INSTANCE,
            transport.service());
    kv.start();
    // The GC worker runs on whichever PD node leads, and reaches the ranges like any client.
    PdClient selfPd = new PdClient(List.copyOf(peers.values()), GrpcKvChannel::connect);
    KvClient gcClient =
        new KvClient(
            selfPd,
            GrpcKvChannel::connect,
            SystemClock.INSTANCE,
            scheduler,
            SplittableRandomSource.fromEntropy(),
            ClientOptions.defaults());
    GcWorker gc =
        new GcWorker(
            new TxnClient(
                gcClient,
                selfPd,
                SystemClock.INSTANCE,
                scheduler,
                SplittableRandomSource.fromEntropy(),
                TxnClient.Options.defaults()),
            selfPd,
            scheduler,
            gcLifetime,
            Duration.ofSeconds(30),
            driver::isLeader);
    gc.start();
    onShutdown(() -> {
      gc.stop();
      gcClient.close();
      selfPd.close();
      server.close();
      driver.close();
      kv.close();
      transport.close();
      engine.close();
      scheduler.close();
      lock.release();
    });
    out.println(
        NodeCommand.READY
            + listen.host()
            + ":"
            + server.port()
            + " as placement driver node "
            + self.value()
            + " of "
            + peers.size());
    out.flush();
    server.awaitTermination();
    return 0;
  }

  /** {@code distdb ranges --pd PD,...}: the placement driver's directory, one range per line. */
  static int ranges(List<String> argv, PrintStream out, PrintStream err) throws Exception {
    Args args = Args.parse(argv, java.util.Set.of("pd", "timeout"));
    Duration timeout = Args.duration(args.option("timeout", "5s"));
    try (PdClient pd =
        new PdClient(List.of(args.required("pd").split(",")), GrpcKvChannel::connect)) {
      ListRangesResponse list = null;
      long deadline = System.nanoTime() + timeout.toNanos();
      while (list == null && System.nanoTime() < deadline) {
        try {
          list =
              pd.call(
                      PdRequest.newBuilder()
                          .setListRanges(ListRangesRequest.getDefaultInstance())
                          .build(),
                      Duration.ofSeconds(2))
                  .join()
                  .getListRanges();
        } catch (RuntimeException e) {
          Thread.sleep(200); // that PD node is down or not the leader: the client moved on
        }
      }
      if (list == null) {
        err.println("error: no placement driver answered");
        return KvCommand.FAILED;
      }
      for (RangeInfo r : list.getRangesList()) {
        out.println(Ranges.describe(r.getDesc()) + " leader " + r.getLeader());
      }
      return KvCommand.OK;
    }
  }

  private static void halt(PrintStream err, Throwable failure) {
    // Carrying on after an I/O error risks serving or writing garbage. Stop now; a restart
    // recovers from what is safely on disk (and the logs on the others).
    err.println("fatal: storage engine failed, stopping: " + failure);
    err.flush();
    Runtime.getRuntime().halt(NodeCommand.STORAGE_FAILURE_EXIT);
  }

  private static void onShutdown(Runnable r) {
    Runtime.getRuntime().addShutdownHook(new Thread(r, "distdb-shutdown"));
  }

  /** An OS lock on {@code DIR/LOCK}: one process per data directory. */
  static final class DataDirLock {
    private final FileChannel file;
    private final FileLock lock;

    private DataDirLock(FileChannel file, FileLock lock) {
      this.file = file;
      this.lock = lock;
    }

    /** The lock, or null (after printing why) if another process holds it. */
    static DataDirLock acquire(Path dataDir, PrintStream err) throws IOException {
      Files.createDirectories(dataDir);
      FileChannel file =
          FileChannel.open(
              dataDir.resolve("LOCK"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      FileLock lock;
      try {
        lock = file.tryLock();
      } catch (OverlappingFileLockException e) {
        lock = null;
      }
      if (lock == null) {
        file.close();
        err.println("error: another node is already using " + dataDir);
        return null;
      }
      return new DataDirLock(file, lock);
    }

    void release() {
      try {
        lock.release();
        file.close();
      } catch (IOException e) {
        // The process is exiting; the OS drops the lock anyway.
      }
    }
  }
}

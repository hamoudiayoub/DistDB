package io.distdb.server;

import io.distdb.common.NodeId;
import io.distdb.env.ExecutorScheduler;
import io.distdb.env.LocalFileSystem;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.SystemClock;
import io.distdb.kv.ReplicatedKv;
import io.distdb.rpc.grpc.GrpcKvServer;
import io.distdb.rpc.grpc.GrpcTransport;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@code distdb node}: one replica of the KV group, serving clients and talking Raft to its peers
 * over gRPC until the process is stopped. Without {@code --peers} it is a group of one.
 */
final class NodeCommand {

  static final String DEFAULT_LISTEN = "127.0.0.1:7700";

  /** Printed once the node accepts requests; scripts and tests wait for it. */
  static final String READY = "distdb node listening on ";

  /** Exit status when the storage engine fails: the process stops at once (crash-only). */
  static final int STORAGE_FAILURE_EXIT = 70;

  private NodeCommand() {}

  static int run(List<String> argv, PrintStream out, PrintStream err) throws Exception {
    Args args =
        Args.parse(
            argv,
            Set.of(
                "data-dir",
                "listen",
                "id",
                "peers",
                "pd",
                "founders",
                "sql",
                "http",
                "tune",
                "chaos"));
    if (!args.positional().isEmpty()) {
      throw new Args.UsageException("unexpected argument " + args.positional().getFirst());
    }
    if (args.option("pd", null) != null) {
      return ShardedCommands.node(args, out, err); // a data node of a sharded cluster
    }
    Path dataDir = Path.of(args.required("data-dir")).toAbsolutePath();
    NodeId self = nodeId(args.option("id", "1"));
    String peersText = args.option("peers", null);
    Map<NodeId, String> peers;
    String listenText;
    if (peersText == null) {
      listenText = args.option("listen", DEFAULT_LISTEN);
      peers = Map.of(self, listenText);
    } else {
      peers = parsePeers(peersText);
      if (!peers.containsKey(self)) {
        throw new Args.UsageException("--peers must include this node, " + self.value());
      }
      listenText = args.option("listen", peers.get(self));
    }
    Args.HostPort listen = Args.hostPort(listenText);

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
    GrpcTransport transport = new GrpcTransport(self, peers);
    ReplicatedKv replica =
        new ReplicatedKv(
            ReplicatedKv.Config.defaults(self, peers),
            engine,
            transport,
            SystemClock.INSTANCE,
            scheduler,
            SplittableRandomSource.fromEntropy(),
            failure -> {
              // Carrying on after an I/O error risks serving or writing garbage. Stop now; a
              // restart recovers from what is safely on disk (and the log on the others).
              err.println("fatal: storage engine failed, stopping: " + failure);
              err.flush();
              Runtime.getRuntime().halt(STORAGE_FAILURE_EXIT);
            },
            null);
    GrpcKvServer server =
        GrpcKvServer.start(
            new InetSocketAddress(listen.host(), listen.port()),
            replica,
            SystemClock.INSTANCE,
            transport.service());
    replica.start();

    FileLock heldLock = lock;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  server.close(); // let requests in flight finish first
                  replica.close();
                  transport.close();
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

    out.println(
        READY
            + listen.host()
            + ":"
            + server.port()
            + " as node "
            + self.value()
            + " of "
            + peers.size()
            + " (data in "
            + dataDir
            + ")");
    out.flush();
    server.awaitTermination();
    return 0;
  }

  /** {@code 1=host:port,2=host:port,...} */
  static Map<NodeId, String> parsePeers(String text) throws Args.UsageException {
    Map<NodeId, String> peers = new LinkedHashMap<>();
    for (String part : text.split(",")) {
      int eq = part.indexOf('=');
      if (eq <= 0) {
        throw new Args.UsageException("--peers wants id=host:port,...; got " + part);
      }
      NodeId id = nodeId(part.substring(0, eq).trim());
      String address = part.substring(eq + 1).trim();
      Args.hostPort(address); // validate
      if (peers.put(id, address) != null) {
        throw new Args.UsageException("node " + id.value() + " appears twice in --peers");
      }
    }
    return peers;
  }

  static NodeId nodeId(String text) throws Args.UsageException {
    try {
      return new NodeId(Integer.parseInt(text));
    } catch (IllegalArgumentException e) {
      throw new Args.UsageException("a node id is a positive number: " + text);
    }
  }
}

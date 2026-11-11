package io.distdb.server;

import io.distdb.client.ClientOptions;
import io.distdb.client.Entry;
import io.distdb.client.KvClient;
import io.distdb.client.KvException;
import io.distdb.env.ExecutorScheduler;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.SystemClock;
import io.distdb.rpc.grpc.GrpcKvChannel;
import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** {@code distdb kv}: one Get, Put, Delete or Scan against a node, from the command line. */
final class KvCommand {

  static final int OK = 0;
  static final int NOT_FOUND = 1;
  static final int FAILED = 3;

  /** A write failed but may have been applied: read the key to find out. */
  static final int OUTCOME_UNKNOWN = 4;

  private KvCommand() {}

  static int run(List<String> argv, PrintStream out, PrintStream err) throws Args.UsageException {
    Args args = Args.parse(argv, Set.of("server", "timeout", "limit"));
    String server = args.option("server", NodeCommand.DEFAULT_LISTEN);
    Duration timeout = Args.duration(args.option("timeout", "5s"));
    List<String> p = args.positional();
    if (p.isEmpty()) {
      throw new Args.UsageException("missing operation: get, put, delete or scan");
    }
    String op = p.getFirst();
    Operation operation =
        switch (op) {
          case "get" -> {
            expectArgs(p, 2, 2);
            byte[] key = ByteText.parse(p.get(1));
            yield client -> printValue(KvClient.await(client.get(key, timeout)), out);
          }
          case "put" -> {
            expectArgs(p, 3, 3);
            byte[] key = ByteText.parse(p.get(1));
            byte[] value = ByteText.parse(p.get(2));
            yield client -> {
              KvClient.await(client.put(key, value, timeout));
              out.println("OK");
              return OK;
            };
          }
          case "delete" -> {
            expectArgs(p, 2, 2);
            byte[] key = ByteText.parse(p.get(1));
            yield client -> {
              KvClient.await(client.delete(key, timeout));
              out.println("OK");
              return OK;
            };
          }
          case "scan" -> {
            expectArgs(p, 1, 3);
            byte[] start = p.size() > 1 ? ByteText.parse(p.get(1)) : new byte[0];
            byte[] end = p.size() > 2 ? ByteText.parse(p.get(2)) : null;
            int limit = parseLimit(args.option("limit", "100"));
            yield client -> {
              for (Entry e : KvClient.await(client.scan(start, end, limit, timeout))) {
                out.println(ByteText.format(e.key()) + "\t" + ByteText.format(e.value()));
              }
              return OK;
            };
          }
          default -> throw new Args.UsageException("unknown operation " + op);
        };

    // Any member will do: the client follows redirects to the leader.
    List<String> seeds = List.of(server.split(","));
    try (ExecutorScheduler scheduler = new ExecutorScheduler();
        KvClient client =
            new KvClient(
                seeds,
                GrpcKvChannel::connect,
                SystemClock.INSTANCE,
                scheduler,
                SplittableRandomSource.fromEntropy(),
                ClientOptions.defaults().withDefaultTimeout(timeout))) {
      return operation.run(client);
    } catch (KvException e) {
      err.println("error: " + e.getMessage());
      return e.outcomeUnknown() ? OUTCOME_UNKNOWN : FAILED;
    } catch (IllegalArgumentException e) {
      throw new Args.UsageException(e.getMessage());
    }
  }

  @FunctionalInterface
  private interface Operation {
    int run(KvClient client);
  }

  private static int printValue(Optional<byte[]> value, PrintStream out) {
    if (value.isEmpty()) {
      out.println("(not found)");
      return NOT_FOUND;
    }
    out.println(ByteText.format(value.get()));
    return OK;
  }

  private static void expectArgs(List<String> p, int min, int max) throws Args.UsageException {
    if (p.size() < min || p.size() > max) {
      throw new Args.UsageException("wrong number of arguments for " + p.getFirst());
    }
  }

  private static int parseLimit(String text) throws Args.UsageException {
    try {
      int n = Integer.parseInt(text);
      if (n >= 1) {
        return n;
      }
    } catch (NumberFormatException e) {
      // fall through
    }
    throw new Args.UsageException("--limit must be a positive number: " + text);
  }
}

package io.distdb.server;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

/** The {@code distdb} binary. */
public final class Main {

  static final int USAGE = 2;

  static final String HELP =
      """
      usage:
        distdb node --data-dir DIR [--listen HOST:PORT]
            Serve DIR on HOST:PORT (default %1$s) until stopped, as a group of one.

        distdb node --data-dir DIR --id N --peers 1=HOST:PORT,2=HOST:PORT,3=HOST:PORT
            Run node N of a replicated group; it listens on its own entry in --peers.

      A sharded cluster: a placement driver of three nodes, and data nodes that it fills.

        distdb pd --data-dir DIR --id N --peers 1=HOST:PORT,2=HOST:PORT,3=HOST:PORT
                  [--split-keys N] [--dead-after DURATION] [--gc-lifetime DURATION]
            Run node N of the placement driver; it listens on its own entry in --peers.

        distdb node --data-dir DIR --id N --listen HOST:PORT --pd PD_ADDRESSES [--founders 1,2,3]
            Run data node N. The founders create the first range when the cluster is new;
            the placement driver then splits ranges and spreads them over all data nodes.

        distdb ranges --pd PD_ADDRESSES
            Print every range: its span, epoch, replicas and leader.

        distdb kv [--server HOST:PORT[,HOST:PORT...] | --pd PD_ADDRESSES] [--timeout DURATION] OPERATION
            get KEY                         print the value (exit 1 if there is none)
            put KEY VALUE                   set KEY to VALUE
            delete KEY                      remove KEY
            scan [START [END]] [--limit N]  print up to N entries (default 100) in [START, END)

      Keys and values are UTF-8 text; write \\xNN for any byte and \\\\ for a backslash.
      DURATION looks like 500ms, 5s or 2m (default 5s).
      Exit status: 0 ok, 1 not found, 2 usage error, 3 failed,
                   4 a write failed but may have been applied.
      """
          .formatted(NodeCommand.DEFAULT_LISTEN);

  private Main() {}

  public static void main(String[] args) {
    int status = run(Arrays.asList(args), System.out, System.err);
    System.out.flush();
    System.exit(status);
  }

  /** Runs one command and returns its exit status. {@code node} returns only after shutdown. */
  static int run(List<String> args, PrintStream out, PrintStream err) {
    if (args.isEmpty() || args.getFirst().equals("--help") || args.getFirst().equals("help")) {
      out.print(HELP);
      return args.isEmpty() ? USAGE : 0;
    }
    List<String> rest = args.subList(1, args.size());
    try {
      return switch (args.getFirst()) {
        case "node" -> NodeCommand.run(rest, out, err);
        case "kv" -> KvCommand.run(rest, out, err);
        case "pd" -> ShardedCommands.pd(rest, out, err);
        case "ranges" -> ShardedCommands.ranges(rest, out, err);
        default -> throw new Args.UsageException("unknown command " + args.getFirst());
      };
    } catch (Args.UsageException | IllegalArgumentException e) {
      err.println("error: " + e.getMessage());
      err.println();
      err.print(HELP);
      return USAGE;
    } catch (Exception e) {
      err.println("error: " + e);
      return KvCommand.FAILED;
    }
  }
}

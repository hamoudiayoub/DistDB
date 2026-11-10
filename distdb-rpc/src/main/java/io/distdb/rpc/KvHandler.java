package io.distdb.rpc;

import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import java.util.concurrent.CompletableFuture;

/**
 * The server side of the KV API: what a node does with one request, independent of how the request
 * arrived. The gRPC server calls it for real traffic; the simulator calls it directly.
 */
@FunctionalInterface
public interface KvHandler {

  /** Means the caller set no deadline. */
  long NO_DEADLINE = Long.MAX_VALUE;

  /**
   * Handles one request.
   *
   * <p>The future completes with a response, which may carry a typed {@link
   * io.distdb.rpc.proto.Error}. It completes exceptionally only on a bug; the transport reports
   * that to the caller as a failed call.
   *
   * @param deadlineNanos when the caller stops waiting, on the scale of this node's {@link
   *     io.distdb.env.Clock#nanoTime()}, or {@link #NO_DEADLINE}. Deadlines cross the network as
   *     time remaining, never as a clock reading, because clocks on different machines disagree.
   */
  CompletableFuture<KvResponse> handle(KvRequest request, long deadlineNanos);
}

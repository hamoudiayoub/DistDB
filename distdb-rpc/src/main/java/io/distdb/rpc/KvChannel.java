package io.distdb.rpc;

import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * The client side of the KV API: a way to send one request to one node and get its answer. It makes
 * exactly one attempt; retries, backoff and request ids are the job of {@code KvClient} in {@code
 * distdb-client}, which is written against this interface so that the simulator can substitute a
 * lossy in-memory channel.
 */
public interface KvChannel extends AutoCloseable {

  /**
   * Sends {@code request} once. The future completes with the server's response (which may carry a
   * typed error), or exceptionally with an {@link RpcException} if no response arrived within
   * {@code timeout}. Never blocks.
   *
   * <p>A failed call has an <b>unknown outcome</b>: the request may have reached the server and
   * been applied, and only the response was lost.
   */
  CompletableFuture<KvResponse> call(KvRequest request, Duration timeout);

  @Override
  void close();
}

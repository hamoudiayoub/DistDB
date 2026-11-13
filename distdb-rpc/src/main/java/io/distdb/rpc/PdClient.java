package io.distdb.rpc;

import io.distdb.rpc.proto.Error;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.PdRequest;
import io.distdb.rpc.proto.PdResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Talks to the placement driver, whichever of its nodes leads. It sends to the node it believes
 * leads; a {@code NotLeader} answer that names the leader sends it straight there (once per call),
 * and anything else makes it try the next PD node on the following call.
 *
 * <p>One call is one attempt (plus at most one redirect): stores retry on their heartbeat interval,
 * and clients within their own retry loop. Thread-safe.
 */
public final class PdClient implements AutoCloseable {

  private final List<String> seeds;
  private final Function<String, KvChannel> connect;
  private final Map<String, KvChannel> channels = new ConcurrentHashMap<>();
  private volatile String target;

  /**
   * @param seeds the PD nodes' addresses
   * @param connect opens a channel to one of them
   */
  public PdClient(List<String> seeds, Function<String, KvChannel> connect) {
    if (seeds.isEmpty()) {
      throw new IllegalArgumentException("at least one PD address is needed");
    }
    this.seeds = List.copyOf(seeds);
    this.connect = Objects.requireNonNull(connect);
    this.target = this.seeds.getFirst();
  }

  /** Sends {@code request} to the PD leader; fails with an {@link RpcException} if none answers. */
  public CompletableFuture<PdResponse> call(PdRequest request, Duration timeout) {
    return attempt(request, timeout, true);
  }

  private CompletableFuture<PdResponse> attempt(
      PdRequest request, Duration timeout, boolean mayRedirect) {
    String to = target;
    CompletableFuture<KvResponse> answer;
    try {
      answer =
          channels
              .computeIfAbsent(to, connect)
              .call(KvRequest.newBuilder().setPd(request).build(), timeout);
    } catch (RuntimeException e) {
      answer = CompletableFuture.failedFuture(e);
    }
    return answer
        .handle((response, failure) -> {
          if (failure != null) {
            moveOn(to);
            return CompletableFuture.<PdResponse>failedFuture(failure);
          }
          if (response.hasPd()) {
            return CompletableFuture.completedFuture(response.getPd());
          }
          Error error = response.getError();
          if (error.hasNotLeader()) {
            String hint = error.getNotLeader().getLeaderAddress();
            if (mayRedirect && !hint.isEmpty() && !hint.equals(to)) {
              synchronized (this) {
                target = hint;
              }
              return attempt(request, timeout, false);
            }
          }
          moveOn(to);
          return CompletableFuture.<PdResponse>failedFuture(
              new RpcException(
                  RpcException.Kind.UNAVAILABLE,
                  false,
                  "PD at " + to + ": " + error.getKindCase() + " " + error.getMessage(),
                  null));
        })
        .thenCompose(f -> f);
  }

  private synchronized void moveOn(String failed) {
    if (target.equals(failed)) {
      target = seeds.get((seeds.indexOf(failed) + 1) % seeds.size());
    }
  }

  /** Closes the channels it opened. */
  @Override
  public void close() {
    channels.values().forEach(KvChannel::close);
    channels.clear();
  }
}

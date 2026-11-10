package io.distdb.rpc.grpc;

import io.distdb.rpc.KvChannel;
import io.distdb.rpc.RpcException;
import io.distdb.rpc.proto.KvGrpc;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.grpc.CallOptions;
import io.grpc.ClientStreamTracer;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link KvChannel} to one node over gRPC. One HTTP/2 connection carries any number of concurrent
 * calls; gRPC reconnects on its own after a failure. Thread-safe.
 */
public final class GrpcKvChannel implements KvChannel {

  private final String target;
  private final ManagedChannel channel;

  private GrpcKvChannel(String target, ManagedChannel channel) {
    this.target = target;
    this.channel = channel;
  }

  /**
   * A channel to {@code target} ({@code host:port}). It connects lazily, on the first call, so this
   * succeeds even if the node is down.
   */
  public static GrpcKvChannel connect(String target) {
    // Plaintext: authentication and TLS are out of scope, which is why a node listens on
    // localhost unless told otherwise.
    return new GrpcKvChannel(
        target, Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build());
  }

  @Override
  public CompletableFuture<KvResponse> call(KvRequest request, Duration timeout) {
    CompletableFuture<KvResponse> result = new CompletableFuture<>();
    // Set when the request is handed to a real connection. A call that fails before that (no
    // connection could be made, or the deadline passed while connecting) was never sent, and the
    // client may then report a write as certainly not applied.
    AtomicBoolean sent = new AtomicBoolean();
    ClientStreamTracer.Factory tracer =
        new ClientStreamTracer.Factory() {
          @Override
          public ClientStreamTracer newClientStreamTracer(
              ClientStreamTracer.StreamInfo info, Metadata headers) {
            return new ClientStreamTracer() {
              @Override
              public void outboundMessage(int seqNo) {
                sent.set(true);
              }
            };
          }
        };
    // The deadline travels to the server in the grpc-timeout header, so the server can skip work
    // nobody will wait for.
    CallOptions options =
        CallOptions.DEFAULT
            .withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
            .withStreamTracerFactory(tracer);
    ClientCalls.asyncUnaryCall(
        channel.newCall(KvGrpc.getCallMethod(), options),
        request,
        new StreamObserver<>() {
          @Override
          public void onNext(KvResponse response) {
            result.complete(response);
          }

          @Override
          public void onError(Throwable t) {
            result.completeExceptionally(toRpcException(t, sent.get()));
          }

          @Override
          public void onCompleted() {
            if (!result.isDone()) {
              result.completeExceptionally(
                  new RpcException(RpcException.Kind.FAILED, "no response from " + target));
            }
          }
        });
    return result;
  }

  private RpcException toRpcException(Throwable t, boolean sent) {
    Status status = Status.fromThrowable(t);
    RpcException.Kind kind =
        switch (status.getCode()) {
          case DEADLINE_EXCEEDED -> RpcException.Kind.TIMEOUT;
          // CANCELLED here means the connection went away under the call (e.g. server shutdown).
          case UNAVAILABLE, CANCELLED -> RpcException.Kind.UNAVAILABLE;
          default -> RpcException.Kind.FAILED;
        };
    String description = status.getDescription() == null ? "" : ": " + status.getDescription();
    return new RpcException(kind, sent, target + " " + status.getCode() + description, t);
  }

  @Override
  public void close() {
    channel.shutdownNow();
    try {
      channel.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public String toString() {
    return "GrpcKvChannel[" + target + "]";
  }
}

package io.distdb.rpc.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.distdb.env.SystemClock;
import io.distdb.rpc.KvHandler;
import io.distdb.rpc.RpcException;
import io.distdb.rpc.proto.GetRequest;
import io.distdb.rpc.proto.GetResponse;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The real gRPC transport over localhost TCP. */
class GrpcKvTest {

  private static final KvRequest GET =
      KvRequest.newBuilder()
          .setGet(GetRequest.newBuilder().setKey(ByteString.copyFromUtf8("k")))
          .build();

  private GrpcKvServer server;
  private GrpcKvChannel channel;

  @AfterEach
  void tearDown() {
    if (channel != null) {
      channel.close();
    }
    if (server != null) {
      server.close(Duration.ofSeconds(1));
    }
  }

  private void serve(KvHandler handler) {
    server =
        GrpcKvServer.start(new InetSocketAddress("127.0.0.1", 0), handler, SystemClock.INSTANCE);
    channel = GrpcKvChannel.connect("127.0.0.1:" + server.port());
  }

  @Test
  void aRequestAndItsResponseCrossTheNetwork() {
    serve((request, deadline) ->
        CompletableFuture.completedFuture(
            KvResponse.newBuilder()
                .setGet(
                    GetResponse.newBuilder()
                        .setFound(true)
                        .setValue(request.getGet().getKey().concat(ByteString.copyFromUtf8("!"))))
                .build()));
    KvResponse response = channel.call(GET, Duration.ofSeconds(10)).join();
    assertThat(response.getGet().getValue().toStringUtf8()).isEqualTo("k!");
  }

  @Test
  void theCallersDeadlineReachesTheHandlerAsTimeRemaining() {
    AtomicLong remaining = new AtomicLong();
    serve((request, deadline) -> {
      remaining.set(deadline - SystemClock.INSTANCE.nanoTime());
      return CompletableFuture.completedFuture(KvResponse.getDefaultInstance());
    });
    channel.call(GET, Duration.ofSeconds(30)).join();
    assertThat(remaining.get())
        .isBetween(Duration.ofSeconds(20).toNanos(), Duration.ofSeconds(30).toNanos());
  }

  @Test
  void aCallWithNoAnswerTimesOutAndMayHaveBeenDelivered() throws Exception {
    AtomicLong calls = new AtomicLong();
    CountDownLatch arrived = new CountDownLatch(1);
    serve((request, deadline) -> {
      if (calls.incrementAndGet() == 1) {
        return CompletableFuture.completedFuture(KvResponse.getDefaultInstance());
      }
      arrived.countDown();
      return new CompletableFuture<>(); // never answers
    });
    // Connect first. Otherwise connection setup can eat the whole 300 ms, and the request is never
    // sent at all: also a timeout, and the client cannot tell the two apart.
    channel.call(GET, Duration.ofSeconds(10)).join();
    RpcException e = failure(channel.call(GET, Duration.ofMillis(300)));
    assertThat(e.kind()).isEqualTo(RpcException.Kind.TIMEOUT);
    assertThat(e.maybeDelivered()).isTrue();
    assertThat(arrived.await(5, TimeUnit.SECONDS))
        .as("it was in fact delivered: a timeout cannot tell")
        .isTrue();
  }

  @Test
  void aRefusedConnectionIsKnownNotToHaveDeliveredAnything() throws Exception {
    int port;
    try (ServerSocket s = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
      port = s.getLocalPort();
    } // now nothing listens there
    channel = GrpcKvChannel.connect("127.0.0.1:" + port);
    RpcException e = failure(channel.call(GET, Duration.ofSeconds(10)));
    assertThat(e.kind()).isEqualTo(RpcException.Kind.UNAVAILABLE);
    assertThat(e.maybeDelivered()).isFalse();
  }

  @Test
  void aCallThatTimesOutWhileStillConnectingWasNeverSent() throws Exception {
    // Accepts TCP connections but never speaks HTTP/2, so no connection ever becomes usable.
    try (ServerSocket silent = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
      Thread.ofVirtual()
          .start(() -> {
            try {
              while (true) {
                silent.accept(); // hold the socket open and say nothing
              }
            } catch (java.io.IOException closed) {
              // test over
            }
          });
      channel = GrpcKvChannel.connect("127.0.0.1:" + silent.getLocalPort());
      RpcException e = failure(channel.call(GET, Duration.ofMillis(500)));
      assertThat(e.kind()).isEqualTo(RpcException.Kind.TIMEOUT);
      assertThat(e.maybeDelivered()).isFalse();
    }
  }

  @Test
  void aHandlerFailureIsReportedAsAFailedCall() {
    serve((request, deadline) -> {
      throw new IllegalStateException("bug");
    });
    RpcException e = failure(channel.call(GET, Duration.ofSeconds(10)));
    assertThat(e.kind()).isEqualTo(RpcException.Kind.FAILED);
    assertThat(e.kind().retryable()).isFalse();
    assertThat(e.maybeDelivered()).isTrue();
  }

  @Test
  void manyConcurrentCallsShareOneConnection() {
    serve(
        (request, deadline) -> CompletableFuture.completedFuture(KvResponse.getDefaultInstance()));
    CompletableFuture<?>[] calls = new CompletableFuture<?>[500];
    for (int i = 0; i < calls.length; i++) {
      calls[i] = channel.call(GET, Duration.ofSeconds(30));
    }
    CompletableFuture.allOf(calls).join();
  }

  private static RpcException failure(CompletableFuture<KvResponse> call) {
    try {
      call.join();
    } catch (CompletionException e) {
      assertThat(e.getCause()).isInstanceOf(RpcException.class);
      return (RpcException) e.getCause();
    }
    throw new AssertionError("the call succeeded");
  }
}

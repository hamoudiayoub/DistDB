package io.distdb.rpc.grpc;

import io.distdb.env.Clock;
import io.distdb.rpc.KvHandler;
import io.distdb.rpc.proto.KvGrpc;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Serves a {@link KvHandler} over gRPC. Each call runs on its own virtual thread, so a handler may
 * block on disk I/O (an fsync) without holding up the network threads.
 */
public final class GrpcKvServer implements AutoCloseable {

  private static final System.Logger LOG = System.getLogger(GrpcKvServer.class.getName());

  private final Server server;
  private final ExecutorService executor;

  private GrpcKvServer(Server server, ExecutorService executor) {
    this.server = server;
    this.executor = executor;
  }

  /**
   * Starts serving on {@code address}. Port 0 picks a free port; {@link #port()} says which.
   *
   * @param clock the clock the handler's deadlines are expressed on
   * @param others more services on the same port, such as {@link GrpcTransport#service()}
   */
  public static GrpcKvServer start(
      InetSocketAddress address, KvHandler handler, Clock clock, BindableService... others) {
    Objects.requireNonNull(handler);
    Objects.requireNonNull(clock);
    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    NettyServerBuilder builder =
        NettyServerBuilder.forAddress(address)
            .executor(executor)
            .addService(new Service(handler, clock));
    for (BindableService s : others) {
      builder.addService(s);
    }
    Server server = builder.build();
    try {
      server.start();
    } catch (IOException e) {
      executor.close();
      throw new UncheckedIOException("cannot listen on " + address, e);
    }
    return new GrpcKvServer(server, executor);
  }

  /** The port actually bound. */
  public int port() {
    return server.getPort();
  }

  /** Blocks until the server has shut down. */
  public void awaitTermination() throws InterruptedException {
    server.awaitTermination();
  }

  /**
   * Stops accepting calls, gives the ones in flight up to {@code grace} to finish, then cancels the
   * rest.
   */
  public void close(Duration grace) {
    server.shutdown();
    try {
      if (!server.awaitTermination(grace.toNanos(), TimeUnit.NANOSECONDS)) {
        server.shutdownNow();
        server.awaitTermination(1, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      server.shutdownNow();
      Thread.currentThread().interrupt();
    }
    executor.shutdownNow();
  }

  @Override
  public void close() {
    close(Duration.ofSeconds(5));
  }

  private static final class Service extends KvGrpc.KvImplBase {
    private final KvHandler handler;
    private final Clock clock;

    Service(KvHandler handler, Clock clock) {
      this.handler = handler;
      this.clock = clock;
    }

    @Override
    public void call(KvRequest request, StreamObserver<KvResponse> responses) {
      // gRPC carries the deadline as time remaining (the grpc-timeout header); turn it back into
      // a point on this node's clock.
      Deadline deadline = Context.current().getDeadline();
      long deadlineNanos = KvHandler.NO_DEADLINE;
      if (deadline != null) {
        try {
          deadlineNanos =
              Math.addExact(clock.nanoTime(), deadline.timeRemaining(TimeUnit.NANOSECONDS));
        } catch (ArithmeticException farFuture) {
          // Leave it as NO_DEADLINE.
        }
      }
      CompletableFuture<KvResponse> result;
      try {
        result = handler.handle(request, deadlineNanos);
      } catch (RuntimeException e) {
        result = CompletableFuture.failedFuture(e);
      }
      result.whenComplete((response, failure) -> reply(responses, response, failure));
    }

    private static void reply(
        StreamObserver<KvResponse> responses, KvResponse response, Throwable failure) {
      if (((ServerCallStreamObserver<KvResponse>) responses).isCancelled()) {
        return; // The client gave up (deadline, or it went away); nobody is listening.
      }
      try {
        if (failure != null) {
          LOG.log(System.Logger.Level.ERROR, "KV handler failed", failure);
          responses.onError(
              Status.INTERNAL
                  .withDescription("server failed on the request: " + failure)
                  .asRuntimeException());
        } else {
          responses.onNext(response);
          responses.onCompleted();
        }
      } catch (StatusRuntimeException e) {
        // Cancelled between the check and the reply: same as above.
      }
    }
  }
}

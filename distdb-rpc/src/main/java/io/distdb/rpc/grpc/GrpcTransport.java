package io.distdb.rpc.grpc;

import com.google.protobuf.ByteString;
import io.distdb.common.NodeId;
import io.distdb.env.Transport;
import io.distdb.rpc.proto.RaftAck;
import io.distdb.rpc.proto.RaftFrame;
import io.distdb.rpc.proto.RaftTransportGrpc;
import io.grpc.BindableService;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The production {@link Transport} for Raft: one long-lived gRPC stream to each peer, carrying
 * {@link io.distdb.raft.RaftCodec}-encoded messages one way. Raft tolerates lost messages, so when
 * a stream breaks (the peer is down, say) the messages on it are simply lost and the next send
 * opens a new one. Register {@link #service()} on this node's gRPC server to receive.
 *
 * <p>Thread-safe. Not flow-controlled: a slow peer's messages queue in memory until its stream
 * fails.
 */
public final class GrpcTransport implements Transport, AutoCloseable {

  private static final System.Logger LOG = System.getLogger(GrpcTransport.class.getName());

  private final NodeId self;
  private final Map<NodeId, String> addresses;
  private final Map<NodeId, ManagedChannel> channels = new HashMap<>();
  private final Map<NodeId, StreamObserver<RaftFrame>> streams = new HashMap<>();
  private volatile MessageHandler handler;

  /**
   * @param self this node
   * @param addresses {@code host:port} of every peer (this node may be included; it is ignored)
   */
  public GrpcTransport(NodeId self, Map<NodeId, String> addresses) {
    this.self = Objects.requireNonNull(self);
    this.addresses = Map.copyOf(addresses);
  }

  @Override
  public NodeId localNode() {
    return self;
  }

  @Override
  public void setHandler(MessageHandler handler) {
    this.handler = handler;
  }

  @Override
  public synchronized void send(NodeId to, byte[] message) {
    StreamObserver<RaftFrame> stream = streams.get(to);
    if (stream == null) {
      String address = addresses.get(to);
      if (address == null) {
        return; // unknown peer: drop, as a network would
      }
      ManagedChannel channel =
          channels.computeIfAbsent(
              to,
              n -> Grpc.newChannelBuilder(address, InsecureChannelCredentials.create()).build());
      stream = RaftTransportGrpc.newStub(channel).send(new Closer(to));
      streams.put(to, stream);
    }
    stream.onNext(
        RaftFrame.newBuilder()
            .setFrom(self.value())
            .setTo(to.value())
            .setMessage(ByteString.copyFrom(message))
            .build());
  }

  /** The receiving side, to register on this node's gRPC server. */
  public BindableService service() {
    return new RaftTransportGrpc.RaftTransportImplBase() {
      @Override
      public StreamObserver<RaftFrame> send(StreamObserver<RaftAck> ack) {
        return new StreamObserver<>() {
          @Override
          public void onNext(RaftFrame frame) {
            MessageHandler h = handler;
            if (h != null && frame.getTo() == self.value() && frame.getFrom() > 0) {
              h.onMessage(new NodeId(frame.getFrom()), frame.getMessage().toByteArray());
            }
          }

          @Override
          public void onError(Throwable t) {
            // The sender went away; it will open a new stream.
          }

          @Override
          public void onCompleted() {
            ack.onNext(RaftAck.getDefaultInstance());
            ack.onCompleted();
          }
        };
      }
    };
  }

  /** Forgets a broken stream, so the next send opens a new one. */
  private final class Closer implements StreamObserver<RaftAck> {
    private final NodeId peer;

    Closer(NodeId peer) {
      this.peer = peer;
    }

    @Override
    public void onNext(RaftAck value) {}

    @Override
    public void onError(Throwable t) {
      LOG.log(System.Logger.Level.DEBUG, "raft stream to {0} broke: {1}", peer, t.toString());
      synchronized (GrpcTransport.this) {
        streams.remove(peer);
      }
    }

    @Override
    public void onCompleted() {
      synchronized (GrpcTransport.this) {
        streams.remove(peer);
      }
    }
  }

  @Override
  public synchronized void close() {
    streams
        .values()
        .forEach(s -> {
          try {
            s.onCompleted();
          } catch (RuntimeException ignored) {
            // already broken
          }
        });
    streams.clear();
    for (ManagedChannel c : channels.values()) {
      c.shutdownNow();
      try {
        c.awaitTermination(2, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    channels.clear();
  }
}

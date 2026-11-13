package io.distdb.pgwire;

import io.distdb.sql.Engine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A PostgreSQL-compatible server: {@code psql}, JDBC and other PostgreSQL drivers connect to it.
 * Each connection gets a virtual thread and a {@link io.distdb.sql.Session}; the SQL code blocks on
 * the cluster's futures, which a virtual thread does cheaply.
 */
public final class PgServer implements AutoCloseable {

  private final ServerSocket socket;
  private final Engine engine;
  private final Thread acceptor;
  private final AtomicInteger connections = new AtomicInteger();
  private volatile boolean closed;

  private PgServer(ServerSocket socket, Engine engine) {
    this.socket = socket;
    this.engine = engine;
    this.acceptor = Thread.ofVirtual().name("pgwire-accept").unstarted(this::accept);
  }

  /** Listens on {@code address} (port 0: any free port) and starts accepting connections. */
  public static PgServer start(InetSocketAddress address, Engine engine) throws IOException {
    ServerSocket socket = new ServerSocket();
    socket.setReuseAddress(true);
    socket.bind(address);
    PgServer server = new PgServer(socket, Objects.requireNonNull(engine));
    server.acceptor.start();
    return server;
  }

  public int port() {
    return socket.getLocalPort();
  }

  private void accept() {
    while (!closed) {
      try {
        Socket s = socket.accept();
        s.setTcpNoDelay(true);
        int pid = connections.incrementAndGet();
        Thread.ofVirtual().name("pgwire-" + pid).start(new PgConnection(s, engine, pid));
      } catch (IOException e) {
        if (closed) {
          return;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
    socket.close();
  }
}

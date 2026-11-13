package io.distdb.pgwire;

import io.distdb.sql.Engine;
import io.distdb.sql.Prepared;
import io.distdb.sql.Result;
import io.distdb.sql.Session;
import io.distdb.sql.SqlException;
import io.distdb.sql.SqlState;
import io.distdb.sql.types.Type;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One client connection, speaking PostgreSQL's frontend/backend protocol v3: startup (TLS and GSS
 * requests are declined, authentication is trust), the simple query protocol ({@code Q}), and the
 * extended one ({@code P}arse, {@code B}ind, {@code D}escribe, {@code E}xecute, {@code S}ync).
 * After an error in an extended-protocol exchange, messages are skipped up to the next Sync.
 */
final class PgConnection implements Runnable {

  private static final int SSL_REQUEST = 80877103;
  private static final int GSS_REQUEST = 80877104;
  private static final int CANCEL_REQUEST = 80877102;
  private static final int PROTOCOL_3 = 3 << 16;

  /** A bound statement: its parameters, and once executed, the rows still to send. */
  private static final class Portal {
    final Prepared prepared;
    final List<Object> params;
    final int[] resultFormats;
    Result result;
    int sent;

    Portal(Prepared prepared, List<Object> params, int[] resultFormats) {
      this.prepared = prepared;
      this.params = params;
      this.resultFormats = resultFormats;
    }
  }

  private final Socket socket;
  private final Engine engine;
  private final int pid;
  private final DataInputStream in;
  private final OutputStream out;
  private Session session;
  private final Map<String, Prepared> statements = new HashMap<>();
  private final Map<String, int[]> statementOids = new HashMap<>();
  private final Map<String, Portal> portals = new HashMap<>();
  private boolean skipToSync;

  PgConnection(Socket socket, Engine engine, int pid) throws IOException {
    this.socket = socket;
    this.engine = engine;
    this.pid = pid;
    this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
    this.out = new BufferedOutputStream(socket.getOutputStream());
  }

  @Override
  public void run() {
    try (socket) {
      if (!startup()) {
        return;
      }
      session = engine.session();
      readyForQuery();
      while (true) {
        int type = in.read();
        if (type < 0) {
          return;
        }
        int length = in.readInt();
        byte[] body = new byte[length - 4];
        in.readFully(body);
        if (type == 'X') {
          return;
        }
        handle(type, ByteBuffer.wrap(body));
      }
    } catch (EOFException | java.net.SocketException e) {
      // the client went away
    } catch (IOException e) {
      // nothing to tell a client we cannot write to
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Startup

  /** Reads the startup packets; false when the connection should close (a cancel request). */
  private boolean startup() throws IOException {
    while (true) {
      int length = in.readInt();
      int code = in.readInt();
      byte[] rest = new byte[length - 8];
      in.readFully(rest);
      if (code == SSL_REQUEST || code == GSS_REQUEST) {
        out.write('N'); // no encryption: the client continues in the clear
        out.flush();
        continue;
      }
      if (code == CANCEL_REQUEST) {
        return false; // queries are short; there is nothing to cancel
      }
      if ((code >>> 16) != 3) {
        error(
            SqlState.PROTOCOL_VIOLATION,
            "unsupported frontend protocol " + (code >>> 16) + "." + (code & 0xFFFF));
        out.flush();
        return false;
      }
      List<String> options = new ArrayList<>();
      ByteBuffer b = ByteBuffer.wrap(rest);
      while (b.hasRemaining()) {
        String key = cstring(b);
        if (key.isEmpty()) {
          break;
        }
        cstring(b);
        if (key.startsWith("_pq_.")) {
          options.add(key);
        }
      }
      if ((code & 0xFFFF) != 0 || !options.isEmpty()) {
        // Offer protocol 3.0 and none of the client's protocol options.
        Message m = new Message('v');
        m.int32(PROTOCOL_3);
        m.int32(options.size());
        for (String o : options) {
          m.cstring(o);
        }
        m.send(out);
      }
      Message ok = new Message('R');
      ok.int32(0); // AuthenticationOk
      ok.send(out);
      for (Map.Entry<String, String> e : Session.startupSettings().entrySet()) {
        Message s = new Message('S');
        s.cstring(e.getKey());
        s.cstring(e.getValue());
        s.send(out);
      }
      Message key = new Message('K');
      key.int32(pid);
      key.int32(pid * 7919);
      key.send(out);
      return true;
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Messages

  private void handle(int type, ByteBuffer b) throws IOException {
    if (skipToSync && type != 'S') {
      return;
    }
    try {
      switch (type) {
        case 'Q' -> simpleQuery(cstring(b));
        case 'P' -> parse(b);
        case 'B' -> bind(b);
        case 'D' -> describe(b);
        case 'E' -> execute(b);
        case 'C' -> close(b);
        case 'S' -> {
          skipToSync = false;
          readyForQuery();
        }
        case 'H' -> out.flush();
        default ->
            throw new SqlException(
                SqlState.PROTOCOL_VIOLATION, "unsupported message type '" + (char) type + "'");
      }
    } catch (SqlException e) {
      error(e.state(), e.getMessage());
      if (type == 'Q') {
        readyForQuery();
      } else {
        skipToSync = true;
        out.flush();
      }
    } catch (RuntimeException e) {
      error(SqlState.INTERNAL_ERROR, "internal error: " + e);
      if (type == 'Q') {
        readyForQuery();
      } else {
        skipToSync = true;
        out.flush();
      }
    }
  }

  private void simpleQuery(String sql) throws IOException {
    int[] results = {0};
    session.execute(
        sql,
        r -> {
          results[0]++;
          try {
            sendResult(r, null, 0, true);
          } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
          }
        });
    if (results[0] == 0) {
      new Message('I').send(out); // EmptyQueryResponse: only spaces or comments
    }
    readyForQuery();
  }

  private void parse(ByteBuffer b) throws IOException {
    String name = cstring(b);
    String sql = cstring(b);
    int n = b.getShort();
    int[] oids = new int[n];
    List<Type> hints = new ArrayList<>();
    for (int k = 0; k < n; k++) {
      oids[k] = b.getInt();
      Type t = Type.ofOid(oids[k]);
      hints.add(t == null ? Type.UNKNOWN : t);
    }
    Prepared p = session.prepare(sql, hints);
    statements.put(name, p);
    statementOids.put(name, oids);
    new Message('1').send(out); // ParseComplete
  }

  private void bind(ByteBuffer b) throws IOException {
    String portal = cstring(b);
    String name = cstring(b);
    Prepared p = statement(name);
    int[] declared = statementOids.getOrDefault(name, new int[0]);
    int nformats = b.getShort();
    int[] formats = new int[nformats];
    for (int k = 0; k < nformats; k++) {
      formats[k] = b.getShort();
    }
    int nparams = b.getShort();
    if (nparams != p.paramTypes().size()) {
      throw new SqlException(
          SqlState.PROTOCOL_VIOLATION,
          "bind message supplies "
              + nparams
              + " parameters, but prepared statement \""
              + name
              + "\" requires "
              + p.paramTypes().size());
    }
    List<Object> params = new ArrayList<>();
    for (int k = 0; k < nparams; k++) {
      int len = b.getInt();
      if (len < 0) {
        params.add(null);
        continue;
      }
      byte[] bytes = new byte[len];
      b.get(bytes);
      int format = nformats == 0 ? 0 : (nformats == 1 ? formats[0] : formats[k]);
      int oid = k < declared.length ? declared[k] : 0;
      params.add(PgTypes.decode(bytes, format, oid, p.paramTypes().get(k)));
    }
    int nresult = b.getShort();
    int[] resultFormats = new int[nresult];
    for (int k = 0; k < nresult; k++) {
      resultFormats[k] = b.getShort();
    }
    portals.put(portal, new Portal(p, params, resultFormats));
    new Message('2').send(out); // BindComplete
  }

  private void describe(ByteBuffer b) throws IOException {
    int kind = b.get();
    String name = cstring(b);
    if (kind == 'S') {
      Prepared p = statement(name);
      int[] declared = statementOids.getOrDefault(name, new int[0]);
      Message m = new Message('t'); // ParameterDescription
      m.int16(p.paramTypes().size());
      for (int k = 0; k < p.paramTypes().size(); k++) {
        int oid =
            k < declared.length && declared[k] != 0 ? declared[k] : p.paramTypes().get(k).oid();
        m.int32(oid);
      }
      m.send(out);
      rowDescription(p.columns(), null);
    } else {
      Portal portal = portal(name);
      rowDescription(portal.prepared.columns(), portal.resultFormats);
    }
  }

  private void execute(ByteBuffer b) throws IOException {
    String name = cstring(b);
    int maxRows = b.getInt();
    Portal portal = portal(name);
    if (portal.prepared.empty()) {
      new Message('I').send(out);
      return;
    }
    if (portal.result == null) {
      portal.result = session.execute(portal.prepared, portal.params);
      for (String notice : portal.result.notices()) {
        notice(notice);
      }
    }
    Result r = portal.result;
    if (!r.returnsRows()) {
      complete(r.tag());
      return;
    }
    int end = maxRows <= 0 ? r.rows().size() : Math.min(r.rows().size(), portal.sent + maxRows);
    for (int k = portal.sent; k < end; k++) {
      dataRow(r.rows().get(k), r.columns(), portal.resultFormats);
    }
    portal.sent = end;
    if (end < r.rows().size()) {
      new Message('s').send(out); // PortalSuspended
    } else {
      complete(r.tag());
    }
  }

  private void close(ByteBuffer b) throws IOException {
    int kind = b.get();
    String name = cstring(b);
    if (kind == 'S') {
      statements.remove(name);
      statementOids.remove(name);
    } else {
      portals.remove(name);
    }
    new Message('3').send(out); // CloseComplete
  }

  private Prepared statement(String name) {
    Prepared p = statements.get(name);
    if (p == null) {
      throw new SqlException("26000", "prepared statement \"" + name + "\" does not exist");
    }
    return p;
  }

  private Portal portal(String name) {
    Portal p = portals.get(name);
    if (p == null) {
      throw new SqlException("34000", "portal \"" + name + "\" does not exist");
    }
    return p;
  }

  // -----------------------------------------------------------------------------------------------
  // Results

  private void sendResult(Result r, int[] formats, int maxRows, boolean describe)
      throws IOException {
    for (String notice : r.notices()) {
      notice(notice);
    }
    if (r.returnsRows()) {
      if (describe) {
        rowDescription(r.columns(), formats);
      }
      for (Object[] row : r.rows()) {
        dataRow(row, r.columns(), formats);
      }
    }
    complete(r.tag());
  }

  private void rowDescription(List<Result.Column> columns, int[] formats) throws IOException {
    if (columns == null) {
      new Message('n').send(out); // NoData
      return;
    }
    Message m = new Message('T');
    m.int16(columns.size());
    for (int k = 0; k < columns.size(); k++) {
      Result.Column c = columns.get(k);
      Type t = c.type() == Type.UNKNOWN ? Type.TEXT : c.type();
      m.cstring(c.name());
      m.int32(0); // table OID
      m.int16(0); // column number
      m.int32(t.oid());
      m.int16(t.size());
      m.int32(-1); // type modifier
      m.int16(format(formats, k));
    }
    m.send(out);
  }

  private void dataRow(Object[] row, List<Result.Column> columns, int[] formats)
      throws IOException {
    Message m = new Message('D');
    m.int16(row.length);
    for (int k = 0; k < row.length; k++) {
      if (row[k] == null) {
        m.int32(-1);
      } else {
        byte[] v = PgTypes.encode(row[k], columns.get(k).type(), format(formats, k));
        m.int32(v.length);
        m.bytes(v);
      }
    }
    m.send(out);
  }

  private static int format(int[] formats, int k) {
    if (formats == null || formats.length == 0) {
      return 0;
    }
    return formats.length == 1 ? formats[0] : formats[k];
  }

  private void complete(String tag) throws IOException {
    Message m = new Message('C');
    m.cstring(tag);
    m.send(out);
  }

  private void readyForQuery() throws IOException {
    Message m = new Message('Z');
    m.int8(
        switch (session.state()) {
          case IDLE -> 'I';
          case OPEN -> 'T';
          case FAILED -> 'E';
        });
    m.send(out);
    out.flush();
  }

  private void error(String state, String message) throws IOException {
    Message m = new Message('E');
    m.int8('S');
    m.cstring("ERROR");
    m.int8('V');
    m.cstring("ERROR");
    m.int8('C');
    m.cstring(state);
    m.int8('M');
    m.cstring(message == null ? "" : message);
    m.int8(0);
    m.send(out);
  }

  private void notice(String text) throws IOException {
    boolean warning = text.startsWith("WARNING: ");
    Message m = new Message('N');
    m.int8('S');
    m.cstring(warning ? "WARNING" : "NOTICE");
    m.int8('V');
    m.cstring(warning ? "WARNING" : "NOTICE");
    m.int8('C');
    m.cstring(warning ? "25001" : "00000");
    m.int8('M');
    m.cstring(warning ? text.substring("WARNING: ".length()) : text);
    m.int8(0);
    m.send(out);
  }

  private static String cstring(ByteBuffer b) {
    int start = b.position();
    while (b.get() != 0) {
      // find the terminator
    }
    return new String(b.array(), start, b.position() - start - 1, StandardCharsets.UTF_8);
  }

  /** A backend message being built: a type byte, then a length that includes itself. */
  private static final class Message {
    private final int type;
    private final ByteArrayOutputStream body = new ByteArrayOutputStream();

    Message(int type) {
      this.type = type;
    }

    void int8(int v) {
      body.write(v);
    }

    void int16(int v) {
      body.write(v >>> 8);
      body.write(v);
    }

    void int32(int v) {
      body.write(v >>> 24);
      body.write(v >>> 16);
      body.write(v >>> 8);
      body.write(v);
    }

    void bytes(byte[] b) {
      body.writeBytes(b);
    }

    void cstring(String s) {
      body.writeBytes(s.getBytes(StandardCharsets.UTF_8));
      body.write(0);
    }

    void send(OutputStream out) throws IOException {
      out.write(type);
      int length = body.size() + 4;
      out.write(length >>> 24);
      out.write(length >>> 16);
      out.write(length >>> 8);
      out.write(length);
      body.writeTo(out);
    }
  }
}

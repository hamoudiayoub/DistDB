package io.distdb.shell;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A client for PostgreSQL's wire protocol, version 3.0, with the simple query flow: enough for an
 * interactive shell, against DistDB or against PostgreSQL itself. Not thread-safe, except {@link
 * #cancel()}.
 */
public final class PgClient implements AutoCloseable {

  /** A result column: its name and its type's OID (which decides alignment). */
  public record Column(String name, int typeOid) {}

  /** A server message that is not an error: a NOTICE or a WARNING. */
  public record Notice(String severity, String code, String message, String detail, String hint) {}

  /** One statement's rows (none for a command) and its completion tag. */
  public record Result(String tag, List<Column> columns, List<String[]> rows) {
    public boolean hasRows() {
      return columns != null;
    }
  }

  /** An ErrorResponse: the statement failed, with its SQLSTATE. */
  public static final class PgError extends Exception {
    @java.io.Serial private static final long serialVersionUID = 1L;

    private final String severity;
    private final String code;
    private final String detail;
    private final String hint;
    private final int position;

    PgError(Map<Character, String> fields) {
      super(fields.getOrDefault('M', "unknown error"));
      this.severity = fields.getOrDefault('V', fields.getOrDefault('S', "ERROR"));
      this.code = fields.getOrDefault('C', "XX000");
      this.detail = fields.get('D');
      this.hint = fields.get('H');
      int p = 0;
      try {
        p = Integer.parseInt(fields.getOrDefault('P', "0"));
      } catch (NumberFormatException ignored) {
        // no position
      }
      this.position = p;
    }

    public String severity() {
      return severity;
    }

    /** The SQLSTATE, e.g. {@code 42P01}. */
    public String code() {
      return code;
    }

    public String detail() {
      return detail;
    }

    public String hint() {
      return hint;
    }

    /** The 1-based character position of the error in the query, or 0. */
    public int position() {
      return position;
    }
  }

  /** What one statement of a query string came to: rows or a command tag, or an error. */
  public sealed interface Outcome permits Ok, Failed {
    List<Notice> notices();
  }

  public record Ok(Result result, List<Notice> notices) implements Outcome {}

  public record Failed(PgError error, List<Notice> notices) implements Outcome {}

  private static final int PROTOCOL_3 = 196608;
  private static final int CANCEL_CODE = 80877102;

  private final String host;
  private final int port;
  private final Socket socket;
  private final DataInputStream in;
  private final OutputStream out;
  private final Map<String, String> parameters = new LinkedHashMap<>();
  private char status = 'I';
  private int pid;
  private int secret;

  private PgClient(String host, int port, Socket socket) throws IOException {
    this.host = host;
    this.port = port;
    this.socket = socket;
    this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 1 << 16));
    this.out = new BufferedOutputStream(socket.getOutputStream(), 1 << 16);
  }

  /**
   * Connects and authenticates (trust, cleartext password or MD5).
   *
   * @param password used only if the server asks for one; may be null
   */
  public static PgClient connect(
      String host, int port, String user, String database, String password, Duration timeout)
      throws IOException, PgError {
    Socket s = new Socket();
    try {
      s.connect(new InetSocketAddress(host, port), (int) timeout.toMillis());
      s.setTcpNoDelay(true);
      PgClient c = new PgClient(host, port, s);
      c.startup(user, database, password);
      return c;
    } catch (IOException | PgError | RuntimeException e) {
      s.close();
      throw e;
    }
  }

  private void startup(String user, String database, String password) throws IOException, PgError {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    putInt(body, PROTOCOL_3);
    for (String[] kv :
        new String[][] {
          {"user", user}, {"database", database}, {"application_name", "distdb-shell"}
        }) {
      putCString(body, kv[0]);
      putCString(body, kv[1]);
    }
    body.write(0);
    ByteArrayOutputStream msg = new ByteArrayOutputStream();
    putInt(msg, body.size() + 4);
    body.writeTo(msg);
    msg.writeTo(out);
    out.flush();
    while (true) {
      char type = (char) in.readUnsignedByte();
      byte[] payload = payload();
      switch (type) {
        case 'R' -> authenticate(payload, user, password);
        case 'S' -> parameter(payload);
        case 'K' -> {
          pid = readInt(payload, 0);
          secret = readInt(payload, 4);
        }
        case 'N' -> {
          // a notice before the session starts: nothing to attach it to
        }
        case 'E' -> throw new PgError(fields(payload));
        case 'Z' -> {
          status = (char) payload[0];
          return;
        }
        default -> throw new IOException("unexpected message '" + type + "' during startup");
      }
    }
  }

  private void authenticate(byte[] payload, String user, String password)
      throws IOException, PgError {
    int code = readInt(payload, 0);
    switch (code) {
      case 0 -> {
        // AuthenticationOk
      }
      case 3 -> send('p', cstring(requirePassword(password)));
      case 5 -> {
        byte[] salt = java.util.Arrays.copyOfRange(payload, 4, 8);
        String inner = md5Hex((requirePassword(password) + user).getBytes(StandardCharsets.UTF_8));
        byte[] outerInput = concat(inner.getBytes(StandardCharsets.UTF_8), salt);
        send('p', cstring("md5" + md5Hex(outerInput)));
      }
      default ->
          throw new IOException(
              "the server asks for an authentication method this shell does not speak (code "
                  + code
                  + "); DistDB needs none");
    }
  }

  private static String requirePassword(String password) throws IOException {
    if (password == null) {
      throw new IOException("the server asks for a password: set PGPASSWORD");
    }
    return password;
  }

  /**
   * Runs {@code sql} (one or more statements) with the simple query protocol and returns each
   * statement's outcome. After an error the server runs nothing more of the string.
   */
  public List<Outcome> query(String sql) throws IOException {
    send('Q', cstring(sql));
    List<Outcome> outcomes = new ArrayList<>();
    List<Notice> notices = new ArrayList<>();
    List<Column> columns = null;
    List<String[]> rows = null;
    while (true) {
      char type = (char) in.readUnsignedByte();
      byte[] p = payload();
      switch (type) {
        case 'T' -> {
          int n = readShort(p, 0);
          columns = new ArrayList<>(n);
          int at = 2;
          for (int i = 0; i < n; i++) {
            int end = indexOf(p, at);
            String name = new String(p, at, end - at, StandardCharsets.UTF_8);
            at = end + 1;
            int oid = readInt(p, at + 6);
            at += 18;
            columns.add(new Column(name, oid));
          }
          rows = new ArrayList<>();
        }
        case 'D' -> {
          int n = readShort(p, 0);
          String[] row = new String[n];
          int at = 2;
          for (int i = 0; i < n; i++) {
            int len = readInt(p, at);
            at += 4;
            if (len >= 0) {
              row[i] = new String(p, at, len, StandardCharsets.UTF_8);
              at += len;
            }
          }
          if (rows != null) {
            rows.add(row);
          }
        }
        case 'C' -> {
          String tag = new String(p, 0, indexOf(p, 0), StandardCharsets.UTF_8);
          outcomes.add(new Ok(new Result(tag, columns, rows), notices));
          notices = new ArrayList<>();
          columns = null;
          rows = null;
        }
        case 'I' -> {
          // EmptyQueryResponse: the string held no statement
        }
        case 'E' -> {
          outcomes.add(new Failed(new PgError(fields(p)), notices));
          notices = new ArrayList<>();
          columns = null;
          rows = null;
        }
        case 'N' -> {
          Map<Character, String> f = fields(p);
          notices.add(
              new Notice(
                  f.getOrDefault('V', f.getOrDefault('S', "NOTICE")),
                  f.getOrDefault('C', "00000"),
                  f.getOrDefault('M', ""),
                  f.get('D'),
                  f.get('H')));
        }
        case 'S' -> parameter(p);
        case 'A' -> {
          // LISTEN/NOTIFY: not used
        }
        case 'G', 'H' -> throw new IOException("COPY is not supported by this shell");
        case 'Z' -> {
          status = (char) p[0];
          if (!notices.isEmpty() && !outcomes.isEmpty()) {
            Outcome last = outcomes.removeLast();
            List<Notice> all = new ArrayList<>(last.notices());
            all.addAll(notices);
            outcomes.add(
                switch (last) {
                  case Ok ok -> new Ok(ok.result(), all);
                  case Failed f -> new Failed(f.error(), all);
                });
          }
          return outcomes;
        }
        default -> {
          // a message the simple query flow does not use: skip it
        }
      }
    }
  }

  /** {@code I} idle, {@code T} in a transaction block, {@code E} in a failed one. */
  public char transactionStatus() {
    return status;
  }

  /** What the server reported with ParameterStatus: server_version, TimeZone, ... */
  public Map<String, String> parameters() {
    return parameters;
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public int backendPid() {
    return pid;
  }

  /** Asks the server, on a new connection, to cancel the running statement. Thread-safe. */
  public void cancel() {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress(host, port), 2000);
      ByteArrayOutputStream m = new ByteArrayOutputStream();
      putInt(m, 16);
      putInt(m, CANCEL_CODE);
      putInt(m, pid);
      putInt(m, secret);
      m.writeTo(s.getOutputStream());
      s.getOutputStream().flush();
    } catch (IOException ignored) {
      // best effort, as in psql
    }
  }

  @Override
  public void close() {
    try {
      send('X', new byte[0]);
    } catch (IOException ignored) {
      // the connection is going away anyway
    }
    try {
      socket.close();
    } catch (IOException ignored) {
      // nothing left to do
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Framing

  private void send(char type, byte[] body) throws IOException {
    out.write(type);
    ByteArrayOutputStream len = new ByteArrayOutputStream(4);
    putInt(len, body.length + 4);
    len.writeTo(out);
    out.write(body);
    out.flush();
  }

  private byte[] payload() throws IOException {
    int len = in.readInt();
    if (len < 4 || len > (1 << 30)) {
      throw new IOException("bad message length " + len);
    }
    byte[] p = new byte[len - 4];
    try {
      in.readFully(p);
    } catch (EOFException e) {
      throw new IOException("the server closed the connection", e);
    }
    return p;
  }

  private void parameter(byte[] p) {
    int end = indexOf(p, 0);
    String name = new String(p, 0, end, StandardCharsets.UTF_8);
    int end2 = indexOf(p, end + 1);
    parameters.put(name, new String(p, end + 1, end2 - end - 1, StandardCharsets.UTF_8));
  }

  private static Map<Character, String> fields(byte[] p) {
    Map<Character, String> f = new LinkedHashMap<>();
    int at = 0;
    while (at < p.length && p[at] != 0) {
      char key = (char) p[at];
      int end = indexOf(p, at + 1);
      f.put(key, new String(p, at + 1, end - at - 1, StandardCharsets.UTF_8));
      at = end + 1;
    }
    return f;
  }

  private static int indexOf(byte[] p, int from) {
    for (int i = from; i < p.length; i++) {
      if (p[i] == 0) {
        return i;
      }
    }
    return p.length;
  }

  private static int readInt(byte[] p, int at) {
    return ((p[at] & 0xFF) << 24)
        | ((p[at + 1] & 0xFF) << 16)
        | ((p[at + 2] & 0xFF) << 8)
        | (p[at + 3] & 0xFF);
  }

  private static int readShort(byte[] p, int at) {
    return ((p[at] & 0xFF) << 8) | (p[at + 1] & 0xFF);
  }

  private static void putInt(ByteArrayOutputStream o, int v) {
    o.write(v >>> 24);
    o.write(v >>> 16);
    o.write(v >>> 8);
    o.write(v);
  }

  private static void putCString(ByteArrayOutputStream o, String s) {
    o.writeBytes(s.getBytes(StandardCharsets.UTF_8));
    o.write(0);
  }

  private static byte[] cstring(String s) {
    ByteArrayOutputStream o = new ByteArrayOutputStream();
    putCString(o, s);
    return o.toByteArray();
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] c = java.util.Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, c, a.length, b.length);
    return c;
  }

  private static String md5Hex(byte[] input) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(input));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 is missing from this JDK", e);
    }
  }
}

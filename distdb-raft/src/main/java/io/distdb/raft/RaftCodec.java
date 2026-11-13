package io.distdb.raft;

import io.distdb.common.NodeId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Binary encodings for messages (what crosses {@link io.distdb.env.Transport}) and for what a host
 * stores (entries, hard state, snapshots). Hand-written so the Raft module depends on nothing; the
 * format is a type tag followed by fixed-width fields and length-prefixed byte strings.
 */
public final class RaftCodec {

  private static final byte REQUEST_VOTE = 1;
  private static final byte VOTE_RESPONSE = 2;
  private static final byte APPEND_ENTRIES = 3;
  private static final byte APPEND_RESPONSE = 4;
  private static final byte HEARTBEAT = 5;
  private static final byte HEARTBEAT_RESPONSE = 6;
  private static final byte INSTALL_SNAPSHOT = 7;
  private static final byte TIMEOUT_NOW = 8;

  private RaftCodec() {}

  // -----------------------------------------------------------------------------------------------
  // Messages

  public static byte[] encode(Message m) {
    return write(out -> {
      switch (m) {
        case Message.RequestVote v -> {
          header(out, REQUEST_VOTE, m);
          out.writeLong(v.lastIndex());
          out.writeLong(v.lastTerm());
          out.writeBoolean(v.preVote());
          out.writeBoolean(v.transfer());
        }
        case Message.VoteResponse v -> {
          header(out, VOTE_RESPONSE, m);
          out.writeBoolean(v.granted());
          out.writeBoolean(v.preVote());
        }
        case Message.AppendEntries a -> {
          header(out, APPEND_ENTRIES, m);
          out.writeLong(a.prevIndex());
          out.writeLong(a.prevTerm());
          out.writeLong(a.commit());
          out.writeInt(a.entries().size());
          for (Entry e : a.entries()) {
            writeEntry(out, e);
          }
        }
        case Message.AppendResponse a -> {
          header(out, APPEND_RESPONSE, m);
          out.writeBoolean(a.success());
          out.writeLong(a.index());
          out.writeLong(a.conflictIndex());
          out.writeLong(a.conflictTerm());
        }
        case Message.Heartbeat h -> {
          header(out, HEARTBEAT, m);
          out.writeLong(h.commit());
          out.writeLong(h.round());
        }
        case Message.HeartbeatResponse h -> {
          header(out, HEARTBEAT_RESPONSE, m);
          out.writeLong(h.round());
        }
        case Message.InstallSnapshot s -> {
          header(out, INSTALL_SNAPSHOT, m);
          writeSnapshot(out, s.snapshot());
        }
        case Message.TimeoutNow t -> header(out, TIMEOUT_NOW, m);
      }
    });
  }

  /**
   * Decodes a message.
   *
   * @throws IllegalArgumentException if {@code bytes} is not a valid encoding
   */
  public static Message decode(byte[] bytes) {
    return read(
        bytes,
        in -> {
          byte type = in.readByte();
          NodeId from = readNode(in);
          NodeId to = readNode(in);
          long term = in.readLong();
          return switch (type) {
            case REQUEST_VOTE ->
                new Message.RequestVote(
                    from,
                    to,
                    term,
                    in.readLong(),
                    in.readLong(),
                    in.readBoolean(),
                    in.readBoolean());
            case VOTE_RESPONSE ->
                new Message.VoteResponse(from, to, term, in.readBoolean(), in.readBoolean());
            case APPEND_ENTRIES -> {
              long prevIndex = in.readLong();
              long prevTerm = in.readLong();
              long commit = in.readLong();
              int n = count(in);
              List<Entry> entries = new ArrayList<>(n);
              for (int i = 0; i < n; i++) {
                entries.add(readEntry(in));
              }
              yield new Message.AppendEntries(from, to, term, prevIndex, prevTerm, entries, commit);
            }
            case APPEND_RESPONSE ->
                new Message.AppendResponse(
                    from, to, term, in.readBoolean(), in.readLong(), in.readLong(), in.readLong());
            case HEARTBEAT -> new Message.Heartbeat(from, to, term, in.readLong(), in.readLong());
            case HEARTBEAT_RESPONSE -> new Message.HeartbeatResponse(from, to, term, in.readLong());
            case INSTALL_SNAPSHOT -> new Message.InstallSnapshot(from, to, term, readSnapshot(in));
            case TIMEOUT_NOW -> new Message.TimeoutNow(from, to, term);
            default -> throw new IllegalArgumentException("unknown message type " + type);
          };
        });
  }

  // -----------------------------------------------------------------------------------------------
  // What a host stores

  public static byte[] encodeEntry(Entry e) {
    return write(out -> writeEntry(out, e));
  }

  public static Entry decodeEntry(byte[] bytes) {
    return read(bytes, RaftCodec::readEntry);
  }

  public static byte[] encodeHardState(HardState h) {
    return write(out -> {
      out.writeLong(h.term());
      writeNode(out, h.vote());
      out.writeLong(h.commit());
    });
  }

  public static HardState decodeHardState(byte[] bytes) {
    return read(bytes, in -> new HardState(in.readLong(), readNode(in), in.readLong()));
  }

  public static byte[] encodeSnapshot(Snapshot s) {
    return write(out -> writeSnapshot(out, s));
  }

  public static Snapshot decodeSnapshot(byte[] bytes) {
    return read(bytes, RaftCodec::readSnapshot);
  }

  public static byte[] encodeMembership(Membership m) {
    return write(out -> writeMembership(out, m));
  }

  public static Membership decodeMembership(byte[] bytes) {
    return read(bytes, RaftCodec::readMembership);
  }

  // -----------------------------------------------------------------------------------------------

  private static void header(DataOutputStream out, byte type, Message m) throws IOException {
    out.writeByte(type);
    writeNode(out, m.from());
    writeNode(out, m.to());
    out.writeLong(m.term());
  }

  private static void writeEntry(DataOutputStream out, Entry e) throws IOException {
    out.writeLong(e.index());
    out.writeLong(e.term());
    out.writeByte(e.type().ordinal());
    writeBytes(out, e.data());
  }

  private static Entry readEntry(DataInputStream in) throws IOException {
    long index = in.readLong();
    long term = in.readLong();
    int type = in.readByte();
    if (type < 0 || type >= Entry.Type.values().length) {
      throw new IllegalArgumentException("unknown entry type " + type);
    }
    return new Entry(index, term, Entry.Type.values()[type], readBytes(in));
  }

  private static void writeSnapshot(DataOutputStream out, Snapshot s) throws IOException {
    out.writeLong(s.index());
    out.writeLong(s.term());
    writeMembership(out, s.membership());
    writeBytes(out, s.data());
  }

  private static Snapshot readSnapshot(DataInputStream in) throws IOException {
    return new Snapshot(in.readLong(), in.readLong(), readMembership(in), readBytes(in));
  }

  private static void writeMembership(DataOutputStream out, Membership m) throws IOException {
    writeNodes(out, m.voters());
    writeNodes(out, m.learners());
  }

  private static Membership readMembership(DataInputStream in) throws IOException {
    return new Membership(readNodes(in), readNodes(in));
  }

  private static void writeNodes(DataOutputStream out, List<NodeId> ids) throws IOException {
    out.writeInt(ids.size());
    for (NodeId id : ids) {
      out.writeInt(id.value());
    }
  }

  private static List<NodeId> readNodes(DataInputStream in) throws IOException {
    int n = count(in);
    List<NodeId> ids = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      ids.add(new NodeId(in.readInt()));
    }
    return ids;
  }

  private static void writeNode(DataOutputStream out, NodeId id) throws IOException {
    out.writeInt(id == null ? 0 : id.value());
  }

  private static NodeId readNode(DataInputStream in) throws IOException {
    int v = in.readInt();
    return v == 0 ? null : new NodeId(v);
  }

  private static void writeBytes(DataOutputStream out, byte[] b) throws IOException {
    out.writeInt(b.length);
    out.write(b);
  }

  private static byte[] readBytes(DataInputStream in) throws IOException {
    byte[] b = new byte[count(in)];
    in.readFully(b);
    return b;
  }

  private static int count(DataInputStream in) throws IOException {
    int n = in.readInt();
    if (n < 0 || n > in.available()) {
      throw new IllegalArgumentException("bad length " + n);
    }
    return n;
  }

  @FunctionalInterface
  private interface Writer {
    void write(DataOutputStream out) throws IOException;
  }

  @FunctionalInterface
  private interface Reader<T> {
    T read(DataInputStream in) throws IOException;
  }

  private static byte[] write(Writer w) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      w.write(out);
    } catch (IOException e) {
      throw new UncheckedIOException(e); // cannot happen with an in-memory stream
    }
    return bytes.toByteArray();
  }

  private static <T> T read(byte[] bytes, Reader<T> r) {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      T value = r.read(in);
      if (in.available() != 0) {
        throw new IllegalArgumentException(in.available() + " trailing bytes");
      }
      return value;
    } catch (EOFException e) {
      throw new IllegalArgumentException("truncated encoding", e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}

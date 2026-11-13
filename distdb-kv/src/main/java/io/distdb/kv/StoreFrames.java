package io.distdb.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * What one store sends another: a frame of items, each for one range. A store collects everything
 * its replicas send to the same peer while its mailbox runs, and sends it as one frame. With many
 * ranges per node that is one message per peer per round instead of one per range.
 *
 * <pre>
 *   frame := count:int item*
 *   item  := rangeId:long kind:byte configIndex:long length:int payload
 * </pre>
 */
final class StoreFrames {

  /** A Raft message (encoded by {@code RaftCodec}) for the range. */
  static final byte RAFT = 0;

  /**
   * "You are not a member of this range": sent to a replica that a range removed, which nobody else
   * tells. {@code configIndex} is the sender's applied conf_ver, which excludes the receiver.
   */
  static final byte REMOVED = 1;

  /**
   * "Do you count me as a member?" from a replica that has heard from no leader for a while, to the
   * range's members as the placement driver knows them. A member that does not count the sender
   * answers {@link #REMOVED}; otherwise it says nothing.
   */
  static final byte PROBE = 2;

  /**
   * One item.
   *
   * @param configIndex for {@link #RAFT}: the sender's latest configuration index, so a receiver
   *     that destroyed its replica can tell stale messages from ones that add it back
   */
  record Item(long rangeId, byte kind, long configIndex, byte[] payload) {}

  private StoreFrames() {}

  static byte[] encode(List<Item> items) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(items.size());
      for (Item i : items) {
        out.writeLong(i.rangeId());
        out.writeByte(i.kind());
        out.writeLong(i.configIndex());
        out.writeInt(i.payload().length);
        out.write(i.payload());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bytes.toByteArray();
  }

  /**
   * @throws IllegalArgumentException if {@code frame} is not a valid encoding
   */
  static List<Item> decode(byte[] frame) {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame))) {
      int n = in.readInt();
      if (n < 0 || n > frame.length) {
        throw new IllegalArgumentException("bad item count " + n);
      }
      List<Item> items = new ArrayList<>(n);
      for (int k = 0; k < n; k++) {
        long range = in.readLong();
        byte kind = in.readByte();
        long configIndex = in.readLong();
        int length = in.readInt();
        if (length < 0 || length > in.available()) {
          throw new IllegalArgumentException("bad payload length " + length);
        }
        byte[] payload = new byte[length];
        in.readFully(payload);
        items.add(new Item(range, kind, configIndex, payload));
      }
      return items;
    } catch (IOException e) {
      throw new IllegalArgumentException("truncated store frame", e);
    }
  }
}

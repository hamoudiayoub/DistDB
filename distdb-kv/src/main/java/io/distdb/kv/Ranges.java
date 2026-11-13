package io.distdb.kv;

import io.distdb.common.Bytes;
import io.distdb.common.NodeId;
import io.distdb.raft.Membership;
import io.distdb.rpc.proto.RangeDescriptor;
import java.util.ArrayList;
import java.util.List;

/** Helpers for {@link RangeDescriptor}s: spans, members, and printing. */
public final class Ranges {

  private Ranges() {}

  /** Whether client key {@code key} is in {@code d}'s span. */
  public static boolean contains(RangeDescriptor d, byte[] key) {
    return Bytes.compare(d.getStartKey().toByteArray(), key) <= 0
        && (!d.hasEndKey() || Bytes.compare(key, d.getEndKey().toByteArray()) < 0);
  }

  /** Whether {@code [start, end)} lies inside {@code d}'s span; a null {@code end} means +∞. */
  public static boolean containsSpan(RangeDescriptor d, byte[] start, byte[] end) {
    if (Bytes.compare(d.getStartKey().toByteArray(), start) > 0) {
      return false;
    }
    if (!d.hasEndKey()) {
      return true;
    }
    return end != null && Bytes.compare(end, d.getEndKey().toByteArray()) <= 0;
  }

  /** Whether two spans share a key. */
  public static boolean overlaps(RangeDescriptor a, RangeDescriptor b) {
    boolean aEndsBeforeB =
        a.hasEndKey()
            && Bytes.compare(a.getEndKey().toByteArray(), b.getStartKey().toByteArray()) <= 0;
    boolean bEndsBeforeA =
        b.hasEndKey()
            && Bytes.compare(b.getEndKey().toByteArray(), a.getStartKey().toByteArray()) <= 0;
    return !aEndsBeforeB && !bEndsBeforeA;
  }

  /** Whether {@code node} is a voter or a learner of {@code d}. */
  public static boolean isMember(RangeDescriptor d, NodeId node) {
    return d.getVotersList().contains(node.value()) || d.getLearnersList().contains(node.value());
  }

  public static Membership membership(RangeDescriptor d) {
    return new Membership(nodes(d.getVotersList()), nodes(d.getLearnersList()));
  }

  /** {@code d} with the replicas of {@code m}, changed by the entry at {@code index}. */
  public static RangeDescriptor withMembership(RangeDescriptor d, Membership m, long index) {
    return d.toBuilder()
        .clearVoters()
        .addAllVoters(m.voters().stream().map(NodeId::value).toList())
        .clearLearners()
        .addAllLearners(m.learners().stream().map(NodeId::value).toList())
        .setConfVer(index)
        .build();
  }

  /** The engine keys of {@code d}'s client data: {@code [start, end)} of the user space. */
  static byte[][] userSpan(RangeDescriptor d) {
    return new byte[][] {
      KeySpace.user(d.getStartKey().toByteArray()),
      KeySpace.userEnd(d.hasEndKey() ? d.getEndKey().toByteArray() : null)
    };
  }

  /** For logs and failure messages: {@code r7[k10, k20) v3 c12 voters [1, 2, 3]}. */
  public static String describe(RangeDescriptor d) {
    return "r"
        + d.getRangeId()
        + "["
        + text(d.getStartKey().toByteArray())
        + ", "
        + (d.hasEndKey() ? text(d.getEndKey().toByteArray()) : "+inf")
        + ") v"
        + d.getVersion()
        + " c"
        + d.getConfVer()
        + " voters "
        + d.getVotersList()
        + (d.getLearnersCount() > 0 ? " learners " + d.getLearnersList() : "");
  }

  private static String text(byte[] key) {
    StringBuilder out = new StringBuilder();
    for (byte b : key) {
      if (b >= 0x21 && b < 0x7F) {
        out.append((char) b);
      } else {
        out.append(String.format("\\x%02x", b & 0xFF));
      }
    }
    return out.toString();
  }

  private static List<NodeId> nodes(List<Integer> ids) {
    List<NodeId> out = new ArrayList<>(ids.size());
    ids.forEach(i -> out.add(new NodeId(i)));
    return out;
  }
}

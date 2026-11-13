package io.distdb.client;

import io.distdb.rpc.proto.LocateResponse;
import io.distdb.rpc.proto.RangeDescriptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * What a client believes about where keys live: start key → range descriptor, its replicas'
 * addresses, and which one leads. Filled from the placement driver, corrected by redirects, and
 * dropped entry by entry when a node says the range is not there ({@code RangeNotFound}) or has
 * changed ({@code EpochMismatch}). Never trusted for correctness: a node checks every request
 * against its own descriptor.
 *
 * <p>Thread-safe.
 */
final class RangeCache {

  /** One cached range. Mutable fields are guarded by the cache. */
  static final class Route {
    final RangeDescriptor desc;
    final Map<Integer, String> addresses;
    private String leader; // address, or null if unknown
    private int rotation;

    Route(RangeDescriptor desc, Map<Integer, String> addresses, String leader) {
      this.desc = desc;
      this.addresses = Map.copyOf(addresses);
      this.leader = leader;
    }
  }

  private final TreeMap<byte[], Route> byStart = new TreeMap<>(Arrays::compareUnsigned);

  /** The cached range holding {@code key}, or null. */
  synchronized Route lookup(byte[] key) {
    Map.Entry<byte[], Route> e = byStart.floorEntry(key);
    return e != null && contains(e.getValue().desc, key) ? e.getValue() : null;
  }

  /** Caches what the PD said, replacing every cached range it overlaps. */
  synchronized Route put(LocateResponse located) {
    RangeDescriptor d = located.getRange().getDesc();
    byStart.values().removeIf(r -> overlaps(r.desc, d));
    String leader = located.getAddressesMap().get(located.getRange().getLeader());
    Route route = new Route(d, located.getAddressesMap(), leader);
    byStart.put(d.getStartKey().toByteArray(), route);
    return route;
  }

  synchronized void invalidate(Route r) {
    byStart.remove(r.desc.getStartKey().toByteArray(), r);
  }

  synchronized void setLeader(Route r, String address) {
    r.leader = address;
  }

  /** The leader did not answer: try the next replica. */
  synchronized void rotate(Route r) {
    r.leader = null;
    r.rotation++;
  }

  /** Where to send a request for {@code r}: its leader if known, else a replica; null if none. */
  synchronized String address(Route r) {
    if (r.leader != null) {
      return r.leader;
    }
    List<String> known = new ArrayList<>();
    List<Integer> members = new ArrayList<>(r.desc.getVotersList());
    members.addAll(r.desc.getLearnersList());
    for (int n : members) {
      String a = r.addresses.get(n);
      if (a != null) {
        known.add(a);
      }
    }
    return known.isEmpty() ? null : known.get(Math.floorMod(r.rotation, known.size()));
  }

  static boolean contains(RangeDescriptor d, byte[] key) {
    return Arrays.compareUnsigned(d.getStartKey().toByteArray(), key) <= 0
        && (!d.hasEndKey() || Arrays.compareUnsigned(key, d.getEndKey().toByteArray()) < 0);
  }

  private static boolean overlaps(RangeDescriptor a, RangeDescriptor b) {
    boolean aBeforeB =
        a.hasEndKey()
            && Arrays.compareUnsigned(a.getEndKey().toByteArray(), b.getStartKey().toByteArray())
                <= 0;
    boolean bBeforeA =
        b.hasEndKey()
            && Arrays.compareUnsigned(b.getEndKey().toByteArray(), a.getStartKey().toByteArray())
                <= 0;
    return !aBeforeB && !bBeforeA;
  }
}

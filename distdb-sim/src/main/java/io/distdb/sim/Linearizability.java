package io.distdb.sim;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Checks that a history of key-value operations is linearizable: that there is one order of the
 * operations, consistent with real time (an operation that finished before another started comes
 * first), in which a single register per key would have returned exactly what the clients saw.
 *
 * <p>The search is Wing and Gong's, with Lowe's memoization, as in Porcupine. It tries, at each
 * step, every operation that could go next (one whose call precedes every pending return), applies
 * it to the model, and backtracks when the model disagrees with an observed result. A cache of
 * (operations placed so far, model state) prunes states already explored. Linearizability is
 * compositional, so each key is checked on its own, which keeps the search small.
 *
 * <p>A write whose outcome the client never learned has {@code ret = Long.MAX_VALUE}: it may be
 * placed anywhere after its call, including after everything else, which is the same as never
 * happening. Reads with unknown outcomes, and writes known not to have happened, are simply left
 * out of the history.
 */
public final class Linearizability {

  /** What a client did. */
  public enum Kind {
    PUT,
    DELETE,
    GET
  }

  /**
   * One operation.
   *
   * @param client who issued it (for messages only)
   * @param value for PUT the value written; for GET the value read, null if the key was absent
   * @param call when it was invoked
   * @param ret when its answer arrived, or {@code Long.MAX_VALUE} if never
   */
  public record Op(int client, String key, Kind kind, String value, long call, long ret) {
    public Op {
      Objects.requireNonNull(key);
      Objects.requireNonNull(kind);
      if (ret < call) {
        throw new IllegalArgumentException("returns before its call: " + call + " > " + ret);
      }
    }

    @Override
    public String toString() {
      String end = ret == Long.MAX_VALUE ? "?" : String.valueOf(ret);
      return "c"
          + client
          + " "
          + kind
          + "("
          + key
          + (kind == Kind.GET ? ")=" : ", ")
          + value
          + (kind == Kind.GET ? "" : ")")
          + " ["
          + call
          + ", "
          + end
          + "]";
    }
  }

  /** The verdict: linearizable, or the first key whose history is not. */
  public record Result(boolean linearizable, String key, List<Op> history) {}

  private Linearizability() {}

  public static Result check(List<Op> history) {
    Map<String, List<Op>> byKey = new LinkedHashMap<>();
    for (Op op : history) {
      byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
    }
    for (Map.Entry<String, List<Op>> e : byKey.entrySet()) {
      if (!checkRegister(e.getValue())) {
        return new Result(false, e.getKey(), e.getValue());
      }
    }
    return new Result(true, null, List.of());
  }

  // -----------------------------------------------------------------------------------------------

  /** A call or return event in a doubly linked list ordered by time. */
  private static final class Event {
    final boolean call;
    final int op;
    final long time;
    Event match; // a call's return, a return's call
    Event prev;
    Event next;

    Event(boolean call, int op, long time) {
      this.call = call;
      this.op = op;
      this.time = time;
    }
  }

  private record Explored(BitSet placed, String state) {}

  private record Frame(Event call, String state) {}

  /** Wing and Gong's search over one key, the model being a register (absent = null). */
  static boolean checkRegister(List<Op> ops) {
    List<Event> events = new ArrayList<>();
    for (int i = 0; i < ops.size(); i++) {
      Event c = new Event(true, i, ops.get(i).call());
      Event r = new Event(false, i, ops.get(i).ret());
      c.match = r;
      r.match = c;
      events.add(c);
      events.add(r);
    }
    // By time; at the same instant, calls first, which treats the two operations as concurrent
    // (the more permissive reading, so a pass is never an artefact of tie-breaking).
    events.sort(Comparator.comparingLong((Event e) -> e.time).thenComparing(e -> !e.call));
    Event head = new Event(true, -1, Long.MIN_VALUE);
    Event prev = head;
    for (Event e : events) {
      prev.next = e;
      e.prev = prev;
      prev = e;
    }

    String state = null;
    BitSet placed = new BitSet(ops.size());
    Set<Explored> explored = new HashSet<>();
    Deque<Frame> stack = new ArrayDeque<>();
    Event entry = head.next;
    while (head.next != null) {
      if (entry.call) {
        Op op = ops.get(entry.op);
        String next = step(state, op);
        if (next != INVALID) {
          BitSet withIt = (BitSet) placed.clone();
          withIt.set(entry.op);
          if (explored.add(new Explored(withIt, next))) {
            stack.push(new Frame(entry, state));
            state = next;
            placed.set(entry.op);
            lift(entry);
            entry = head.next;
            continue;
          }
        }
        entry = entry.next;
      } else {
        // A return: its operation must already be placed. Nothing left to try here: backtrack.
        if (stack.isEmpty()) {
          return false;
        }
        Frame f = stack.pop();
        state = f.state();
        placed.clear(f.call().op);
        unlift(f.call());
        entry = f.call().next;
      }
    }
    return true;
  }

  /** Marks "the model refused this operation". */
  private static final String INVALID = new String("invalid");

  /** The register after {@code op}, or {@link #INVALID} if {@code op} could not happen here. */
  private static String step(String state, Op op) {
    return switch (op.kind()) {
      case PUT -> op.value();
      case DELETE -> null;
      case GET -> Objects.equals(state, op.value()) ? state : INVALID;
    };
  }

  /** Removes an operation's call and return from the list (they stay linked to their place). */
  private static void lift(Event call) {
    call.prev.next = call.next;
    if (call.next != null) {
      call.next.prev = call.prev;
    }
    Event ret = call.match;
    ret.prev.next = ret.next;
    if (ret.next != null) {
      ret.next.prev = ret.prev;
    }
  }

  /** Puts them back, in reverse order of removal. */
  private static void unlift(Event call) {
    Event ret = call.match;
    ret.prev.next = ret;
    if (ret.next != null) {
      ret.next.prev = ret;
    }
    call.prev.next = call;
    if (call.next != null) {
      call.next.prev = call;
    }
  }
}

package io.distdb.raft;

import io.distdb.common.NodeId;
import io.distdb.env.SplittableRandomSource;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * A few Raft nodes wired together in memory for unit tests. Messages wait in one queue until the
 * test delivers them, and a test can drop any message it likes. Each node has a store that keeps
 * exactly what its {@link Ready}s asked to persist, and a state machine that records the commands
 * it applied, so tests can crash and restart nodes.
 */
final class TestCluster {

  static final Comparator<NodeId> BY_VALUE = Comparator.comparingInt(NodeId::value);

  final Map<NodeId, RaftNode> nodes = new TreeMap<>(BY_VALUE);
  final Map<NodeId, Store> stores = new TreeMap<>(BY_VALUE);
  final Map<NodeId, List<String>> applied = new TreeMap<>(BY_VALUE);
  final Map<NodeId, List<Ready.ReadState>> reads = new TreeMap<>(BY_VALUE);
  final ArrayDeque<Message> queue = new ArrayDeque<>();
  final Set<NodeId> down = new HashSet<>();
  final List<Message> sent = new ArrayList<>();
  Predicate<Message> drop = m -> false;
  private final RaftConfig template;
  private long seed = 1;

  /** Callbacks a test can hook to see what happens. */
  TestCluster(RaftConfig template) {
    this.template = template;
  }

  /** A group of {@code n} voters, n1..n{@code n}, with default settings. */
  static TestCluster of(int n) {
    return of(n, RaftConfig.defaults(new NodeId(1)));
  }

  static TestCluster of(int n, RaftConfig template) {
    TestCluster c = new TestCluster(template);
    NodeId[] ids = new NodeId[n];
    for (int i = 0; i < n; i++) {
      ids[i] = new NodeId(i + 1);
    }
    for (NodeId id : ids) {
      c.add(id, RaftNode.bootstrap(c.config(id), Membership.ofVoters(ids), c.random()));
    }
    c.process();
    return c;
  }

  RaftConfig config(NodeId id) {
    return new RaftConfig(
        id,
        template.electionTicks(),
        template.heartbeatTicks(),
        template.preVote(),
        template.checkQuorum(),
        template.maxEntriesPerAppend(),
        template.maxInflightAppends());
  }

  private SplittableRandomSource random() {
    return SplittableRandomSource.seeded(seed++);
  }

  void add(NodeId id, RaftNode node) {
    nodes.put(id, node);
    stores.putIfAbsent(id, new Store());
    applied.putIfAbsent(id, new ArrayList<>());
    reads.putIfAbsent(id, new ArrayList<>());
  }

  /** Adds a node that has stored nothing and waits to be added to the group. */
  RaftNode addEmpty(int i) {
    NodeId id = new NodeId(i);
    RaftNode node = RaftNode.restart(config(id), PersistedState.empty(), 0, random());
    add(id, node);
    return node;
  }

  RaftNode node(int i) {
    return nodes.get(new NodeId(i));
  }

  NodeId id(int i) {
    return new NodeId(i);
  }

  /** Commands node {@code i} applied, in order (no-ops and config entries left out). */
  List<String> applied(int i) {
    return applied.get(id(i));
  }

  /** Simulates a crash: the node loses everything it had not been asked to persist. */
  void crash(int i) {
    down.add(id(i));
  }

  /** Brings a crashed node back from its store; its state machine is rebuilt from the snapshot. */
  void restart(int i) {
    NodeId id = id(i);
    down.remove(id);
    Store s = stores.get(id);
    PersistedState state = s.load();
    applied.put(id, new ArrayList<>(decodeCommands(state.snapshot().data())));
    nodes.put(id, RaftNode.restart(config(id), state, state.snapshot().index(), random()));
  }

  /** Handles every node's Ready the way a correct host must: persist, send, apply, advance. */
  void process() {
    boolean any = true;
    while (any) {
      any = false;
      for (Map.Entry<NodeId, RaftNode> e : nodes.entrySet()) {
        NodeId id = e.getKey();
        RaftNode node = e.getValue();
        if (down.contains(id)) {
          continue;
        }
        while (node.hasReady()) {
          any = true;
          Ready r = node.ready();
          stores.get(id).save(r);
          queue.addAll(r.messages());
          sent.addAll(r.messages());
          if (r.snapshot() != null && r.snapshotReplacesLog()) {
            applied.put(id, new ArrayList<>(decodeCommands(r.snapshot().data())));
          }
          for (Entry entry : r.committedEntries()) {
            if (entry.type() == Entry.Type.NORMAL && entry.data().length > 0) {
              applied.get(id).add(new String(entry.data(), StandardCharsets.UTF_8));
            }
          }
          reads.get(id).addAll(r.readStates());
          node.advance(r);
        }
      }
    }
  }

  /** Delivers queued messages (and the ones they cause) until nothing is left. */
  void deliverAll() {
    process();
    int budget = 100_000;
    while (!queue.isEmpty()) {
      if (--budget == 0) {
        throw new AssertionError("messages never stop");
      }
      Message m = queue.pollFirst();
      if (down.contains(m.to()) || down.contains(m.from()) || drop.test(m)) {
        continue;
      }
      RaftNode to = nodes.get(m.to());
      if (to != null) {
        to.step(m);
      }
      process();
    }
  }

  /** Ticks every live node once and delivers the consequences. */
  void tickAll() {
    for (Map.Entry<NodeId, RaftNode> e : nodes.entrySet()) {
      if (!down.contains(e.getKey())) {
        e.getValue().tick();
      }
    }
    deliverAll();
  }

  void tickAll(int times) {
    for (int i = 0; i < times; i++) {
      tickAll();
    }
  }

  /** Ticks node {@code i} alone until it starts an election, then lets the election run. */
  void elect(int i) {
    RaftNode n = node(i);
    for (int t = 0; t < 100 && n.status().role() == RaftNode.Role.FOLLOWER; t++) {
      n.tick();
    }
    deliverAll();
  }

  /** The current leaders among live nodes. */
  List<NodeId> leaders() {
    List<NodeId> out = new ArrayList<>();
    nodes.forEach((id, n) -> {
      if (!down.contains(id) && n.status().role() == RaftNode.Role.LEADER) {
        out.add(id);
      }
    });
    return out;
  }

  /** Makes node {@code i} compact its log to its applied index. */
  void compact(int i) {
    RaftNode n = node(i);
    n.compact(n.status().applied(), encodeCommands(applied(i)));
    process();
  }

  static byte[] encodeCommands(List<String> commands) {
    return String.join("\n", commands).getBytes(StandardCharsets.UTF_8);
  }

  static List<String> decodeCommands(byte[] data) {
    String s = new String(data, StandardCharsets.UTF_8);
    return s.isEmpty() ? List.of() : Arrays.asList(s.split("\n"));
  }

  static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /** Exactly what a node's Readies asked to persist. */
  static final class Store {
    HardState hardState = HardState.EMPTY;
    Snapshot snapshot = Snapshot.initial(Membership.EMPTY);
    final TreeMap<Long, Entry> entries = new TreeMap<>();

    void save(Ready r) {
      if (r.hardState() != null) {
        hardState = r.hardState();
      }
      if (r.snapshot() != null) {
        snapshot = r.snapshot();
        if (r.snapshotReplacesLog()) {
          entries.clear();
        } else {
          entries.headMap(r.snapshot().index(), true).clear();
        }
      }
      if (!r.entries().isEmpty()) {
        entries.tailMap(r.entries().getFirst().index(), true).clear();
        r.entries().forEach(e -> entries.put(e.index(), e));
      }
    }

    PersistedState load() {
      return new PersistedState(hardState, snapshot, List.copyOf(entries.values()));
    }

    long lastIndex() {
      return entries.isEmpty() ? snapshot.index() : entries.lastKey();
    }
  }
}

package io.distdb.sim;

import io.distdb.common.NodeId;
import io.distdb.env.RandomSource;
import io.distdb.env.Scheduler;
import io.distdb.env.Transport;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * The simulated network: a {@link Transport} per node, with every fault a real network has and
 * nothing it doesn't. Messages may be lost, duplicated, delayed and therefore reordered, and a
 * partition cuts links in both directions; but a delivered message is never corrupted or sent to
 * the wrong node. Delivery runs on the simulator's {@link Scheduler}, so it is part of the seeded,
 * replayable run.
 */
public final class SimNetwork {

  /**
   * Fault rates.
   *
   * @param dropRate chance a message is lost
   * @param duplicateRate chance a message is delivered twice
   * @param minDelay the shortest usual delivery delay
   * @param maxDelay the longest usual delivery delay
   * @param slowRate chance a message is slow instead
   * @param slowMaxDelay the longest delay of a slow message
   */
  public record Faults(
      double dropRate,
      double duplicateRate,
      Duration minDelay,
      Duration maxDelay,
      double slowRate,
      Duration slowMaxDelay) {

    /** 2% lost, 1% duplicated, 1–5 ms usually, 3% up to 200 ms. */
    public static Faults defaults() {
      return new Faults(
          0.02, 0.01, Duration.ofMillis(1), Duration.ofMillis(5), 0.03, Duration.ofMillis(200));
    }
  }

  /** Counters, for checking that the faults actually happened. */
  public static final class Stats {
    public long sent;
    public long delivered;
    public long dropped;
    public long duplicated;
    public long cut;
  }

  private static final Comparator<NodeId> BY_VALUE = Comparator.comparingInt(NodeId::value);

  private final Scheduler scheduler;
  private final RandomSource random;
  private final Faults faults;
  private final Map<NodeId, Endpoint> endpoints = new TreeMap<>(BY_VALUE);
  private final Set<Long> blocked = new HashSet<>();
  private final Stats stats = new Stats();

  public SimNetwork(Scheduler scheduler, RandomSource random, Faults faults) {
    this.scheduler = Objects.requireNonNull(scheduler);
    this.random = Objects.requireNonNull(random);
    this.faults = Objects.requireNonNull(faults);
  }

  /** The transport for {@code id}, created on first use. */
  public Transport endpoint(NodeId id) {
    return endpoints.computeIfAbsent(id, Endpoint::new);
  }

  /** Cuts every link between {@code side} and everyone else. */
  public void partition(Collection<NodeId> side) {
    for (NodeId a : endpoints.keySet()) {
      for (NodeId b : endpoints.keySet()) {
        if (side.contains(a) != side.contains(b)) {
          blocked.add(link(a, b));
        }
      }
    }
  }

  /** Cuts {@code id} off from everyone. */
  public void isolate(NodeId id) {
    partition(Set.of(id));
  }

  /** Restores every link. */
  public void heal() {
    blocked.clear();
  }

  public boolean connected(NodeId a, NodeId b) {
    return !blocked.contains(link(a, b));
  }

  public Stats stats() {
    return stats;
  }

  private void route(NodeId from, NodeId to, byte[] message) {
    stats.sent++;
    if (!connected(from, to)) {
      stats.cut++;
      return;
    }
    if (random.nextDouble() < faults.dropRate()) {
      stats.dropped++;
      return;
    }
    int copies = 1;
    if (random.nextDouble() < faults.duplicateRate()) {
      copies = 2;
      stats.duplicated++;
    }
    for (int i = 0; i < copies; i++) {
      scheduler.schedule(delay(), () -> deliver(from, to, message));
    }
  }

  private void deliver(NodeId from, NodeId to, byte[] message) {
    Endpoint e = endpoints.get(to);
    // A partition that started while the message was in flight catches it too.
    if (e == null || e.handler == null || !connected(from, to)) {
      stats.cut++;
      return;
    }
    stats.delivered++;
    e.handler.onMessage(from, message);
  }

  private Duration delay() {
    if (random.nextDouble() < faults.slowRate()) {
      return Duration.ofNanos(random.nextLong(0, faults.slowMaxDelay().toNanos() + 1));
    }
    return Duration.ofNanos(
        random.nextLong(faults.minDelay().toNanos(), faults.maxDelay().toNanos() + 1));
  }

  private static long link(NodeId a, NodeId b) {
    int lo = Math.min(a.value(), b.value());
    int hi = Math.max(a.value(), b.value());
    return ((long) lo << 32) | hi;
  }

  private final class Endpoint implements Transport {
    private final NodeId id;
    private MessageHandler handler;

    Endpoint(NodeId id) {
      this.id = id;
    }

    @Override
    public NodeId localNode() {
      return id;
    }

    @Override
    public void send(NodeId to, byte[] message) {
      route(id, to, message);
    }

    /** A null handler drops everything: the process is down. */
    @Override
    public void setHandler(MessageHandler handler) {
      this.handler = handler;
    }
  }
}

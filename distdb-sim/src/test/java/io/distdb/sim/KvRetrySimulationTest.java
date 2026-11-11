package io.distdb.sim;

import static org.assertj.core.api.Assertions.assertThat;

import io.distdb.client.ClientOptions;
import io.distdb.client.KvClient;
import io.distdb.client.KvException;
import io.distdb.common.Bytes;
import io.distdb.env.RandomSource;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.kv.KeySpace;
import io.distdb.kv.KvService;
import io.distdb.kv.KvServiceOptions;
import io.distdb.rpc.KvChannel;
import io.distdb.rpc.RpcException;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.storage.EngineSnapshot;
import io.distdb.storage.KvEngine;
import io.distdb.storage.KvIterator;
import io.distdb.storage.StorageException;
import io.distdb.storage.WriteBatch;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.storage.lsm.LsmEngine;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/**
 * Clients retrying against one node over a network that loses, delays and duplicates messages,
 * while the node loses power, and its disk sometimes fails in the middle of a write. Everything
 * runs on one thread from one seed, so any failure replays exactly.
 *
 * <p>The property checked is the point of request ids: <b>every write takes effect at most once,
 * every acknowledged write exactly once, and every write reported as certainly failed not at
 * all.</b> "Takes effect" is counted at the engine: a write that returned, or one that failed but
 * whose batch was found on disk after the restart.
 *
 * <p>{@code -Ddistdb.sim.kvSeeds=N} runs seeds 1..N (default 100); {@code -Ddistdb.sim.kvSeed=S}
 * replays seed S alone.
 */
class KvRetrySimulationTest {

  private static final int CLIENTS = 4;
  private static final int OPS_PER_CLIENT = 40;
  private static final Path DIR = Path.of("/node");
  private static final EngineOptions SMALL =
      new EngineOptions(8 * 1024, 512, 10, 16 * 1024, 2, 8, 64 * 1024, 4);

  @Test
  void everyWriteTakesEffectAtMostOnceAndEveryAcknowledgedOneExactlyOnce() {
    Long only = Long.getLong("distdb.sim.kvSeed");
    int seeds = Integer.getInteger("distdb.sim.kvSeeds", 100);
    Stats total = new Stats();
    for (long seed = only == null ? 1 : only; seed <= (only == null ? seeds : only); seed++) {
      try {
        total.add(new Run(seed).run());
      } catch (AssertionError | RuntimeException e) {
        throw new AssertionError(
            "seed " + seed + " failed; replay with -Ddistdb.sim.kvSeed=" + seed, e);
      }
    }
    System.out.println("KvRetrySimulationTest: " + total);
    // The faults must actually have happened, or the property was checked against nothing.
    assertThat(total.acknowledged).isPositive();
    if (only == null) {
      assertThat(total.powerCuts).isPositive();
      assertThat(total.diskFailures).isPositive();
      assertThat(total.extraCalls).isPositive();
      assertThat(total.unknownOutcomes).isPositive();
      assertThat(total.answeredFromRecord).isPositive();
      assertThat(total.writtenAfterFailure).isPositive();
      assertThat(total.definitelyFailed).isPositive();
    }
  }

  private static final class Stats {
    long acknowledged;
    long unknownOutcomes;
    long definitelyFailed;
    long extraCalls;
    long powerCuts;
    long diskFailures;
    long answeredFromRecord;
    long writtenAfterFailure;

    void add(Stats s) {
      acknowledged += s.acknowledged;
      unknownOutcomes += s.unknownOutcomes;
      definitelyFailed += s.definitelyFailed;
      extraCalls += s.extraCalls;
      powerCuts += s.powerCuts;
      diskFailures += s.diskFailures;
      answeredFromRecord += s.answeredFromRecord;
      writtenAfterFailure += s.writtenAfterFailure;
    }

    @Override
    public String toString() {
      return String.format(
          "writes acknowledged %d, unknown %d, certainly failed %d; extra calls %d; power cuts %d,"
              + " disk failures %d; retries answered from the record %d; failed writes found on"
              + " disk after restart %d",
          acknowledged,
          unknownOutcomes,
          definitelyFailed,
          extraCalls,
          powerCuts,
          diskFailures,
          answeredFromRecord,
          writtenAfterFailure);
    }
  }

  /** What the client finally reported for a write. */
  private enum Outcome {
    ACKNOWLEDGED,
    UNKNOWN,
    FAILED
  }

  private static final class Run {
    private final RandomSource random;
    private final SimClock clock = new SimClock();
    private final SimScheduler scheduler = new SimScheduler(clock);
    private final SimFileSystem fs;
    private final Stats stats = new Stats();

    /** Times each write (by request id) took effect at the engine. */
    private final Map<String, Integer> effects = new HashMap<>();

    /** What the client reported for each write. */
    private final Map<String, Outcome> outcomes = new HashMap<>();

    /** Writes whose engine write failed in the current process life: did they reach the disk? */
    private final List<Pending> uncertain = new ArrayList<>();

    private record Pending(String requestId, long lastSequence) {}

    // The node process. A power cut ends its life: its timers are dropped and it serves nothing.
    private Life life;
    private LsmEngine engine;
    private KvService service;
    private boolean up;
    private int clientsDone;

    /** The request the node is handling right now; the engine attributes writes to it. */
    private String handling;

    Run(long seed) {
      this.random = SplittableRandomSource.seeded(seed);
      this.fs = new SimFileSystem(random.split());
    }

    Stats run() {
      boot();
      schedulePowerCut();
      for (int i = 0; i < CLIENTS; i++) {
        new SimClient(i, random.split()).next();
      }
      long cap = clock.nanoTime() + Duration.ofHours(2).toNanos();
      while (clientsDone < CLIENTS) {
        assertThat(scheduler.runNext()).as("the simulation ran out of work").isTrue();
        assertThat(clock.nanoTime()).as("clients stopped making progress").isLessThan(cap);
      }
      if (!up) {
        boot(); // settle whether the last failed write reached the disk
      }
      check();
      return stats;
    }

    private void check() {
      for (Map.Entry<String, Outcome> e : outcomes.entrySet()) {
        int n = effects.getOrDefault(e.getKey(), 0);
        switch (e.getValue()) {
          case ACKNOWLEDGED -> assertThat(n).as("acknowledged write %s", e.getKey()).isEqualTo(1);
          case FAILED -> assertThat(n).as("write %s reported as not applied", e.getKey()).isZero();
          case UNKNOWN -> assertThat(n).as("write %s of unknown outcome", e.getKey()).isLessThan(2);
        }
      }
      for (Map.Entry<String, Integer> e : effects.entrySet()) {
        assertThat(e.getValue()).as("write %s took effect", e.getKey()).isLessThanOrEqualTo(1);
      }
    }

    // ---------------------------------------------------------------------------------------------
    // The node

    private void boot() {
      while (true) {
        life = new Life(scheduler);
        try {
          engine = LsmEngine.open(fs, life, DIR, SMALL);
          break;
        } catch (StorageException e) {
          assertThat(fs.isDead()).as("recovery failed on its own: " + e).isTrue();
          fs.crash(); // the disk failed during recovery; power-cycle and try again
        }
      }
      // A failed write either reached the disk or not, all of it or none of it (Phase 2's prefix
      // property). Recovery's last sequence number says which.
      for (Pending p : uncertain) {
        if (engine.lastSequence() >= p.lastSequence()) {
          effects.merge(p.requestId(), 1, Integer::sum);
          stats.writtenAfterFailure++;
        }
      }
      uncertain.clear();
      service =
          new KvService(
              new CountingEngine(engine),
              clock,
              life,
              KvServiceOptions.defaults(),
              failure -> {
                // Crash-only: the process dies on a storage error and is restarted.
                stats.diskFailures++;
                scheduler.execute(this::powerCut);
              });
      up = true;
      if (random.nextInt(3) == 0) {
        fs.crashAfter(random.nextInt(1, 300)); // the disk will fail mid-operation
      }
    }

    private void powerCut() {
      if (!up) {
        return;
      }
      up = false;
      life.end();
      fs.crash();
      stats.powerCuts++;
      scheduler.schedule(Duration.ofMillis(random.nextInt(0, 1500)), this::boot);
    }

    private void schedulePowerCut() {
      if (clientsDone == CLIENTS) {
        return;
      }
      scheduler.schedule(
          Duration.ofMillis(random.nextInt(500, 6000)),
          () -> {
            powerCut();
            schedulePowerCut();
          });
    }

    /** Counts, per request, the writes to client data that reached the engine. */
    private final class CountingEngine implements KvEngine {
      private final LsmEngine delegate;

      CountingEngine(LsmEngine delegate) {
        this.delegate = delegate;
      }

      @Override
      public void write(WriteBatch batch, boolean sync) {
        boolean[] touchesUserData = {false};
        batch.forEach(
            new WriteBatch.Visitor() {
              @Override
              public void put(byte[] key, byte[] value) {
                touchesUserData[0] |= KeySpace.isUser(key);
              }

              @Override
              public void delete(byte[] key) {
                touchesUserData[0] |= KeySpace.isUser(key);
              }
            });
        if (!touchesUserData[0]) {
          delegate.write(batch, sync);
          return;
        }
        assertThat(handling).as("a client write outside any request").isNotNull();
        long last = delegate.lastSequence() + batch.count();
        try {
          delegate.write(batch, sync);
        } catch (StorageException e) {
          uncertain.add(new Pending(handling, last));
          throw e;
        }
        effects.merge(handling, 1, Integer::sum);
      }

      @Override
      public Optional<byte[]> get(byte[] key) {
        return delegate.get(key);
      }

      @Override
      public KvIterator scan(byte[] startInclusive, byte[] endExclusive) {
        return delegate.scan(startInclusive, endExclusive);
      }

      @Override
      public EngineSnapshot snapshot() {
        return delegate.snapshot();
      }

      @Override
      public void close() {}
    }

    // ---------------------------------------------------------------------------------------------
    // The network

    /** One client's connection: loses, delays and duplicates messages in both directions. */
    private final class LossyChannel implements KvChannel {
      private final RandomSource net;
      String lastWrite;
      long calls;

      LossyChannel(RandomSource net) {
        this.net = net;
      }

      @Override
      public CompletableFuture<KvResponse> call(KvRequest request, Duration timeout) {
        calls++;
        if (request.hasRequestId()) {
          lastWrite = id(request);
        }
        CompletableFuture<KvResponse> reply = new CompletableFuture<>();
        long deadline = clock.nanoTime() + timeout.toNanos();
        scheduler.schedule(
            timeout,
            () -> reply.completeExceptionally(new RpcException(RpcException.Kind.TIMEOUT, "lost")));
        if (!up && net.nextBoolean()) {
          // The node is down and the connection is refused: certainly not delivered.
          reply.completeExceptionally(
              new RpcException(RpcException.Kind.UNAVAILABLE, false, "refused", null));
          return reply;
        }
        if (net.nextInt(100) >= 10) { // 10% of requests are lost
          deliverLater(request, deadline, reply);
        }
        if (net.nextInt(100) < 5) {
          deliverLater(request, deadline, null); // a stray duplicate nobody waits for
        }
        return reply;
      }

      private void deliverLater(
          KvRequest request, long deadline, CompletableFuture<KvResponse> reply) {
        scheduler.schedule(
            delay(),
            () -> {
              if (!up) {
                return; // arrived while the node was down
              }
              String write = request.hasRequestId() ? id(request) : null;
              int effectsBefore = write == null ? 0 : effects.getOrDefault(write, 0);
              handling = write;
              KvResponse response;
              try {
                response = service.handle(request, deadline).join();
              } finally {
                handling = null;
              }
              if (write != null && !response.hasError() && effectsBefore > 0) {
                assertThat(effects.get(write)).as("a retry of %s was applied", write).isEqualTo(1);
                stats.answeredFromRecord++;
              }
              if (response.hasError() && response.getError().hasStaleRequest()) {
                // Only a copy of an attempt the client has finished with can be stale.
                assertThat(reply == null || reply.isDone())
                    .as("a stale answer for %s reached a client still waiting", write)
                    .isTrue();
              }
              if (reply != null && net.nextInt(100) >= 10) { // 10% of replies are lost
                scheduler.schedule(delay(), () -> reply.complete(response));
              }
            });
      }

      /** Usually a few milliseconds; sometimes seconds, longer than an attempt waits. */
      private Duration delay() {
        return net.nextInt(10) == 0
            ? Duration.ofMillis(net.nextInt(0, 3000))
            : Duration.ofMillis(net.nextInt(0, 20));
      }

      @Override
      public void close() {}
    }

    // ---------------------------------------------------------------------------------------------
    // The clients

    private final class SimClient {
      private final int number;
      private final RandomSource choices;
      private final LossyChannel channel;
      private final KvClient client;
      private int remaining = OPS_PER_CLIENT;
      private int written;

      SimClient(int number, RandomSource random) {
        this.number = number;
        this.choices = random.split();
        this.channel = new LossyChannel(random.split());
        this.client =
            new KvClient(channel, clock, scheduler, random.split(), ClientOptions.defaults());
      }

      void next() {
        if (remaining-- == 0) {
          clientsDone++;
          stats.extraCalls += channel.calls - OPS_PER_CLIENT; // retries and scan pages
          return;
        }
        byte[] key = utf8("k" + choices.nextInt(6));
        // Mostly the default 5 s; sometimes short, so some writes give up while the node is down.
        Duration timeout =
            choices.nextInt(10) == 0
                ? Duration.ofMillis(choices.nextInt(50, 1000))
                : ClientOptions.defaults().defaultTimeout();
        int dice = choices.nextInt(100);
        CompletableFuture<?> op;
        boolean write = dice < 70;
        if (dice < 55) {
          op = client.put(key, utf8("c" + number + "-" + written++), timeout);
        } else if (dice < 70) {
          op = client.delete(key, timeout);
        } else if (dice < 90) {
          op = client.get(key, timeout);
        } else {
          op = client.scan(new byte[0], null, 4, timeout);
        }
        String requestId = write ? channel.lastWrite : null;
        op.whenComplete((value, failure) -> {
          record(requestId, failure);
          scheduler.schedule(Duration.ofMillis(choices.nextInt(0, 50)), this::next);
        });
      }

      private void record(String requestId, Throwable failure) {
        Throwable cause = failure instanceof CompletionException c ? c.getCause() : failure;
        if (cause != null) {
          assertThat(cause).isInstanceOf(KvException.class);
          KvException e = (KvException) cause;
          assertThat(e.code()).as("%s", e).isEqualTo(KvException.Code.DEADLINE_EXCEEDED);
        }
        if (requestId == null) {
          return; // a read
        }
        Outcome outcome;
        if (cause == null) {
          outcome = Outcome.ACKNOWLEDGED;
          stats.acknowledged++;
        } else if (((KvException) cause).outcomeUnknown()) {
          outcome = Outcome.UNKNOWN;
          stats.unknownOutcomes++;
        } else {
          outcome = Outcome.FAILED;
          stats.definitelyFailed++;
        }
        assertThat(outcomes.put(requestId, outcome)).as("one outcome per write").isNull();
      }
    }

    private static String id(KvRequest request) {
      return Bytes.toHex(request.getRequestId().getClientId().toByteArray())
          + "/"
          + request.getRequestId().getSeq();
    }
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}

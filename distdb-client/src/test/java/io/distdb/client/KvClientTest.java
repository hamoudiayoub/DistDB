package io.distdb.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimScheduler;
import io.distdb.rpc.KvChannel;
import io.distdb.rpc.KvErrors;
import io.distdb.rpc.RpcException;
import io.distdb.rpc.proto.KeyValue;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.PutResponse;
import io.distdb.rpc.proto.ScanResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class KvClientTest {

  private static final KvResponse PUT_OK =
      KvResponse.newBuilder().setPut(PutResponse.getDefaultInstance()).build();

  private final SimClock clock = new SimClock();
  private final SimScheduler scheduler = new SimScheduler(clock);
  private final ScriptedChannel channel = new ScriptedChannel();
  private final KvClient client =
      new KvClient(
          channel, clock, scheduler, SplittableRandomSource.seeded(42), ClientOptions.defaults());

  @Test
  void retriesAfterLostAttemptsKeepTheSameRequestId() {
    channel.script(lost(), unavailable(), respond(PUT_OK));
    CompletableFuture<Void> put = client.put(utf8("k"), utf8("v"));
    scheduler.runUntilIdle();
    assertThat(put).isCompleted();
    put.join();
    assertThat(channel.sent).hasSize(3);
    assertThat(channel.sent.stream().map(s -> s.request.getRequestId()).distinct()).hasSize(1);
    assertThat(channel.sent.getFirst().request.getRequestId().getSeq()).isEqualTo(1);
  }

  @Test
  void backoffDoublesWithFullJitterAndGivesUpAtTheDeadline() {
    channel.otherwise(busy());
    CompletableFuture<Void> put = client.put(utf8("k"), utf8("v"));
    scheduler.runUntilIdle();

    KvException e = failure(put);
    assertThat(e.code()).isEqualTo(KvException.Code.DEADLINE_EXCEEDED);
    assertThat(e.outcomeUnknown())
        .as("every attempt was refused, so nothing was applied")
        .isFalse();
    assertThat(clock.nanoTime()).isLessThanOrEqualTo(Duration.ofSeconds(5).toNanos());

    Set<Long> gaps = new HashSet<>();
    for (int i = 1; i < channel.sent.size(); i++) {
      long gap = channel.sent.get(i).atNanos - channel.sent.get(i - 1).atNanos;
      long ceiling =
          Math.min(Duration.ofMillis(20).toNanos() << (i - 1), Duration.ofSeconds(1).toNanos());
      assertThat(gap).as("wait before attempt %d", i + 1).isBetween(0L, ceiling);
      gaps.add(gap);
    }
    assertThat(channel.sent.size()).isGreaterThan(5);
    assertThat(gaps).as("jitter spreads the waits").hasSizeGreaterThan(channel.sent.size() / 2);
  }

  @Test
  void aWriteThatRunsOutOfTimeAfterALostAttemptHasAnUnknownOutcome() {
    channel.otherwise(lost());
    CompletableFuture<Void> put = client.put(utf8("k"), utf8("v"), Duration.ofSeconds(5));
    scheduler.runUntilIdle();

    KvException e = failure(put);
    assertThat(e.code()).isEqualTo(KvException.Code.DEADLINE_EXCEEDED);
    assertThat(e.outcomeUnknown()).isTrue();
    assertThat(e).hasMessageContaining("may or may not have been applied");
    // Each attempt waits at most attemptTimeout, and never past the overall deadline.
    long deadline = Duration.ofSeconds(5).toNanos();
    for (Sent s : channel.sent) {
      assertThat(s.timeout).isLessThanOrEqualTo(Duration.ofSeconds(2));
      assertThat(s.atNanos + s.timeout.toNanos()).isLessThanOrEqualTo(deadline);
    }
    assertThat(channel.sent.getFirst().timeout).isEqualTo(Duration.ofSeconds(2));
  }

  @Test
  void refusedConnectionsMeanTheWriteWasNotApplied() {
    channel.otherwise(refused());
    CompletableFuture<Void> put = client.put(utf8("k"), utf8("v"), Duration.ofSeconds(1));
    scheduler.runUntilIdle();
    KvException e = failure(put);
    assertThat(e.code()).isEqualTo(KvException.Code.DEADLINE_EXCEEDED);
    assertThat(e.outcomeUnknown()).isFalse();
  }

  @Test
  void readsNeverHaveAnUnknownOutcome() {
    channel.otherwise(lost());
    CompletableFuture<?> get = client.get(utf8("k"), Duration.ofSeconds(3));
    scheduler.runUntilIdle();
    assertThat(failure(get).outcomeUnknown()).isFalse();
  }

  @Test
  void anInvalidRequestIsNotRetried() {
    channel.otherwise(respond(KvErrors.invalidArgument("key too long")));
    CompletableFuture<Void> put = client.put(utf8("k"), utf8("v"));
    scheduler.runUntilIdle();
    KvException e = failure(put);
    assertThat(e.code()).isEqualTo(KvException.Code.INVALID_ARGUMENT);
    assertThat(e.outcomeUnknown()).isFalse();
    assertThat(channel.sent).hasSize(1);
  }

  @Test
  void aStorageFailureIsRetriedBecauseTheNodeRestarts() {
    channel.script(respond(KvErrors.storageFailure(true, "disk")), unavailable(), respond(PUT_OK));
    CompletableFuture<Void> put = client.put(utf8("k"), utf8("v"));
    scheduler.runUntilIdle();
    put.join();
  }

  @Test
  void aStorageFailureDuringTheWriteMakesTheOutcomeUnknown() {
    channel.script(respond(KvErrors.storageFailure(true, "fsync failed")));
    channel.otherwise(respond(KvErrors.storageFailure(false, "restarting")));
    CompletableFuture<Void> put = client.put(utf8("k"), utf8("v"), Duration.ofSeconds(1));
    scheduler.runUntilIdle();
    assertThat(failure(put).outcomeUnknown()).isTrue();

    channel.script();
    CompletableFuture<Void> refused = client.put(utf8("k"), utf8("v"), Duration.ofSeconds(1));
    scheduler.runUntilIdle();
    assertThat(failure(refused).outcomeUnknown()).isFalse();
  }

  @Test
  void theFloorIsTheOldestWriteStillWaitingForAnAnswer() {
    Map<Long, CompletableFuture<KvResponse>> held = new HashMap<>();
    channel.otherwise((request, timeout) ->
        held.computeIfAbsent(request.getRequestId().getSeq(), s -> new CompletableFuture<>()));
    CompletableFuture<Void> w1 = client.put(utf8("a"), utf8("1"), Duration.ofMinutes(1));
    client.put(utf8("b"), utf8("2"), Duration.ofMinutes(1));
    client.put(utf8("c"), utf8("3"), Duration.ofMinutes(1));
    assertThat(floors()).containsExactly(1L, 1L, 1L);

    held.get(1L).complete(PUT_OK);
    assertThat(w1).isCompleted();
    client.put(utf8("d"), utf8("4"), Duration.ofMinutes(1));
    assertThat(floors().getLast()).isEqualTo(2);

    held.get(3L).complete(PUT_OK); // out of order: 2 is still waiting
    client.put(utf8("e"), utf8("5"), Duration.ofMinutes(1));
    assertThat(floors().getLast()).isEqualTo(2);

    held.get(2L).complete(PUT_OK);
    client.put(utf8("f"), utf8("6"), Duration.ofMinutes(1));
    assertThat(floors().getLast()).isEqualTo(4);
  }

  @Test
  void aRetrySendsTheFloorAsItIsNow() {
    CompletableFuture<KvResponse> first = new CompletableFuture<>();
    channel.script((request, timeout) -> first, lost(), respond(PUT_OK));
    client.put(utf8("a"), utf8("1"), Duration.ofMinutes(1)); // seq 1, held
    CompletableFuture<Void> w2 = client.put(utf8("b"), utf8("2"), Duration.ofMinutes(1));
    first.complete(PUT_OK); // seq 1 answered while seq 2 waits to retry
    scheduler.runUntilIdle();
    w2.join();
    assertThat(floors()).containsExactly(1L, 1L, 2L);
  }

  @Test
  void anAbandonedWriteNoLongerHoldsTheFloorDown() {
    channel.otherwise(lost());
    CompletableFuture<Void> lostWrite = client.put(utf8("a"), utf8("1"), Duration.ofSeconds(1));
    scheduler.runUntilIdle();
    assertThat(failure(lostWrite).outcomeUnknown()).isTrue();
    channel.otherwise(respond(PUT_OK));
    client.put(utf8("b"), utf8("2")).join();
    // The client will never send seq 1 again, so the server may reject any late copy of it.
    assertThat(floors().getLast()).isEqualTo(2);
  }

  @Test
  void scanFollowsResumeKeysAndStopsAtTheLimit() {
    channel.script(
        respond(page(List.of("a", "b"), "b\0")),
        respond(page(List.of("c", "d"), "d\0")),
        respond(page(List.of("e"), null)));
    CompletableFuture<List<Entry>> scan = client.scan(new byte[0], null, 3);
    scheduler.runUntilIdle();
    assertThat(scan.join().stream().map(e -> new String(e.key(), StandardCharsets.UTF_8)))
        .containsExactly("a", "b", "c");
    assertThat(channel.sent).hasSize(2);
    assertThat(channel.sent.get(0).request.getScan().getLimit()).isEqualTo(3);
    assertThat(channel.sent.get(1).request.getScan().getStartKey().toStringUtf8()).isEqualTo("b\0");
    assertThat(channel.sent.get(1).request.getScan().getLimit()).isEqualTo(1);
    assertThat(channel.sent.get(1).request.getScan().hasEndKey()).isFalse();

    CompletableFuture<List<Entry>> rest = client.scan(utf8("e"), utf8("z"), 10);
    assertThat(rest.join()).hasSize(1);
    assertThat(channel.sent.getLast().request.getScan().getEndKey().toStringUtf8()).isEqualTo("z");
  }

  @Test
  void timeoutsMustBePositiveAndBounded() {
    assertThatThrownBy(() -> client.get(utf8("k"), Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> client.put(utf8("k"), utf8("v"), Duration.ofMinutes(6)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(channel.sent).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // The scripted channel

  private record Sent(KvRequest request, Duration timeout, long atNanos) {}

  @FunctionalInterface
  private interface Behavior {
    CompletableFuture<KvResponse> answer(KvRequest request, Duration timeout);
  }

  private final class ScriptedChannel implements KvChannel {
    final List<Sent> sent = new ArrayList<>();
    private final Deque<Behavior> script = new ArrayDeque<>();
    private Behavior otherwise = (r, t) -> CompletableFuture.failedFuture(new AssertionError());

    void script(Behavior... behaviors) {
      script.addAll(List.of(behaviors));
    }

    void otherwise(Behavior behavior) {
      otherwise = behavior;
    }

    @Override
    public CompletableFuture<KvResponse> call(KvRequest request, Duration timeout) {
      sent.add(new Sent(request, timeout, clock.nanoTime()));
      Behavior b = script.isEmpty() ? otherwise : script.poll();
      return b.answer(request, timeout);
    }

    @Override
    public void close() {}
  }

  private static Behavior respond(KvResponse response) {
    return (r, t) -> CompletableFuture.completedFuture(response);
  }

  private static Behavior busy() {
    return respond(KvErrors.serverBusy("busy"));
  }

  /** The request or its reply is lost: the call times out. */
  private Behavior lost() {
    return (r, t) -> {
      CompletableFuture<KvResponse> f = new CompletableFuture<>();
      scheduler.schedule(
          t, () -> f.completeExceptionally(new RpcException(RpcException.Kind.TIMEOUT, "lost")));
      return f;
    };
  }

  private static Behavior unavailable() {
    return (r, t) ->
        CompletableFuture.failedFuture(
            new RpcException(RpcException.Kind.UNAVAILABLE, true, "connection reset", null));
  }

  private static Behavior refused() {
    return (r, t) ->
        CompletableFuture.failedFuture(
            new RpcException(RpcException.Kind.UNAVAILABLE, false, "connection refused", null));
  }

  private List<Long> floors() {
    return channel.sent.stream()
        .filter(s -> s.request.hasRequestId())
        .map(s -> s.request.getFirstIncompleteSeq())
        .toList();
  }

  private static KvResponse page(List<String> keys, String resume) {
    ScanResponse.Builder page = ScanResponse.newBuilder();
    for (String k : keys) {
      page.addEntries(
          KeyValue.newBuilder()
              .setKey(ByteString.copyFromUtf8(k))
              .setValue(ByteString.copyFromUtf8("v")));
    }
    if (resume != null) {
      page.setResumeKey(ByteString.copyFromUtf8(resume));
    }
    return KvResponse.newBuilder().setScan(page).build();
  }

  private static KvException failure(CompletableFuture<?> f) {
    assertThat(f).isCompletedExceptionally();
    try {
      f.join();
    } catch (CompletionException e) {
      assertThat(e.getCause()).isInstanceOf(KvException.class);
      return (KvException) e.getCause();
    }
    throw new AssertionError("not failed");
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}

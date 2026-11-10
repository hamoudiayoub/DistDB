package io.distdb.kv;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.distdb.env.SplittableRandomSource;
import io.distdb.env.sim.SimClock;
import io.distdb.env.sim.SimFileSystem;
import io.distdb.env.sim.SimScheduler;
import io.distdb.rpc.KvHandler;
import io.distdb.rpc.proto.DeleteRequest;
import io.distdb.rpc.proto.Error;
import io.distdb.rpc.proto.GetRequest;
import io.distdb.rpc.proto.KeyValue;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.PutRequest;
import io.distdb.rpc.proto.RequestId;
import io.distdb.rpc.proto.ScanRequest;
import io.distdb.rpc.proto.ScanResponse;
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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KvServiceTest {

  private static final Path DIR = Path.of("/node");
  private static final EngineOptions SMALL =
      new EngineOptions(16 * 1024, 1024, 10, 32 * 1024, 4, 12, 128 * 1024, 10);

  private final SimFileSystem fs = new SimFileSystem(SplittableRandomSource.seeded(3));
  private SimClock clock;
  private SimScheduler scheduler;
  private LsmEngine engine;
  private KvService service;
  private final List<Throwable> storageFailures = new ArrayList<>();

  private static final byte[] ALICE = id(1);
  private static final byte[] BOB = id(2);

  @BeforeEach
  void open() {
    clock = new SimClock();
    scheduler = new SimScheduler(clock);
    engine = LsmEngine.open(fs, scheduler, DIR, SMALL);
    service = newService(engine, KvServiceOptions.defaults());
  }

  @AfterEach
  void close() {
    service.close();
    engine.close();
  }

  private KvService newService(KvEngine e, KvServiceOptions options) {
    return new KvService(e, clock, scheduler, options, storageFailures::add);
  }

  // ---------------------------------------------------------------------------------------------
  // Basic operations

  @Test
  void putGetDeleteRoundTrip() {
    assertThat(get("k")).isEmpty();
    assertThat(put(ALICE, 1, 1, "k", "v1").hasPut()).isTrue();
    assertThat(get("k")).contains("v1");
    put(ALICE, 2, 2, "k", "v2");
    assertThat(get("k")).contains("v2");
    assertThat(delete(ALICE, 3, 3, "k").hasDelete()).isTrue();
    assertThat(get("k")).isEmpty();
  }

  @Test
  void scanPagesFollowResumeKeysToTheEnd() {
    for (int i = 0; i < 2500; i++) {
      put(ALICE, i + 1, i + 1, key(i), "v" + i);
    }
    List<String> seen = new ArrayList<>();
    byte[] start = new byte[0];
    int pages = 0;
    while (true) {
      ScanResponse page = scan(start, null, 0).getScan();
      pages++;
      page.getEntriesList().forEach(kv -> seen.add(kv.getKey().toStringUtf8()));
      if (!page.hasResumeKey()) {
        break;
      }
      assertThat(page.getEntriesCount()).isEqualTo(1000); // the default page size
      start = page.getResumeKey().toByteArray();
    }
    assertThat(pages).isEqualTo(3);
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 2500; i++) {
      expected.add(key(i));
    }
    assertThat(seen).isEqualTo(expected);
  }

  @Test
  void scanRespectsBoundsAndLimit() {
    for (int i = 0; i < 20; i++) {
      put(ALICE, i + 1, i + 1, key(i), "v");
    }
    ScanResponse page = scan(utf8(key(5)), utf8(key(9)), 0).getScan();
    assertThat(keys(page)).containsExactly(key(5), key(6), key(7), key(8));
    assertThat(page.hasResumeKey()).isFalse();

    page = scan(utf8(key(5)), utf8(key(9)), 2).getScan();
    assertThat(keys(page)).containsExactly(key(5), key(6));
    assertThat(page.getResumeKey().toByteArray()).isEqualTo(utf8(key(6) + "\0"));

    assertThat(scan(utf8(key(9)), utf8(key(5)), 0).getScan().getEntriesCount()).isZero();
    assertThat(scan(utf8(key(5)), utf8(key(5)), 0).getScan().getEntriesCount()).isZero();
  }

  @Test
  void scanPageStopsAtItsByteBudgetButAlwaysMakesProgress() {
    KvServiceOptions d = KvServiceOptions.defaults();
    service.close();
    service =
        newService(
            engine,
            new KvServiceOptions(
                d.maxKeyBytes(),
                d.maxValueBytes(),
                d.defaultScanLimit(),
                d.maxScanLimit(),
                100,
                d.maxInflightRequests(),
                d.sessionTtl(),
                d.sessionSweepInterval()));
    put(ALICE, 1, 1, "a", "x".repeat(500)); // alone larger than a page
    put(ALICE, 2, 2, "b", "x".repeat(33));
    put(ALICE, 3, 3, "c", "x".repeat(33));
    put(ALICE, 4, 4, "d", "x".repeat(33));
    put(ALICE, 5, 5, "e", "x".repeat(33));

    List<List<String>> pages = new ArrayList<>();
    byte[] start = new byte[0];
    while (true) {
      ScanResponse page = scan(start, null, 0).getScan();
      pages.add(keys(page));
      if (!page.hasResumeKey()) {
        break;
      }
      start = page.getResumeKey().toByteArray();
    }
    assertThat(pages)
        .containsExactly(
            List.of("a"),
            List.of("b", "c", "d"),
            List.of("e")); // 34 bytes each: a page stops once it reaches 100
  }

  @Test
  void resumeKeyAfterTheLongestAllowedKeyIsAccepted() {
    // A resume key is the last key plus 0x00, one byte over the key limit. Scans must accept it,
    // or a scan that meets a maximum-length key could never continue.
    String longest = "k".repeat(KvServiceOptions.defaults().maxKeyBytes());
    put(ALICE, 1, 1, longest, "1");
    put(ALICE, 2, 2, "z", "2");
    ScanResponse first = scan(new byte[0], null, 1).getScan();
    assertThat(keys(first)).containsExactly(longest);
    ScanResponse second = scan(first.getResumeKey().toByteArray(), null, 1).getScan();
    assertThat(keys(second)).containsExactly("z");
  }

  @Test
  void theNodesOwnRecordsAreInvisibleToClients() {
    put(ALICE, 1, 1, "k", "v"); // also stores ALICE's session and the write's result
    assertThat(keys(scan(new byte[0], null, 0).getScan())).containsExactly("k");
    // A client key that spells a session record is just a client key.
    byte[] spoof = ClientSessions.sessionKey(ALICE);
    assertThat(get(spoof)).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // Idempotency

  @Test
  void aRetriedPutIsAnsweredNotAppliedAgain() {
    KvResponse first = put(ALICE, 1, 1, "k", "alice");
    put(BOB, 1, 1, "k", "bob");
    // ALICE never saw her answer and retries. Applying it again would overwrite BOB's later
    // write, an order no single execution of the two writes could produce.
    KvResponse retry = put(ALICE, 1, 1, "k", "alice");
    assertThat(retry).isEqualTo(first);
    assertThat(get("k")).contains("bob");
  }

  @Test
  void aRetriedDeleteIsAnsweredNotAppliedAgain() {
    put(ALICE, 1, 1, "k", "v");
    delete(BOB, 1, 1, "k");
    put(ALICE, 2, 2, "k", "again");
    assertThat(delete(BOB, 1, 1, "k").hasDelete()).isTrue();
    assertThat(get("k")).contains("again");
  }

  @Test
  void aLateCopyOfAFinishedWriteIsRejected() {
    put(ALICE, 1, 1, "k", "old");
    // Seq 2 says ALICE has her answer for everything below 2.
    put(ALICE, 2, 2, "k", "new");
    assertThat(errorOf(put(ALICE, 1, 1, "k", "old")).hasStaleRequest()).isTrue();
    assertThat(get("k")).contains("new");
  }

  @Test
  void resultsTheClientNoLongerNeedsAreDeleted() {
    for (int seq = 1; seq <= 10; seq++) {
      put(ALICE, seq, seq, "k" + seq, "v");
    }
    assertThat(storedResults(ALICE)).containsExactly(10L);
    // Writes in flight together all stay until the client moves past them.
    put(BOB, 1, 1, "a", "v");
    put(BOB, 2, 1, "b", "v");
    put(BOB, 3, 1, "c", "v");
    assertThat(storedResults(BOB)).containsExactly(1L, 2L, 3L);
    put(BOB, 4, 3, "d", "v");
    assertThat(storedResults(BOB)).containsExactly(3L, 4L);
  }

  @Test
  void theFloorNeverGoesDown() {
    put(ALICE, 5, 5, "k", "five");
    put(ALICE, 6, 1, "k", "six"); // an out-of-date floor in a reordered request
    assertThat(errorOf(put(ALICE, 4, 4, "k", "four")).hasStaleRequest()).isTrue();
    assertThat(get("k")).contains("six");
  }

  @Test
  void aRetryAfterACrashAndRestartIsStillRecognized() {
    KvResponse first = put(ALICE, 1, 1, "k", "alice");
    put(BOB, 1, 1, "k", "bob");
    // Power fails before ALICE's answer reaches her. Writes are synced before they are answered,
    // and the result was written in the same batch as the data, so both survive.
    service.close();
    fs.crash();
    open();
    assertThat(put(ALICE, 1, 1, "k", "alice")).isEqualTo(first);
    assertThat(get("k")).contains("bob");
  }

  @Test
  void idleSessionsExpireAndActiveOnesStay() {
    put(ALICE, 1, 1, "a", "v");
    scheduler.runFor(Duration.ofMinutes(30));
    put(BOB, 1, 1, "b", "v");
    assertThat(service.sessionCount()).isEqualTo(2);

    scheduler.runFor(Duration.ofMinutes(31)); // ALICE idle 61 min, BOB 31 min
    assertThat(service.sessionCount()).isEqualTo(1);
    assertThat(storedResults(ALICE)).isEmpty();
    assertThat(storedResults(BOB)).containsExactly(1L);

    scheduler.runFor(Duration.ofMinutes(30));
    assertThat(service.sessionCount()).isZero();
    // Only bookkeeping is forgotten, never data.
    assertThat(get("a")).contains("v");
    assertThat(get("b")).contains("v");
  }

  // ---------------------------------------------------------------------------------------------
  // Rejections

  @Test
  void malformedRequestsAreRejectedAsInvalid() {
    KvRequest put = putRequest(ALICE, 1, 1, "k", "v");
    assertInvalid(put.toBuilder().clearRequestId().build());
    assertInvalid(
        put.toBuilder()
            .setRequestId(
                put.getRequestId().toBuilder().setClientId(ByteString.copyFrom(id(1), 0, 8)))
            .build());
    assertInvalid(put.toBuilder().setRequestId(put.getRequestId().toBuilder().setSeq(0)).build());
    assertInvalid(putRequest(ALICE, 3, 4, "k", "v")); // waiting for 4 while sending 3
    int maxKey = KvServiceOptions.defaults().maxKeyBytes();
    int maxValue = KvServiceOptions.defaults().maxValueBytes();
    assertInvalid(putRequest(ALICE, 1, 1, "k".repeat(maxKey + 1), "v"));
    assertInvalid(putRequest(ALICE, 1, 1, "k", "v".repeat(maxValue + 1)));
    assertInvalid(KvRequest.getDefaultInstance());
    assertInvalid(
        KvRequest.newBuilder()
            .setGet(GetRequest.newBuilder().setKey(ByteString.copyFromUtf8("k".repeat(maxKey + 1))))
            .build());
    assertThat(get("k")).isEmpty();
  }

  @Test
  void workIsNotStartedOnceTheDeadlineHasPassed() {
    KvResponse response = handle(putRequest(ALICE, 1, 1, "k", "v"), clock.nanoTime());
    assertThat(errorOf(response).hasDeadlineExceeded()).isTrue();
    assertThat(get("k")).isEmpty();
    // The rejected attempt recorded nothing, so a retry with time left is applied normally.
    assertThat(put(ALICE, 1, 1, "k", "v").hasPut()).isTrue();
    assertThat(get("k")).contains("v");
  }

  @Test
  void requestsBeyondTheInflightLimitAreRefusedAsBusy() throws Exception {
    CountDownLatch inside = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    KvEngine blocking =
        new ForwardingEngine(engine) {
          @Override
          public Optional<byte[]> get(byte[] key) {
            inside.countDown();
            await(release);
            return super.get(key);
          }
        };
    service.close();
    service = newService(blocking, KvServiceOptions.defaults().withMaxInflightRequests(1));
    Thread slow = Thread.ofPlatform().start(() -> get("k"));
    assertThat(inside.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(errorOf(handle(getRequest(utf8("k")), KvHandler.NO_DEADLINE)).hasServerBusy())
        .isTrue();
    release.countDown();
    slow.join();
    assertThat(handle(getRequest(utf8("k")), KvHandler.NO_DEADLINE).hasGet()).isTrue();
  }

  @Test
  void aStorageFailureStopsTheServiceAndIsReportedOnce() {
    AtomicInteger writes = new AtomicInteger();
    KvEngine failing =
        new ForwardingEngine(engine) {
          @Override
          public void write(WriteBatch batch, boolean sync) {
            writes.incrementAndGet();
            throw new StorageException("disk on fire");
          }
        };
    service.close();
    service = newService(failing, KvServiceOptions.defaults());
    // The write that hit the failure may have reached the disk; the ones refused after it did not.
    Error first = errorOf(put(ALICE, 1, 1, "k", "v"));
    assertThat(first.hasStorageFailure()).isTrue();
    assertThat(first.getStorageFailure().getMaybeApplied()).isTrue();
    Error second = errorOf(put(ALICE, 2, 2, "k", "v"));
    assertThat(second.hasStorageFailure()).isTrue();
    assertThat(second.getStorageFailure().getMaybeApplied()).isFalse();
    Error read = errorOf(handle(getRequest(utf8("k")), KvHandler.NO_DEADLINE));
    assertThat(read.getStorageFailure().getMaybeApplied()).isFalse();
    assertThat(writes).hasValue(1);
    assertThat(storageFailures).hasSize(1);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers

  private KvResponse handle(KvRequest request, long deadline) {
    return service.handle(request, deadline).join();
  }

  private KvResponse put(byte[] client, long seq, long floor, String key, String value) {
    return handle(putRequest(client, seq, floor, key, value), KvHandler.NO_DEADLINE);
  }

  private KvResponse delete(byte[] client, long seq, long floor, String key) {
    return handle(
        KvRequest.newBuilder()
            .setRequestId(requestId(client, seq))
            .setFirstIncompleteSeq(floor)
            .setDelete(DeleteRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)))
            .build(),
        KvHandler.NO_DEADLINE);
  }

  private Optional<String> get(String key) {
    return get(utf8(key)).map(v -> new String(v, StandardCharsets.UTF_8));
  }

  private Optional<byte[]> get(byte[] key) {
    KvResponse r = handle(getRequest(key), KvHandler.NO_DEADLINE);
    assertThat(r.hasGet()).as("response %s", r).isTrue();
    return r.getGet().getFound()
        ? Optional.of(r.getGet().getValue().toByteArray())
        : Optional.empty();
  }

  private KvResponse scan(byte[] start, byte[] end, int limit) {
    ScanRequest.Builder scan =
        ScanRequest.newBuilder().setStartKey(ByteString.copyFrom(start)).setLimit(limit);
    if (end != null) {
      scan.setEndKey(ByteString.copyFrom(end));
    }
    KvResponse r = handle(KvRequest.newBuilder().setScan(scan).build(), KvHandler.NO_DEADLINE);
    assertThat(r.hasScan()).as("response %s", r).isTrue();
    return r;
  }

  private void assertInvalid(KvRequest request) {
    assertThat(errorOf(handle(request, KvHandler.NO_DEADLINE)).hasInvalidArgument())
        .as("request %s", request)
        .isTrue();
  }

  /** The seqs whose results are stored for {@code client}. */
  private List<Long> storedResults(byte[] client) {
    List<Long> seqs = new ArrayList<>();
    byte[] prefix = KeySpace.local(KeySpace.RESULT, client);
    byte[] end = Arrays.copyOf(prefix, prefix.length + 9);
    Arrays.fill(end, prefix.length, end.length, (byte) 0xFF);
    try (KvIterator it = engine.scan(prefix, end)) {
      while (it.hasNext()) {
        seqs.add(KeySpace.readUint64(it.next().key(), prefix.length));
      }
    }
    return seqs;
  }

  private static Error errorOf(KvResponse response) {
    assertThat(response.hasError()).as("response %s", response).isTrue();
    return response.getError();
  }

  private static KvRequest putRequest(
      byte[] client, long seq, long floor, String key, String value) {
    return KvRequest.newBuilder()
        .setRequestId(requestId(client, seq))
        .setFirstIncompleteSeq(floor)
        .setPut(
            PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value)))
        .build();
  }

  private static KvRequest getRequest(byte[] key) {
    return KvRequest.newBuilder()
        .setGet(GetRequest.newBuilder().setKey(ByteString.copyFrom(key)))
        .build();
  }

  private static RequestId requestId(byte[] client, long seq) {
    return RequestId.newBuilder().setClientId(ByteString.copyFrom(client)).setSeq(seq).build();
  }

  private static List<String> keys(ScanResponse page) {
    return page.getEntriesList().stream()
        .map(KeyValue::getKey)
        .map(ByteString::toStringUtf8)
        .toList();
  }

  private static byte[] id(int n) {
    byte[] id = new byte[16];
    id[15] = (byte) n;
    return id;
  }

  private static String key(int i) {
    return String.format("key%05d", i);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Passes everything to another engine; tests override what they want to break. */
  private static class ForwardingEngine implements KvEngine {
    private final KvEngine delegate;

    ForwardingEngine(KvEngine delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(WriteBatch batch, boolean sync) {
      delegate.write(batch, sync);
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
}

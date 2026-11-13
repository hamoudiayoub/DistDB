package io.distdb.client;

import com.google.protobuf.ByteString;
import io.distdb.env.Clock;
import io.distdb.env.RandomSource;
import io.distdb.env.Scheduler;
import io.distdb.rpc.KvChannel;
import io.distdb.rpc.PdClient;
import io.distdb.rpc.RpcException;
import io.distdb.rpc.proto.ConditionalPutRequest;
import io.distdb.rpc.proto.DeleteRequest;
import io.distdb.rpc.proto.Error;
import io.distdb.rpc.proto.GetRequest;
import io.distdb.rpc.proto.GetResponse;
import io.distdb.rpc.proto.KeyValue;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.LocateRequest;
import io.distdb.rpc.proto.PdRequest;
import io.distdb.rpc.proto.PutRequest;
import io.distdb.rpc.proto.RangeHeader;
import io.distdb.rpc.proto.RequestId;
import io.distdb.rpc.proto.ScanRequest;
import io.distdb.rpc.proto.ScanResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A client of a KV group. Every call has a deadline; within it, the client retries whatever is
 * worth retrying, with exponential backoff and jitter, and gives up with a {@link KvException}.
 *
 * <p>It sends to the node it believes leads. A {@code NotLeader} answer names the real leader, if
 * the node knows it, and the client goes straight there; a node that does not answer, or knows no
 * leader, makes the client try the next of its seed addresses.
 *
 * <p>Retrying a write is safe because each write carries a request id (this client's random id and
 * a sequence number) that stays the same across its retries, so the server applies it at most once.
 * A failed write is reported with {@link KvException#outcomeUnknown()} if any attempt may have
 * reached the disk: one that went unanswered after it may have been sent, or one the server says
 * failed while writing.
 *
 * <p>Thread-safe and non-blocking: calls return futures, and backoff waits are timers on the {@link
 * Scheduler}, so the simulator can run the client deterministically. {@link #close()} closes the
 * channels it opened; a channel passed in directly stays the caller's.
 */
public final class KvClient implements AutoCloseable {

  private static final int CLIENT_ID_BYTES = 16;

  private final List<String> seeds;
  private final Function<String, KvChannel> connect;
  private final boolean ownsChannels;
  private final Map<String, KvChannel> channels = new ConcurrentHashMap<>();
  private volatile String target;
  private final Clock clock;
  private final Scheduler scheduler;
  private final RandomSource random; // guarded by itself
  private final ClientOptions options;
  private final ByteString clientId;
  private final PdClient pd; // null: no range routing, send to the target and follow redirects
  private final RangeCache cache = new RangeCache();

  private final Object seqLock = new Object();
  private long nextSeq = 1; // guarded by seqLock
  private final TreeSet<Long> incomplete = new TreeSet<>(); // guarded by seqLock

  /** A client of a single node, over a channel the caller owns. */
  public KvClient(
      KvChannel channel,
      Clock clock,
      Scheduler scheduler,
      RandomSource random,
      ClientOptions options) {
    this(List.of("node"), address -> channel, false, null, clock, scheduler, random, options);
  }

  /**
   * A client of a group.
   *
   * @param seeds addresses to start from; any member will do, it redirects to the leader
   * @param connect opens a channel to an address (seeds, and leaders named in redirects)
   */
  public KvClient(
      List<String> seeds,
      Function<String, KvChannel> connect,
      Clock clock,
      Scheduler scheduler,
      RandomSource random,
      ClientOptions options) {
    this(seeds, connect, true, null, clock, scheduler, random, options);
  }

  /**
   * A client of a sharded cluster: it asks the placement driver where each key lives, caches the
   * answer, and sends each request to the leader of the range holding its key.
   *
   * @param pd the placement driver
   * @param connect opens a channel to a node's address
   */
  public KvClient(
      PdClient pd,
      Function<String, KvChannel> connect,
      Clock clock,
      Scheduler scheduler,
      RandomSource random,
      ClientOptions options) {
    this(
        List.of("pd"),
        connect,
        true,
        Objects.requireNonNull(pd),
        clock,
        scheduler,
        random,
        options);
  }

  private KvClient(
      List<String> seeds,
      Function<String, KvChannel> connect,
      boolean ownsChannels,
      PdClient pd,
      Clock clock,
      Scheduler scheduler,
      RandomSource random,
      ClientOptions options) {
    if (seeds.isEmpty()) {
      throw new IllegalArgumentException("at least one seed address is needed");
    }
    this.seeds = List.copyOf(seeds);
    this.connect = Objects.requireNonNull(connect);
    this.ownsChannels = ownsChannels;
    this.pd = pd;
    this.target = this.seeds.getFirst();
    this.clock = Objects.requireNonNull(clock);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.random = Objects.requireNonNull(random);
    this.options = Objects.requireNonNull(options);
    byte[] id = new byte[CLIENT_ID_BYTES];
    random.nextBytes(id);
    this.clientId = ByteString.copyFrom(id);
  }

  /** This client's id, which the server uses to recognize its retries. */
  public byte[] clientId() {
    return clientId.toByteArray();
  }

  /** The address the client currently believes leads (where the next request goes). */
  public String leader() {
    return target;
  }

  /** Closes the channels this client opened. */
  @Override
  public void close() {
    if (ownsChannels) {
      channels.values().forEach(KvChannel::close);
    }
    channels.clear();
  }

  private KvChannel channel(String address) {
    return channels.computeIfAbsent(address, connect);
  }

  /** After {@code failed} did not help, move on to the next seed (unless someone already did). */
  private void moveOn(String failed) {
    int i = seeds.indexOf(failed);
    String next = seeds.get((i + 1) % seeds.size());
    synchronized (this) {
      if (target.equals(failed)) {
        target = next;
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // The API

  /** The value of {@code key}, or empty if it has none. */
  public CompletableFuture<Optional<byte[]>> get(byte[] key) {
    return get(key, options.defaultTimeout());
  }

  public CompletableFuture<Optional<byte[]>> get(byte[] key, Duration timeout) {
    KvRequest request =
        KvRequest.newBuilder().setGet(GetRequest.newBuilder().setKey(bytes(key))).build();
    return read(request, deadline(timeout))
        .thenApply(response -> {
          GetResponse get = expect(response, KvResponse.ResultCase.GET).getGet();
          return get.getFound() ? Optional.of(get.getValue().toByteArray()) : Optional.empty();
        });
  }

  /** Sets {@code key} to {@code value}. When the future succeeds, the write is durable. */
  public CompletableFuture<Void> put(byte[] key, byte[] value) {
    return put(key, value, options.defaultTimeout());
  }

  public CompletableFuture<Void> put(byte[] key, byte[] value, Duration timeout) {
    KvRequest.Builder request =
        KvRequest.newBuilder()
            .setPut(PutRequest.newBuilder().setKey(bytes(key)).setValue(bytes(value)));
    return write(request, deadline(timeout))
        .thenApply(response -> expectVoid(response, KvResponse.ResultCase.PUT));
  }

  /**
   * Sets {@code key} to {@code value} only if its current value is {@code expected} (empty: only if
   * it has none). Completes with whether it did.
   */
  public CompletableFuture<Boolean> conditionalPut(
      byte[] key, byte[] value, Optional<byte[]> expected, Duration timeout) {
    ConditionalPutRequest.Builder cput =
        ConditionalPutRequest.newBuilder().setKey(bytes(key)).setValue(bytes(value));
    expected.ifPresent(e -> cput.setExpected(bytes(e)));
    return write(KvRequest.newBuilder().setCput(cput), deadline(timeout))
        .thenApply(
            response -> expect(response, KvResponse.ResultCase.CPUT).getCput().getSucceeded());
  }

  /** Removes {@code key}; succeeds whether or not it existed. */
  public CompletableFuture<Void> delete(byte[] key) {
    return delete(key, options.defaultTimeout());
  }

  public CompletableFuture<Void> delete(byte[] key, Duration timeout) {
    KvRequest.Builder request =
        KvRequest.newBuilder().setDelete(DeleteRequest.newBuilder().setKey(bytes(key)));
    return write(request, deadline(timeout))
        .thenApply(response -> expectVoid(response, KvResponse.ResultCase.DELETE));
  }

  /**
   * Up to {@code limit} entries of {@code [start, end)} in key order; a null {@code end} means no
   * upper bound. Fetched page by page, all within one deadline. Each page is a consistent snapshot,
   * but the pages together are not one: a key written during the scan may or may not appear.
   */
  public CompletableFuture<List<Entry>> scan(byte[] start, byte[] end, int limit) {
    return scan(start, end, limit, options.defaultTimeout());
  }

  public CompletableFuture<List<Entry>> scan(
      byte[] start, byte[] end, int limit, Duration timeout) {
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be at least 1: " + limit);
    }
    return scanFrom(start, end, limit, deadline(timeout), new ArrayList<>());
  }

  /** Waits for {@code future} and returns its value, rethrowing a failure as its KvException. */
  public static <T> T await(CompletableFuture<T> future) {
    try {
      return future.join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof RuntimeException cause) {
        throw cause;
      }
      throw e;
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Internals

  private CompletableFuture<List<Entry>> scanFrom(
      byte[] start, byte[] end, int limit, long deadline, List<Entry> out) {
    ScanRequest.Builder scan =
        ScanRequest.newBuilder().setStartKey(bytes(start)).setLimit(limit - out.size());
    if (end != null) {
      scan.setEndKey(bytes(end));
    }
    return read(KvRequest.newBuilder().setScan(scan).build(), deadline)
        .thenCompose(response -> {
          ScanResponse page = expect(response, KvResponse.ResultCase.SCAN).getScan();
          for (KeyValue kv : page.getEntriesList()) {
            if (out.size() == limit) {
              break;
            }
            out.add(new Entry(kv.getKey().toByteArray(), kv.getValue().toByteArray()));
          }
          if (out.size() == limit || !page.hasResumeKey()) {
            return CompletableFuture.completedFuture(out);
          }
          return scanFrom(page.getResumeKey().toByteArray(), end, limit, deadline, out);
        });
  }

  private CompletableFuture<KvResponse> read(KvRequest request, long deadline) {
    return new Call(request, false, deadline).start();
  }

  private CompletableFuture<KvResponse> write(KvRequest.Builder request, long deadline) {
    long seq;
    synchronized (seqLock) {
      // Taking the number and marking it incomplete must be one step. Otherwise a concurrent
      // write could compute a floor above this seq, and the server would reject it as stale.
      seq = nextSeq++;
      incomplete.add(seq);
    }
    request.setRequestId(RequestId.newBuilder().setClientId(clientId).setSeq(seq));
    return new Call(request.build(), true, deadline)
        .start()
        .whenComplete((response, failure) -> {
          // Answered or abandoned: this client will never send seq again either way.
          synchronized (seqLock) {
            incomplete.remove(seq);
          }
        });
  }

  /** The lowest seq still waiting for an answer: everything below it is finished. */
  private long floor() {
    synchronized (seqLock) {
      return incomplete.isEmpty() ? nextSeq : incomplete.first();
    }
  }

  private long deadline(Duration timeout) {
    if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(options.maxTimeout()) > 0) {
      throw new IllegalArgumentException(
          "timeout must be positive and at most " + options.maxTimeout() + ": " + timeout);
    }
    return clock.nanoTime() + timeout.toNanos();
  }

  /** One logical call: attempts, separated by backoff, until success or the deadline. */
  private final class Call {
    private final KvRequest request;
    private final boolean write;
    private final long deadline;
    private final CompletableFuture<KvResponse> result = new CompletableFuture<>();

    // Touched by one attempt's callback at a time; each hands over to the next through a future or
    // a timer, which orders the accesses.
    private int attempts;
    private int redirects;
    private boolean outcomeUnknown;
    private String lastFailure = "none";
    private String sentTo;

    Call(KvRequest request, boolean write, long deadline) {
      this.request = request;
      this.write = write;
      this.deadline = deadline;
    }

    CompletableFuture<KvResponse> start() {
      attempt();
      return result;
    }

    private void attempt() {
      long remaining = deadline - clock.nanoTime();
      if (remaining <= 0) {
        giveUp();
        return;
      }
      attempts++;
      if (pd == null) {
        send(target, null);
        return;
      }
      byte[] key = routingKey(request);
      RangeCache.Route cached = cache.lookup(key);
      if (cached != null) {
        send(cache.address(cached), cached);
        return;
      }
      // Not cached: ask the placement driver where the key lives.
      PdRequest locate =
          PdRequest.newBuilder()
              .setLocate(LocateRequest.newBuilder().setKey(ByteString.copyFrom(key)))
              .build();
      Duration timeout = Duration.ofNanos(Math.min(remaining, options.attemptTimeout().toNanos()));
      pd.call(locate, timeout)
          .whenComplete((response, failure) -> {
            if (response != null && response.hasLocate() && response.getLocate().getFound()) {
              RangeCache.Route route = cache.put(response.getLocate());
              send(cache.address(route), route);
            } else {
              lastFailure =
                  "cannot locate the key: "
                      + (failure != null ? failure.getMessage() : "the PD knows no range for it");
              retryLater();
            }
          });
    }

    /** Sends one attempt to {@code address}; {@code route} is the range it is meant for, if any. */
    private void send(String address, RangeCache.Route route) {
      long remaining = deadline - clock.nanoTime();
      if (remaining <= 0) {
        giveUp();
        return;
      }
      if (address == null) {
        cache.invalidate(route); // no known replica address: ask the PD again
        lastFailure = "no address for any replica of r" + route.desc.getRangeId();
        retryLater();
        return;
      }
      // The request id stays the same on every attempt; only the floor may have moved up.
      KvRequest.Builder r = request.toBuilder();
      if (write) {
        r.setFirstIncompleteSeq(floor());
      }
      ByteString cutAt = null;
      if (route != null) {
        r.setHeader(
            RangeHeader.newBuilder()
                .setRangeId(route.desc.getRangeId())
                .setVersion(route.desc.getVersion()));
        if (request.hasScan() && route.desc.hasEndKey()) {
          ByteString end = route.desc.getEndKey();
          if (!request.getScan().hasEndKey()
              || ByteString.unsignedLexicographicalComparator()
                      .compare(request.getScan().getEndKey(), end)
                  > 0) {
            r.setScan(request.getScan().toBuilder().setEndKey(end)); // one range at a time
            cutAt = end;
          }
        }
      }
      Duration timeout = Duration.ofNanos(Math.min(remaining, options.attemptTimeout().toNanos()));
      CompletableFuture<KvResponse> answer;
      sentTo = address;
      try {
        answer = channel(sentTo).call(r.build(), timeout);
      } catch (RuntimeException e) {
        answer = CompletableFuture.failedFuture(e);
      }
      ByteString resumeAt = cutAt;
      answer.whenComplete((response, failure) -> onAnswer(route, resumeAt, response, failure));
    }

    private void onAnswer(
        RangeCache.Route route, ByteString cutAt, KvResponse response, Throwable failure) {
      if (failure != null) {
        Throwable cause =
            failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause()
                : failure;
        // No answer: unless the channel knows nothing was sent, the request may have arrived and
        // been applied.
        boolean maybeDelivered = !(cause instanceof RpcException e) || e.maybeDelivered();
        outcomeUnknown |= write && maybeDelivered;
        lastFailure = String.valueOf(cause.getMessage());
        if (cause instanceof RpcException e && e.kind().retryable()) {
          if (route != null) {
            cache.rotate(route); // that node may be down; another replica may lead now
          } else {
            moveOn(sentTo);
          }
          retryLater();
        } else {
          fail(KvException.Code.INTERNAL, "call failed: " + cause, cause);
        }
        return;
      }
      if (!response.hasError()) {
        if (cutAt != null && response.hasScan() && !response.getScan().hasResumeKey()) {
          // The page reached the end of this range: the scan goes on in the next one.
          response =
              response.toBuilder()
                  .setScan(response.getScan().toBuilder().setResumeKey(cutAt))
                  .build();
        }
        result.complete(response);
        return;
      }
      // A typed error: the server decided, and (unless it says otherwise) this attempt changed
      // nothing.
      Error error = response.getError();
      lastFailure = error.getKindCase() + ": " + error.getMessage();
      switch (error.getKindCase()) {
        case SERVER_BUSY, DEADLINE_EXCEEDED -> retryLater();
        case STORAGE_FAILURE -> {
          outcomeUnknown |= write && error.getStorageFailure().getMaybeApplied();
          retryLater(); // the node restarts, and a retry is recognized if the write survived
        }
        case NOT_LEADER -> {
          // Proposed by a leader that then lost the leadership: it may still commit.
          outcomeUnknown |= write && error.getNotLeader().getMaybeApplied();
          String hint = error.getNotLeader().getLeaderAddress();
          if (!hint.isEmpty() && !hint.equals(sentTo) && redirects < seeds.size() + 4) {
            redirects++;
            if (route != null) {
              cache.setLeader(route, hint);
              send(hint, route); // straight to the leader it named
            } else {
              synchronized (KvClient.this) {
                target = hint;
              }
              send(hint, null);
            }
          } else {
            // It knows no leader (an election is on): try elsewhere, later.
            if (route != null) {
              cache.rotate(route);
            } else {
              moveOn(sentTo);
            }
            retryLater();
          }
        }
        case RANGE_NOT_FOUND, EPOCH_MISMATCH -> {
          // The range is not there, or has split since it was cached: look it up again.
          if (route != null) {
            cache.invalidate(route);
          }
          if (route != null && redirects < 8) {
            redirects++;
            attempt();
          } else {
            retryLater();
          }
        }
        case INVALID_ARGUMENT -> fail(KvException.Code.INVALID_ARGUMENT, error.getMessage(), null);
        case STALE_REQUEST, KIND_NOT_SET ->
            fail(KvException.Code.INTERNAL, "unexpected error from server: " + lastFailure, null);
      }
    }

    private void retryLater() {
      // Exponential backoff with "full jitter": wait a random time up to a ceiling that doubles
      // with each retry, so clients that failed together do not retry together.
      long ceiling = options.initialBackoff().toNanos() << Math.min(attempts - 1, 20);
      ceiling = Math.min(ceiling, options.maxBackoff().toNanos());
      long delay;
      synchronized (random) {
        delay = random.nextLong(0, ceiling + 1);
      }
      if (clock.nanoTime() + delay >= deadline) {
        giveUp();
        return;
      }
      scheduler.schedule(Duration.ofNanos(delay), this::attempt);
    }

    private void giveUp() {
      fail(
          KvException.Code.DEADLINE_EXCEEDED,
          "deadline exceeded after "
              + attempts
              + " attempt(s); last failure: "
              + lastFailure
              + (outcomeUnknown ? "; the write may or may not have been applied" : ""),
          null);
    }

    private void fail(KvException.Code code, String message, Throwable cause) {
      result.completeExceptionally(new KvException(code, outcomeUnknown, message, cause));
    }
  }

  /** The key that decides which range a request goes to. */
  private static byte[] routingKey(KvRequest request) {
    return switch (request.getOpCase()) {
      case GET -> request.getGet().getKey().toByteArray();
      case PUT -> request.getPut().getKey().toByteArray();
      case DELETE -> request.getDelete().getKey().toByteArray();
      case CPUT -> request.getCput().getKey().toByteArray();
      case SCAN -> request.getScan().getStartKey().toByteArray();
      default -> throw new IllegalArgumentException("no key in " + request.getOpCase());
    };
  }

  private static KvResponse expect(KvResponse response, KvResponse.ResultCase expected) {
    if (response.getResultCase() != expected) {
      throw new KvException(
          KvException.Code.INTERNAL,
          false,
          "expected a " + expected + " response, got " + response.getResultCase(),
          null);
    }
    return response;
  }

  private static Void expectVoid(KvResponse response, KvResponse.ResultCase expected) {
    expect(response, expected);
    return null;
  }

  private static ByteString bytes(byte[] b) {
    return ByteString.copyFrom(b);
  }
}

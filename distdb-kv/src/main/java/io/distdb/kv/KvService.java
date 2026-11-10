package io.distdb.kv;

import com.google.protobuf.ByteString;
import io.distdb.common.Bytes;
import io.distdb.env.Clock;
import io.distdb.env.Scheduler;
import io.distdb.rpc.KvErrors;
import io.distdb.rpc.KvHandler;
import io.distdb.rpc.proto.DeleteResponse;
import io.distdb.rpc.proto.GetRequest;
import io.distdb.rpc.proto.GetResponse;
import io.distdb.rpc.proto.KeyValue;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.PutResponse;
import io.distdb.rpc.proto.RequestId;
import io.distdb.rpc.proto.ScanRequest;
import io.distdb.rpc.proto.ScanResponse;
import io.distdb.storage.KvEngine;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import io.distdb.storage.StorageException;
import io.distdb.storage.WriteBatch;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * A single node's KV service: answers Get, Put, Delete and Scan from one {@link KvEngine}.
 *
 * <p>What it guarantees:
 *
 * <ul>
 *   <li>An acknowledged write is durable: every write is synced before it is answered.
 *   <li>A write is applied at most once, however often it is retried, even across a crash and
 *       restart ({@link ClientSessions}).
 *   <li>Work whose deadline has already passed is not started, and a long scan stops at it.
 *   <li>Overload is refused early, with {@code ServerBusy}, instead of queueing without bound.
 *   <li>After a storage error it serves nothing more and reports the failure once ({@code
 *       onStorageFailure}), so the process can exit and recover on restart (crash-only design).
 * </ul>
 *
 * <p>Thread-safe. Reads run concurrently; writes are serialized under one lock, because looking up
 * a write's earlier result and recording a new one must be atomic. The service does not own the
 * engine: close the service first, then the engine.
 */
public final class KvService implements KvHandler, AutoCloseable {

  private static final System.Logger LOG = System.getLogger(KvService.class.getName());

  private final KvEngine engine;
  private final Clock clock;
  private final Scheduler scheduler;
  private final KvServiceOptions options;
  private final Consumer<Throwable> onStorageFailure;
  private final ClientSessions sessions;

  private final ReentrantLock writeLock = new ReentrantLock();
  private final AtomicInteger inflight = new AtomicInteger();
  private volatile Throwable storageFailure;

  // Guarded by this.
  private boolean closed;
  private Scheduler.Cancellable sweepTimer;

  public KvService(
      KvEngine engine,
      Clock clock,
      Scheduler scheduler,
      KvServiceOptions options,
      Consumer<Throwable> onStorageFailure) {
    this.engine = Objects.requireNonNull(engine);
    this.clock = Objects.requireNonNull(clock);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.options = Objects.requireNonNull(options);
    this.onStorageFailure = Objects.requireNonNull(onStorageFailure);
    this.sessions = new ClientSessions(engine);
    scheduleSweep();
  }

  @Override
  public CompletableFuture<KvResponse> handle(KvRequest request, long deadlineNanos) {
    try {
      return CompletableFuture.completedFuture(handleNow(request, deadlineNanos));
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private KvResponse handleNow(KvRequest request, long deadlineNanos) {
    Throwable failure = storageFailure;
    if (failure != null) {
      return KvErrors.storageFailure(false, "storage engine failed, node must restart: " + failure);
    }
    if (inflight.incrementAndGet() > options.maxInflightRequests()) {
      inflight.decrementAndGet();
      return KvErrors.serverBusy(
          "more than " + options.maxInflightRequests() + " requests in flight");
    }
    try {
      if (clock.nanoTime() >= deadlineNanos) {
        return KvErrors.deadlineExceeded("deadline passed before the request was handled");
      }
      return switch (request.getOpCase()) {
        case GET -> get(request.getGet());
        case PUT, DELETE -> write(request);
        case SCAN -> scan(request.getScan(), deadlineNanos);
        case OP_NOT_SET -> KvErrors.invalidArgument("request has no operation");
      };
    } catch (InvalidRequest e) {
      return KvErrors.invalidArgument(e.getMessage());
    } catch (StorageException e) {
      failStop(e);
      return KvErrors.storageFailure(false, "storage engine failed, node must restart: " + e);
    } finally {
      inflight.decrementAndGet();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Reads

  private KvResponse get(GetRequest request) {
    byte[] key = key(request.getKey(), options.maxKeyBytes());
    Optional<byte[]> value = engine.get(KeySpace.user(key));
    GetResponse.Builder response = GetResponse.newBuilder().setFound(value.isPresent());
    value.ifPresent(v -> response.setValue(ByteString.copyFrom(v)));
    return KvResponse.newBuilder().setGet(response).build();
  }

  private KvResponse scan(ScanRequest request, long deadlineNanos) {
    // Bounds may be one byte longer than a stored key: a resume key is the last key returned
    // plus 0x00, and the last key may be as long as allowed.
    byte[] start = key(request.getStartKey(), options.maxKeyBytes() + 1);
    byte[] end = request.hasEndKey() ? key(request.getEndKey(), options.maxKeyBytes() + 1) : null;
    int limit = request.getLimit(); // uint32: "negative" means above 2^31
    if (limit == 0) {
      limit = options.defaultScanLimit();
    } else if (limit < 0 || limit > options.maxScanLimit()) {
      limit = options.maxScanLimit();
    }
    ScanResponse.Builder page = ScanResponse.newBuilder();
    if (end != null && Bytes.compare(start, end) >= 0) {
      return KvResponse.newBuilder().setScan(page).build();
    }
    try (KvIterator it = engine.scan(KeySpace.user(start), KeySpace.userEnd(end))) {
      long bytes = 0;
      int count = 0;
      byte[] last = null;
      while (it.hasNext()) {
        if (count == limit || bytes >= options.scanPageBytes()) {
          // Stopped early: the next page starts just after the last key returned.
          page.setResumeKey(ByteString.copyFrom(Arrays.copyOf(last, last.length + 1)));
          break;
        }
        if ((count & 0xFF) == 0xFF && clock.nanoTime() >= deadlineNanos) {
          return KvErrors.deadlineExceeded("deadline passed during the scan");
        }
        KvEntry e = it.next();
        last = KeySpace.fromUser(e.key());
        page.addEntries(
            KeyValue.newBuilder()
                .setKey(ByteString.copyFrom(last))
                .setValue(ByteString.copyFrom(e.value())));
        bytes += last.length + e.value().length;
        count++;
      }
    }
    return KvResponse.newBuilder().setScan(page).build();
  }

  // -----------------------------------------------------------------------------------------------
  // Writes

  private KvResponse write(KvRequest request) {
    if (!request.hasRequestId()) {
      throw new InvalidRequest("writes must carry a request id, so retries are safe");
    }
    RequestId id = request.getRequestId();
    byte[] clientId = id.getClientId().toByteArray();
    long seq = id.getSeq();
    long firstIncomplete = request.getFirstIncompleteSeq();
    if (clientId.length != ClientSessions.CLIENT_ID_BYTES) {
      throw new InvalidRequest("client id must be " + ClientSessions.CLIENT_ID_BYTES + " bytes");
    }
    if (seq < 1 || firstIncomplete < 0 || firstIncomplete > seq) {
      throw new InvalidRequest(
          "bad request id: seq " + seq + ", first incomplete " + firstIncomplete);
    }
    byte[] key;
    byte[] value = null;
    if (request.hasPut()) {
      key = key(request.getPut().getKey(), options.maxKeyBytes());
      value = request.getPut().getValue().toByteArray();
      if (value.length > options.maxValueBytes()) {
        throw new InvalidRequest(
            "value is " + value.length + " bytes; the limit is " + options.maxValueBytes());
      }
    } else {
      key = key(request.getDelete().getKey(), options.maxKeyBytes());
    }

    writeLock.lock();
    try {
      ClientSessions.Session session = sessions.load(clientId);
      if (seq < session.floor()) {
        return KvErrors.staleRequest(
            "write " + seq + " is below its client's floor " + session.floor());
      }
      Optional<KvResponse> earlier = sessions.result(clientId, seq);
      if (earlier.isPresent()) {
        return earlier.get(); // A retry: answer it, never apply it twice.
      }
      WriteBatch batch = new WriteBatch();
      KvResponse response;
      if (value != null) {
        batch.put(KeySpace.user(key), value);
        response = KvResponse.newBuilder().setPut(PutResponse.getDefaultInstance()).build();
      } else {
        batch.delete(KeySpace.user(key));
        response = KvResponse.newBuilder().setDelete(DeleteResponse.getDefaultInstance()).build();
      }
      // The write and the record of its answer land in one atomic, synced batch.
      sessions.record(
          batch, clientId, session, seq, firstIncomplete, response, clock.wallTimeMillis());
      try {
        engine.write(batch, true);
      } catch (StorageException e) {
        // The batch may be on disk even though the write failed (the append worked but the fsync
        // did not, say), and then it reappears after the restart. Say so: the client must not
        // report this write as certainly not applied.
        failStop(e);
        return KvErrors.storageFailure(true, "storage failed while writing, may be applied: " + e);
      }
      return response;
    } finally {
      writeLock.unlock();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Sessions, failure, shutdown

  /** How many client sessions are stored. */
  public int sessionCount() {
    writeLock.lock();
    try {
      return sessions.count();
    } finally {
      writeLock.unlock();
    }
  }

  private synchronized void scheduleSweep() {
    if (!closed) {
      sweepTimer = scheduler.schedule(options.sessionSweepInterval(), this::sweep);
    }
  }

  private void sweep() {
    if (storageFailure != null) {
      return;
    }
    writeLock.lock();
    try {
      int expired = sessions.expire(clock.wallTimeMillis(), options.sessionTtl());
      if (expired > 0) {
        LOG.log(System.Logger.Level.DEBUG, "expired {0} idle client sessions", expired);
      }
    } catch (StorageException e) {
      failStop(e);
      return;
    } finally {
      writeLock.unlock();
    }
    scheduleSweep();
  }

  private void failStop(StorageException e) {
    synchronized (this) {
      if (storageFailure != null) {
        return;
      }
      storageFailure = e;
    }
    LOG.log(System.Logger.Level.ERROR, "storage engine failed; refusing all requests", e);
    onStorageFailure.accept(e);
  }

  /** Stops the session sweeper. Requests that arrive afterwards are still served. */
  @Override
  public synchronized void close() {
    closed = true;
    if (sweepTimer != null) {
      sweepTimer.cancel();
    }
  }

  private static byte[] key(ByteString key, int maxBytes) {
    if (key.size() > maxBytes) {
      throw new InvalidRequest("key is " + key.size() + " bytes; the limit is " + maxBytes);
    }
    return key.toByteArray();
  }

  /** A request that can never succeed as sent. */
  private static final class InvalidRequest extends RuntimeException {
    private static final long serialVersionUID = 1L;

    InvalidRequest(String message) {
      super(message, null, false, false);
    }
  }
}

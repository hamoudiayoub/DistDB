package io.distdb.kv;

import com.google.protobuf.ByteString;
import io.distdb.rpc.KvErrors;
import io.distdb.rpc.proto.ConditionalPutResponse;
import io.distdb.rpc.proto.DeleteResponse;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.PutResponse;
import io.distdb.rpc.proto.WriteCommand;
import io.distdb.storage.KvEngine;
import io.distdb.storage.WriteBatch;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Applies one client write from the log, the same way on every replica: the dedup rules of {@link
 * ClientSessions}, then the write itself, then its result, all into one batch.
 */
final class KvWrites {

  private KvWrites() {}

  /**
   * What applying a write did.
   *
   * @param effect whether it changed client data
   */
  record Result(KvStateMachine.RequestKey request, KvResponse response, boolean effect) {}

  /**
   * Applies {@code w} into {@code batch}.
   *
   * @param owns whether a client key belongs to this state machine now. A write for a key it no
   *     longer owns (the range split after the write was proposed) is answered {@code
   *     EpochMismatch} and changes nothing, so the client retries where the key is now.
   */
  static Result apply(
      WriteBatch batch,
      KvEngine engine,
      ClientSessions sessions,
      WriteCommand w,
      KvServiceOptions options,
      Predicate<byte[]> owns) {
    KvRequest r = w.getRequest();
    KvStateMachine.RequestKey key = KvStateMachine.RequestKey.of(r);
    try {
      KvRequests.validateWrite(r, options);
    } catch (KvRequests.InvalidRequest bad) {
      // The leader validates before proposing; every replica still reaches the same answer.
      return new Result(key, KvErrors.invalidArgument(bad.getMessage()), false);
    }
    byte[] clientId = r.getRequestId().getClientId().toByteArray();
    long seq = r.getRequestId().getSeq();
    ClientSessions.Session session = sessions.load(clientId);
    if (seq < session.floor()) {
      return new Result(
          key,
          KvErrors.staleRequest("write " + seq + " is below its client's floor " + session.floor()),
          false);
    }
    Optional<KvResponse> earlier = sessions.result(clientId, seq);
    if (earlier.isPresent()) {
      return new Result(key, earlier.get(), false); // a retry: never applied twice
    }
    byte[] userKey = KvRequests.writeKey(r);
    if (!owns.test(userKey)) {
      return new Result(
          key, KvErrors.epochMismatch("the key moved to another range before this write"), false);
    }
    KvResponse response;
    boolean effect = true;
    switch (r.getOpCase()) {
      case PUT -> {
        batch.put(KeySpace.user(userKey), r.getPut().getValue().toByteArray());
        response = KvResponse.newBuilder().setPut(PutResponse.getDefaultInstance()).build();
      }
      case DELETE -> {
        batch.delete(KeySpace.user(userKey));
        response = KvResponse.newBuilder().setDelete(DeleteResponse.getDefaultInstance()).build();
      }
      case CPUT -> {
        Optional<byte[]> actual = engine.get(KeySpace.user(userKey));
        Optional<byte[]> expected =
            r.getCput().hasExpected()
                ? Optional.of(r.getCput().getExpected().toByteArray())
                : Optional.empty();
        boolean matches =
            actual.isPresent() == expected.isPresent()
                && (actual.isEmpty() || Arrays.equals(actual.get(), expected.get()));
        ConditionalPutResponse.Builder cput = ConditionalPutResponse.newBuilder();
        if (matches) {
          batch.put(KeySpace.user(userKey), r.getCput().getValue().toByteArray());
          cput.setSucceeded(true);
        } else {
          actual.ifPresent(v -> cput.setActual(ByteString.copyFrom(v)));
          effect = false;
        }
        response = KvResponse.newBuilder().setCput(cput).build();
      }
      default -> throw new IllegalStateException("not a write: " + r.getOpCase());
    }
    sessions.record(
        batch,
        clientId,
        session,
        seq,
        r.getFirstIncompleteSeq(),
        response,
        w.getProposeTimeMillis());
    return new Result(key, response, effect);
  }
}

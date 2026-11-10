package io.distdb.rpc;

import io.distdb.rpc.proto.DeadlineExceeded;
import io.distdb.rpc.proto.Error;
import io.distdb.rpc.proto.InvalidArgument;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.ServerBusy;
import io.distdb.rpc.proto.StaleRequest;
import io.distdb.rpc.proto.StorageFailure;

/** Builds responses that carry a typed {@link Error}. */
public final class KvErrors {

  private KvErrors() {}

  public static KvResponse serverBusy(String message) {
    return of(Error.newBuilder().setServerBusy(ServerBusy.getDefaultInstance()), message);
  }

  public static KvResponse deadlineExceeded(String message) {
    return of(
        Error.newBuilder().setDeadlineExceeded(DeadlineExceeded.getDefaultInstance()), message);
  }

  public static KvResponse invalidArgument(String message) {
    return of(Error.newBuilder().setInvalidArgument(InvalidArgument.getDefaultInstance()), message);
  }

  /**
   * @param maybeApplied whether the failure struck while this request's write was going to disk
   */
  public static KvResponse storageFailure(boolean maybeApplied, String message) {
    return of(
        Error.newBuilder()
            .setStorageFailure(StorageFailure.newBuilder().setMaybeApplied(maybeApplied)),
        message);
  }

  public static KvResponse staleRequest(String message) {
    return of(Error.newBuilder().setStaleRequest(StaleRequest.getDefaultInstance()), message);
  }

  private static KvResponse of(Error.Builder error, String message) {
    return KvResponse.newBuilder().setError(error.setMessage(message)).build();
  }
}

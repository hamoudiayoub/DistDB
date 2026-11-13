package io.distdb.kv;

import com.google.protobuf.ByteString;
import io.distdb.rpc.proto.KvRequest;
import io.distdb.rpc.proto.RequestId;

/** Checks that a request can succeed at all, before any work is done for it. */
final class KvRequests {

  private KvRequests() {}

  /** A request that can never succeed as sent; answered with {@code InvalidArgument}. */
  static final class InvalidRequest extends RuntimeException {
    private static final long serialVersionUID = 1L;

    InvalidRequest(String message) {
      super(message, null, false, false);
    }
  }

  /** Validates a Get or Scan: keys within limits (scan bounds may be one byte longer). */
  static void validateRead(KvRequest request, KvServiceOptions options) {
    switch (request.getOpCase()) {
      case GET -> key(request.getGet().getKey(), options.maxKeyBytes());
      case SCAN -> {
        key(request.getScan().getStartKey(), options.maxKeyBytes() + 1);
        if (request.getScan().hasEndKey()) {
          key(request.getScan().getEndKey(), options.maxKeyBytes() + 1);
        }
      }
      default -> throw new InvalidRequest("not a read: " + request.getOpCase());
    }
  }

  /**
   * Validates a Put, Delete or ConditionalPut: a well-formed request id, and key and value within
   * limits.
   */
  static void validateWrite(KvRequest request, KvServiceOptions options) {
    if (!request.hasRequestId()) {
      throw new InvalidRequest("writes must carry a request id, so retries are safe");
    }
    RequestId id = request.getRequestId();
    long seq = id.getSeq();
    long firstIncomplete = request.getFirstIncompleteSeq();
    if (id.getClientId().size() != ClientSessions.CLIENT_ID_BYTES) {
      throw new InvalidRequest("client id must be " + ClientSessions.CLIENT_ID_BYTES + " bytes");
    }
    if (seq < 1 || firstIncomplete < 0 || firstIncomplete > seq) {
      throw new InvalidRequest(
          "bad request id: seq " + seq + ", first incomplete " + firstIncomplete);
    }
    switch (request.getOpCase()) {
      case PUT -> {
        key(request.getPut().getKey(), options.maxKeyBytes());
        int size = request.getPut().getValue().size();
        if (size > options.maxValueBytes()) {
          throw new InvalidRequest(
              "value is " + size + " bytes; the limit is " + options.maxValueBytes());
        }
      }
      case DELETE -> key(request.getDelete().getKey(), options.maxKeyBytes());
      case CPUT -> {
        key(request.getCput().getKey(), options.maxKeyBytes());
        int size = request.getCput().getValue().size();
        if (size > options.maxValueBytes()) {
          throw new InvalidRequest(
              "value is " + size + " bytes; the limit is " + options.maxValueBytes());
        }
      }
      default -> throw new InvalidRequest("not a write: " + request.getOpCase());
    }
  }

  /** Whether {@code request} is a write (it then needs a request id). */
  static boolean isWrite(KvRequest request) {
    return request.hasPut() || request.hasDelete() || request.hasCput();
  }

  /** The client key a write changes. */
  static byte[] writeKey(KvRequest request) {
    return switch (request.getOpCase()) {
      case PUT -> request.getPut().getKey().toByteArray();
      case DELETE -> request.getDelete().getKey().toByteArray();
      case CPUT -> request.getCput().getKey().toByteArray();
      default -> throw new IllegalArgumentException("not a write: " + request.getOpCase());
    };
  }

  static byte[] key(ByteString key, int maxBytes) {
    if (key.size() > maxBytes) {
      throw new InvalidRequest("key is " + key.size() + " bytes; the limit is " + maxBytes);
    }
    return key.toByteArray();
  }
}

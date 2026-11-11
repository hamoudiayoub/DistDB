package io.distdb.kv;

import com.google.protobuf.ByteString;
import io.distdb.common.Bytes;
import io.distdb.env.Clock;
import io.distdb.rpc.KvErrors;
import io.distdb.rpc.proto.GetRequest;
import io.distdb.rpc.proto.GetResponse;
import io.distdb.rpc.proto.KeyValue;
import io.distdb.rpc.proto.KvResponse;
import io.distdb.rpc.proto.ScanRequest;
import io.distdb.rpc.proto.ScanResponse;
import io.distdb.storage.KvEngine;
import io.distdb.storage.KvEntry;
import io.distdb.storage.KvIterator;
import java.util.Arrays;
import java.util.Optional;

/** Executes Get and Scan against the engine's client data (requests already validated). */
final class KvReads {

  private KvReads() {}

  static KvResponse get(KvEngine engine, GetRequest request) {
    Optional<byte[]> value = engine.get(KeySpace.user(request.getKey().toByteArray()));
    GetResponse.Builder response = GetResponse.newBuilder().setFound(value.isPresent());
    value.ifPresent(v -> response.setValue(ByteString.copyFrom(v)));
    return KvResponse.newBuilder().setGet(response).build();
  }

  /**
   * One page of a scan: at most {@code limit} entries (server default if 0) or about {@code
   * scanPageBytes}, but always at least one entry, so a scan always makes progress. A page that
   * stops early ends with {@code resume_key = last key + 0x00}, the smallest key after it.
   */
  static KvResponse scan(
      KvEngine engine,
      ScanRequest request,
      KvServiceOptions options,
      Clock clock,
      long deadlineNanos) {
    byte[] start = request.getStartKey().toByteArray();
    byte[] end = request.hasEndKey() ? request.getEndKey().toByteArray() : null;
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
}

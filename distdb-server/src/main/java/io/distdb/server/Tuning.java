package io.distdb.server;

import io.distdb.raft.RaftConfig;
import io.distdb.storage.lsm.EngineOptions;
import io.distdb.txn.TxnClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * {@code --tune raft.batch=off,raft.pipeline=off,tso.batch=off,wal.group-commit=off}: turns
 * optimizations off, one by one, so that a benchmark can measure what each one buys. Everything is
 * on by default.
 *
 * @param raftBatch many entries per AppendEntries (off: one)
 * @param raftPipeline many AppendEntries in flight per follower (off: one, then wait)
 * @param tsoBatch concurrent transactions share one timestamp request (off: one request each)
 * @param groupCommit concurrent synced writes share one fsync (off: one fsync each)
 */
record Tuning(boolean raftBatch, boolean raftPipeline, boolean tsoBatch, boolean groupCommit) {

  static final Tuning DEFAULT = new Tuning(true, true, true, true);

  static Tuning parse(String spec) throws Args.UsageException {
    if (spec == null || spec.isBlank()) {
      return DEFAULT;
    }
    boolean batch = true;
    boolean pipeline = true;
    boolean tso = true;
    boolean group = true;
    for (String item : spec.split(",")) {
      String[] kv = item.trim().split("=", 2);
      if (kv.length != 2) {
        throw new Args.UsageException("--tune wants KEY=on|off, got " + item);
      }
      boolean on =
          switch (kv[1].trim().toLowerCase(Locale.ROOT)) {
            case "on", "true" -> true;
            case "off", "false" -> false;
            default -> throw new Args.UsageException("--tune " + kv[0] + " wants on or off");
          };
      switch (kv[0].trim()) {
        case "raft.batch" -> batch = on;
        case "raft.pipeline" -> pipeline = on;
        case "tso.batch" -> tso = on;
        case "wal.group-commit" -> group = on;
        default ->
            throw new Args.UsageException(
                "unknown --tune key "
                    + kv[0]
                    + " (raft.batch, raft.pipeline, tso.batch, wal.group-commit)");
      }
    }
    return new Tuning(batch, pipeline, tso, group);
  }

  RaftConfig raft(RaftConfig c) {
    return new RaftConfig(
        c.id(),
        c.electionTicks(),
        c.heartbeatTicks(),
        c.preVote(),
        c.checkQuorum(),
        raftBatch ? c.maxEntriesPerAppend() : 1,
        raftPipeline ? c.maxInflightAppends() : 1);
  }

  EngineOptions engine(EngineOptions o) {
    return o.withGroupCommit(groupCommit);
  }

  TxnClient.Options txn(TxnClient.Options o) {
    return o.withBatchTimestamps(tsoBatch);
  }

  /** "all on", or what is off. */
  String describe() {
    List<String> off = new ArrayList<>();
    if (!raftBatch) {
      off.add("raft.batch");
    }
    if (!raftPipeline) {
      off.add("raft.pipeline");
    }
    if (!tsoBatch) {
      off.add("tso.batch");
    }
    if (!groupCommit) {
      off.add("wal.group-commit");
    }
    return off.isEmpty() ? "all optimizations on" : "off: " + String.join(", ", off);
  }
}

package io.distdb.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.common.NodeId;
import io.distdb.env.RandomSource;
import io.distdb.env.SplittableRandomSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class RaftCodecTest {

  private final RandomSource r = SplittableRandomSource.seeded(11);

  @Test
  void everyMessageSurvivesARoundTrip() {
    for (int i = 0; i < 5_000; i++) {
      Message m = randomMessage();
      assertThat(RaftCodec.decode(RaftCodec.encode(m))).isEqualTo(m);
    }
  }

  @Test
  void storedStateSurvivesARoundTrip() {
    for (int i = 0; i < 1_000; i++) {
      Entry e = entry(r.nextLong(1, 1_000_000));
      assertThat(RaftCodec.decodeEntry(RaftCodec.encodeEntry(e))).isEqualTo(e);
      HardState h =
          new HardState(r.nextLong(0, 100), r.nextBoolean() ? null : node(), r.nextInt(50));
      assertThat(RaftCodec.decodeHardState(RaftCodec.encodeHardState(h))).isEqualTo(h);
      Snapshot s = snapshot();
      assertThat(RaftCodec.decodeSnapshot(RaftCodec.encodeSnapshot(s))).isEqualTo(s);
    }
  }

  @Test
  void aTruncatedOrPaddedMessageIsRejectedNotMisread() {
    for (int i = 0; i < 1_000; i++) {
      byte[] bytes = RaftCodec.encode(randomMessage());
      int cut = r.nextInt(bytes.length);
      assertThatThrownBy(() -> RaftCodec.decode(Arrays.copyOf(bytes, cut)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> RaftCodec.decode(Arrays.copyOf(bytes, bytes.length + 1)))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  private Message randomMessage() {
    NodeId from = node();
    NodeId to = node();
    long term = r.nextLong(0, 1_000);
    return switch (r.nextInt(7)) {
      case 0 ->
          new Message.RequestVote(from, to, term, r.nextInt(500), r.nextInt(50), r.nextBoolean());
      case 1 -> new Message.VoteResponse(from, to, term, r.nextBoolean(), r.nextBoolean());
      case 2 -> {
        long prev = r.nextLong(0, 1_000);
        List<Entry> es = new ArrayList<>();
        int n = r.nextInt(5);
        for (int i = 0; i < n; i++) {
          es.add(entry(prev + 1 + i));
        }
        yield new Message.AppendEntries(from, to, term, prev, r.nextInt(50), es, r.nextInt(900));
      }
      case 3 ->
          new Message.AppendResponse(
              from, to, term, r.nextBoolean(), r.nextInt(900), r.nextInt(900), r.nextInt(50));
      case 4 -> new Message.Heartbeat(from, to, term, r.nextInt(900), r.nextLong(0, 1L << 40));
      case 5 -> new Message.HeartbeatResponse(from, to, term, r.nextLong(0, 1L << 40));
      default -> new Message.InstallSnapshot(from, to, term, snapshot());
    };
  }

  private Entry entry(long index) {
    byte[] data = new byte[r.nextInt(40)];
    r.nextBytes(data);
    Entry.Type type = r.nextInt(5) == 0 ? Entry.Type.CONFIG : Entry.Type.NORMAL;
    return new Entry(index, r.nextLong(1, 100), type, data);
  }

  private Snapshot snapshot() {
    byte[] data = new byte[r.nextInt(100)];
    r.nextBytes(data);
    List<NodeId> voters = new ArrayList<>();
    List<NodeId> learners = new ArrayList<>();
    for (int i = 1; i <= 7; i++) {
      switch (r.nextInt(3)) {
        case 0 -> voters.add(new NodeId(i));
        case 1 -> learners.add(new NodeId(i));
        default -> {}
      }
    }
    return new Snapshot(
        r.nextLong(0, 10_000), r.nextLong(0, 100), new Membership(voters, learners), data);
  }

  private NodeId node() {
    return new NodeId(r.nextInt(1, 10));
  }
}

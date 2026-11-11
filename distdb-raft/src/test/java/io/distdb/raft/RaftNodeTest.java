package io.distdb.raft;

import static io.distdb.raft.TestCluster.utf8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.distdb.common.NodeId;
import io.distdb.env.SplittableRandomSource;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RaftNodeTest {

  private static final RaftConfig PLAIN =
      RaftConfig.defaults(new NodeId(1)).withPreVoteAndCheckQuorum(false, false);

  // ---------------------------------------------------------------------------------------------
  // Election and replication

  @Test
  void aSingleNodeElectsItselfAndCommits() {
    TestCluster c = TestCluster.of(1);
    c.elect(1);
    assertThat(c.leaders()).containsExactly(c.id(1));
    assertThat(c.node(1).propose(utf8("a"))).isTrue();
    c.process();
    assertThat(c.applied(1)).containsExactly("a");
  }

  @Test
  void threeNodesElectOneLeaderAndReplicateInOrder() {
    TestCluster c = TestCluster.of(3);
    c.elect(1);
    assertThat(c.leaders()).containsExactly(c.id(1));
    for (String s : List.of("a", "b", "c")) {
      assertThat(c.node(1).propose(utf8(s))).isTrue();
    }
    assertThat(c.node(2).propose(utf8("x"))).as("followers refuse proposals").isFalse();
    c.deliverAll();
    c.tickAll(3); // heartbeats carry the final commit index to the followers
    for (int i = 1; i <= 3; i++) {
      assertThat(c.applied(i)).containsExactly("a", "b", "c");
    }
  }

  @Test
  void aCandidateMissingCommittedEntriesCannotWin() {
    TestCluster c = TestCluster.of(3, PLAIN);
    c.elect(1);
    c.drop = m -> m.to().equals(c.id(3)); // n3 misses the next entry
    c.node(1).propose(utf8("a"));
    c.deliverAll();
    assertThat(c.node(1).status().commit()).isEqualTo(c.node(2).status().lastIndex());
    c.drop = m -> false;
    c.crash(1);

    c.elect(3); // n3's log lacks "a", which is committed: n2 must refuse
    assertThat(c.leaders()).isEmpty();
    c.elect(2);
    assertThat(c.leaders()).containsExactly(c.id(2));
    c.tickAll(3);
    assertThat(c.applied(3)).containsExactly("a");
  }

  @Test
  void conflictingEntriesAreRepairedATermAtATime() {
    // n1 and n2: 1..5 in term 1, 6..10 in term 3. n3: 1..5 in term 1, then 55 entries in term 2
    // that were never committed. One entry per round trip would need 55 tries.
    List<Entry> good = new ArrayList<>(entries(1, 5, 1));
    good.addAll(entries(6, 10, 3));
    List<Entry> bad = new ArrayList<>(entries(1, 5, 1));
    bad.addAll(entries(6, 60, 2));
    TestCluster c = new TestCluster(RaftConfig.defaults(new NodeId(1)));
    Membership m = Membership.ofVoters(new NodeId(1), new NodeId(2), new NodeId(3));
    Snapshot base = Snapshot.initial(m);
    c.add(c.id(1), restart(c, 1, new PersistedState(new HardState(3, null, 5), base, good)));
    c.add(c.id(2), restart(c, 2, new PersistedState(new HardState(3, null, 5), base, good)));
    c.add(c.id(3), restart(c, 3, new PersistedState(new HardState(2, null, 5), base, bad)));
    c.stores.get(c.id(3)).save(new Ready(null, bad, null, false, List.of(), List.of(), List.of()));

    c.elect(1);
    assertThat(c.leaders()).containsExactly(c.id(1));
    c.tickAll(2);
    long appendsToN3 =
        c.sent.stream()
            .filter(x -> x instanceof Message.AppendEntries && x.to().equals(c.id(3)))
            .count();
    assertThat(appendsToN3).isLessThanOrEqualTo(4);
    RaftNode leader = c.node(1);
    RaftNode n3 = c.node(3);
    assertThat(n3.status().lastIndex()).isEqualTo(leader.status().lastIndex());
    assertThat(n3.entries(1, n3.status().lastIndex()))
        .isEqualTo(leader.entries(1, leader.status().lastIndex()));
    assertThat(c.stores.get(c.id(3)).lastIndex())
        .as("the stale suffix was deleted from n3's store too")
        .isEqualTo(leader.status().lastIndex());
  }

  @Test
  void aLeaderCommitsOnlyEntriesOfItsOwnTermByCounting() {
    // Figure 8: entry 2 (term 2) is on n1 and n2, a majority, but must not be committed by
    // counting replicas while the leader is in term 3; only an entry of term 3 can commit it.
    Membership m = Membership.ofVoters(new NodeId(1), new NodeId(2), new NodeId(3));
    Snapshot base = Snapshot.initial(m);
    List<Entry> twoTerms = List.of(entry(1, 1), entry(2, 2));
    TestCluster c = new TestCluster(RaftConfig.defaults(new NodeId(1)));
    c.add(c.id(1), restart(c, 1, new PersistedState(new HardState(2, null, 1), base, twoTerms)));
    c.add(c.id(2), restart(c, 2, new PersistedState(new HardState(2, null, 1), base, twoTerms)));
    c.add(
        c.id(3),
        restart(c, 3, new PersistedState(new HardState(2, null, 1), base, List.of(entry(1, 1)))));
    c.drop = x -> x instanceof Message.AppendEntries || x instanceof Message.Heartbeat;
    c.elect(1);
    RaftNode leader = c.node(1);
    assertThat(leader.status().role()).isEqualTo(RaftNode.Role.LEADER);
    long term = leader.status().term();
    assertThat(leader.status().lastIndex()).isEqualTo(3); // its no-op, in term 3

    leader.step(new Message.AppendResponse(c.id(2), c.id(1), term, true, 2, 0, 0));
    assertThat(leader.status().commit())
        .as("index 2 is on a majority but from term 2")
        .isEqualTo(1);

    leader.step(new Message.AppendResponse(c.id(2), c.id(1), term, true, 3, 0, 0));
    assertThat(leader.status().commit()).isEqualTo(3);
  }

  @Test
  void aFollowerCommitsOnlyWhatTheLeaderHasJustVerified() {
    // The follower holds 1:t1, 2:t1 and a stale 3:t2 from an old leader. The new leader (term 3)
    // has committed its own, different entry 3, and sends entry 2 alone. Entry 2 matches, but 3
    // was never checked: the follower may commit up to 2, not up to the leader's commit index.
    Membership m = Membership.ofVoters(new NodeId(1), new NodeId(2), new NodeId(3));
    TestCluster c = new TestCluster(RaftConfig.defaults(new NodeId(1)));
    RaftNode follower =
        restart(
            c,
            2,
            new PersistedState(
                new HardState(2, null, 1),
                Snapshot.initial(m),
                List.of(entry(1, 1), entry(2, 1), entry(3, 2))));
    follower.step(new Message.AppendEntries(c.id(1), c.id(2), 3, 1, 1, List.of(entry(2, 1)), 3));
    assertThat(follower.status().commit()).isEqualTo(2);
  }

  // ---------------------------------------------------------------------------------------------
  // Pre-vote and check-quorum

  @Test
  void withPreVoteARejoiningNodeDoesNotDisruptTheLeader() {
    TestCluster c = TestCluster.of(3);
    c.elect(1);
    long term = c.node(1).status().term();
    c.drop = m -> m.to().equals(c.id(3)) || m.from().equals(c.id(3)); // n3 is cut off
    c.tickAll(200);
    assertThat(c.node(3).status().term()).as("pre-votes never raise the term").isEqualTo(term);
    c.drop = m -> false;
    c.tickAll(50);
    assertThat(c.leaders()).containsExactly(c.id(1));
    assertThat(c.node(1).status().term()).isEqualTo(term);
  }

  @Test
  void withoutPreVoteTheSameRejoinDeposesTheLeader() {
    TestCluster c = TestCluster.of(3, PLAIN);
    c.elect(1);
    long term = c.node(1).status().term();
    c.drop = m -> m.to().equals(c.id(3)) || m.from().equals(c.id(3));
    c.tickAll(200);
    assertThat(c.node(3).status().term()).isGreaterThan(term + 5);
    c.drop = m -> false;
    c.tickAll(50);
    assertThat(c.node(1).status().term()).as("the old leader was forced out").isGreaterThan(term);
  }

  @Test
  void checkQuorumMakesACutOffLeaderStepDown() {
    TestCluster c = TestCluster.of(3);
    c.elect(1);
    c.drop = m -> m.to().equals(c.id(1)) || m.from().equals(c.id(1));
    for (int i = 0; i < 25; i++) {
      c.node(1).tick();
      c.deliverAll();
    }
    assertThat(c.node(1).status().role()).isEqualTo(RaftNode.Role.FOLLOWER);
  }

  // ---------------------------------------------------------------------------------------------
  // Persistence and snapshots

  @Test
  void aRestartedNodeRemembersItsVote() {
    TestCluster c = TestCluster.of(3, PLAIN);
    c.drop = m -> m.to().equals(c.id(3)) || m.from().equals(c.id(3));
    c.node(1).tick(); // n1 eventually campaigns; only n2 hears it
    for (int i = 0; i < 30 && c.leaders().isEmpty(); i++) {
      c.node(1).tick();
      c.deliverAll();
    }
    assertThat(c.leaders()).containsExactly(c.id(1));
    long term = c.node(2).status().term();
    c.crash(2);
    c.restart(2);
    c.node(2).step(new Message.RequestVote(c.id(3), c.id(2), term, 1_000, term, false));
    c.process();
    Message answer = c.queue.peekLast();
    assertThat(answer).isInstanceOf(Message.VoteResponse.class);
    assertThat(((Message.VoteResponse) answer).granted())
        .as("it already voted for n1 in this term, before the restart")
        .isFalse();
  }

  @Test
  void aCrashedNodeComesBackWithEverythingItAcknowledged() {
    TestCluster c = TestCluster.of(3);
    c.elect(1);
    for (int i = 0; i < 20; i++) {
      c.node(1).propose(utf8("v" + i));
    }
    c.tickAll(3);
    c.crash(2);
    c.restart(2);
    c.tickAll(3);
    assertThat(c.applied(2)).isEqualTo(c.applied(1)).hasSize(20);
  }

  @Test
  void aLaggingFollowerCatchesUpFromASnapshot() {
    TestCluster c = TestCluster.of(3);
    c.elect(1);
    c.crash(3);
    for (int i = 0; i < 50; i++) {
      c.node(1).propose(utf8("v" + i));
    }
    c.tickAll(3);
    c.compact(1);
    c.compact(2);
    assertThat(c.node(1).status().snapshotIndex()).isGreaterThan(40);
    c.restart(3);
    c.tickAll(5);
    assertThat(c.sent)
        .anyMatch(m -> m instanceof Message.InstallSnapshot && m.to().equals(c.id(3)));
    c.node(1).propose(utf8("after"));
    c.tickAll(3);
    assertThat(c.applied(3)).isEqualTo(c.applied(1)).endsWith("v49", "after").hasSize(51);
  }

  // ---------------------------------------------------------------------------------------------
  // Membership

  @Test
  void aNewLeaderMayNotChangeMembershipBeforeCommittingInItsTerm() {
    TestCluster c = TestCluster.of(3);
    c.drop = m -> m instanceof Message.AppendEntries; // its no-op cannot commit
    c.elect(1);
    ConfigChange add = new ConfigChange(ConfigChange.Kind.ADD_LEARNER, new NodeId(4));
    assertThat(c.node(1).proposeConfigChange(add)).isFalse();
    c.drop = m -> false;
    c.tickAll(2);
    assertThat(c.node(1).proposeConfigChange(add)).isTrue();
  }

  @Test
  void addALearnerPromoteItThenRemoveTheLeader() {
    TestCluster c = TestCluster.of(3);
    c.elect(1);
    c.tickAll(2);
    c.addEmpty(4);
    for (int i = 0; i < 10; i++) {
      c.node(1).propose(utf8("v" + i));
    }
    RaftNode leader = c.node(1);
    assertThat(leader.proposeConfigChange(learner(4))).isTrue();
    assertThat(leader.proposeConfigChange(promote(4))).as("one change at a time").isFalse();
    c.tickAll(3);
    assertThat(c.applied(4)).as("a learner gets the log").hasSize(10);
    assertThat(c.node(4).status().membership().isLearner(c.id(4))).isTrue();
    assertThat(c.node(4).status().role()).isEqualTo(RaftNode.Role.FOLLOWER);

    assertThat(leader.proposeConfigChange(promote(4))).isTrue();
    c.tickAll(3);
    assertThat(leader.status().membership().voters()).hasSize(4);

    assertThat(leader.proposeConfigChange(new ConfigChange(ConfigChange.Kind.REMOVE, c.id(1))))
        .isTrue();
    c.tickAll(3);
    assertThat(leader.status().role())
        .as("it steps down once its removal commits")
        .isEqualTo(RaftNode.Role.FOLLOWER);
    c.tickAll(60);
    assertThat(c.leaders()).hasSize(1).doesNotContain(c.id(1));
    RaftNode newLeader = c.nodes.get(c.leaders().getFirst());
    newLeader.propose(utf8("after"));
    c.tickAll(3);
    assertThat(c.applied(4)).endsWith("after");
    assertThat(c.applied(1)).doesNotContain("after");
  }

  @Test
  void impossibleChangesAreRejected() {
    TestCluster c = TestCluster.of(1);
    c.elect(1);
    c.process();
    assertThatThrownBy(() -> c.node(1).proposeConfigChange(learner(1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() ->
            c.node(1).proposeConfigChange(new ConfigChange(ConfigChange.Kind.REMOVE, c.id(1))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------------------------
  // ReadIndex

  @Test
  void aReadIsReleasedOnlyAfterAMajorityConfirmsTheLeader() {
    TestCluster c = TestCluster.of(3);
    c.elect(1);
    c.tickAll(2);
    c.node(1).propose(utf8("a"));
    c.deliverAll();
    long commit = c.node(1).status().commit();
    assertThat(c.node(1).readIndex(utf8("r1"))).isTrue();
    c.process();
    assertThat(c.reads.get(c.id(1))).as("not before the heartbeat round").isEmpty();
    c.deliverAll();
    assertThat(c.reads.get(c.id(1))).hasSize(1);
    assertThat(c.reads.get(c.id(1)).getFirst().index()).isEqualTo(commit);
    assertThat(c.node(2).readIndex(utf8("r2"))).isFalse();
  }

  @Test
  void aLeaderThatLostItsMajorityCannotServeAReadIndexRead() {
    TestCluster c = TestCluster.of(3);
    c.elect(1);
    c.tickAll(2);
    c.drop = m -> m.from().equals(c.id(1)) || m.to().equals(c.id(1));
    assertThat(c.node(1).readIndex(utf8("stale"))).isTrue();
    for (int i = 0; i < 8; i++) {
      c.node(1).tick();
      c.deliverAll();
    }
    assertThat(c.reads.get(c.id(1))).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------
  // The log itself

  @Test
  void theLogRefusesToOverwriteACommittedEntry() {
    RaftLog log =
        new RaftLog(Snapshot.initial(Membership.EMPTY), List.of(entry(1, 1), entry(2, 1)), 2);
    assertThatThrownBy(() -> log.truncateAndAppend(List.of(entry(2, 2))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("committed");
  }

  // ---------------------------------------------------------------------------------------------

  private static RaftNode restart(TestCluster c, int i, PersistedState state) {
    c.stores.putIfAbsent(c.id(i), new TestCluster.Store());
    TestCluster.Store s = c.stores.get(c.id(i));
    s.hardState = state.hardState();
    s.snapshot = state.snapshot();
    state.entries().forEach(e -> s.entries.put(e.index(), e));
    return RaftNode.restart(c.config(c.id(i)), state, 0, SplittableRandomSource.seeded(i));
  }

  private static Entry entry(long index, long term) {
    return new Entry(index, term, Entry.Type.NORMAL, utf8("e" + index + "t" + term));
  }

  private static List<Entry> entries(long from, long to, long term) {
    List<Entry> out = new ArrayList<>();
    for (long i = from; i <= to; i++) {
      out.add(entry(i, term));
    }
    return out;
  }

  private static ConfigChange learner(int n) {
    return new ConfigChange(ConfigChange.Kind.ADD_LEARNER, new NodeId(n));
  }

  private static ConfigChange promote(int n) {
    return new ConfigChange(ConfigChange.Kind.PROMOTE, new NodeId(n));
  }
}

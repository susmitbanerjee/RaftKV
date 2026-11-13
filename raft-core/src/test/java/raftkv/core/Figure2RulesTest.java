package raftkv.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import raftkv.core.Input.Propose;
import raftkv.core.Input.Receive;
import raftkv.core.Input.Tick;
import raftkv.core.Message.AppendEntries;
import raftkv.core.Message.AppendEntriesResponse;
import raftkv.core.Message.RequestVote;
import raftkv.core.Message.RequestVoteResponse;
import raftkv.core.Output.RoleChange;
import raftkv.core.Payload.Command;
import raftkv.core.Payload.NoOp;

/** One test per rule of Raft Figure 2, on a 3-node cluster viewed from n0. */
class Figure2RulesTest {

    static final NodeId N0 = new NodeId(0);
    static final NodeId N1 = new NodeId(1);
    static final NodeId N2 = new NodeId(2);
    static final Set<NodeId> MEMBERS = Set.of(N0, N1, N2);
    /** Fixed election timeout of 5 ticks, heartbeat every 2. */
    static final Options OPTIONS = new Options(2, 5, 5, 64, Bug.NONE);

    static Node node(long term, Optional<NodeId> votedFor, LogEntry... log) {
        return new Node(N0, MEMBERS, OPTIONS, new SplittableRandom(1),
                new RestoredState(new HardState(term, votedFor), List.of(log)));
    }

    static Node node(long term, LogEntry... log) {
        return node(term, Optional.empty(), log);
    }

    static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, Command.ofUtf8("c" + index + "@" + term));
    }

    static Output ticks(Node node, int count) {
        Output last = null;
        for (int i = 0; i < count; i++) {
            last = node.step(new Tick());
        }
        return last;
    }

    static Output receive(Node node, NodeId from, Message message) {
        return node.step(new Receive(from, message));
    }

    static AppendEntries appendEntries(long term, long prevIndex, long prevTerm, long leaderCommit, LogEntry... entries) {
        return new AppendEntries(term, N1, prevIndex, prevTerm, List.of(entries), leaderCommit);
    }

    static <T extends Message> T only(Output output, NodeId to, Class<T> type) {
        List<T> matches = output.messages().stream()
                .filter(s -> s.to().equals(to) && type.isInstance(s.message()))
                .map(s -> type.cast(s.message())).toList();
        assertEquals(1, matches.size(), "expected exactly one " + type.getSimpleName() + " to " + to);
        return matches.get(0);
    }

    /** n0 becomes leader of term 1 (log: [1:NoOp@1]) after one granted vote. */
    static Node leader() {
        Node node = node(0);
        ticks(node, 5);
        receive(node, N1, new RequestVoteResponse(1, true));
        return node;
    }

    @Nested
    class AppendEntriesReceiver {
        @Test
        void rule1_repliesFalseIfTermIsStale() {
            Node node = node(2, entry(1, 1));
            Output out = receive(node, N1, appendEntries(1, 0, 0, 0, entry(1, 1)));
            AppendEntriesResponse reply = only(out, N1, AppendEntriesResponse.class);
            assertFalse(reply.success());
            assertEquals(2, reply.term());
            assertTrue(out.persist().isEmpty());
        }

        @Test
        void rule2_repliesFalseIfLogLacksMatchingPrevEntry() {
            Node node = node(1, entry(1, 1));
            assertFalse(only(receive(node, N1, appendEntries(1, 1, 2, 0)), N1, AppendEntriesResponse.class).success());
            assertFalse(only(receive(node, N1, appendEntries(1, 5, 1, 0)), N1, AppendEntriesResponse.class).success());
        }

        @Test
        void rule3_deletesConflictingEntryAndEverythingAfterIt() {
            Node node = node(1, entry(1, 1), entry(2, 1), entry(3, 1));
            Output out = receive(node, N1, appendEntries(2, 1, 1, 0, entry(2, 2)));
            assertEquals(OptionalLong.of(2), out.persist().truncateFrom());
            assertEquals(List.of(entry(2, 2)), out.persist().append());
            // entry 3 is gone: a follow-up entry 3 appends rather than being "already present"
            Output next = receive(node, N1, appendEntries(2, 2, 2, 0, entry(3, 2)));
            assertEquals(List.of(entry(3, 2)), next.persist().append());
            assertTrue(next.persist().truncateFrom().isEmpty());
        }

        @Test
        void rule4_appendsOnlyEntriesNotAlreadyInTheLog() {
            Node node = node(1, entry(1, 1));
            Output out = receive(node, N1, appendEntries(1, 0, 0, 0, entry(1, 1), entry(2, 1)));
            assertTrue(out.persist().truncateFrom().isEmpty());
            assertEquals(List.of(entry(2, 1)), out.persist().append());
            assertEquals(2, only(out, N1, AppendEntriesResponse.class).matchIndex());
        }

        @Test
        void rule5_commitIndexIsMinOfLeaderCommitAndIndexOfLastNewEntry() {
            Node node = node(1, entry(1, 1), entry(2, 1), entry(3, 1));
            // leaderCommit=5 but the last new entry is 1 (prevLogIndex 0 + 1 entry): apply only entry 1.
            Output out = receive(node, N1, appendEntries(1, 0, 0, 5, entry(1, 1)));
            assertEquals(List.of(entry(1, 1)), out.toApply());
            // leaderCommit=2 below the last new entry 3: apply only through 2.
            out = receive(node, N1, appendEntries(1, 2, 1, 2, entry(3, 1)));
            assertEquals(List.of(entry(2, 1)), out.toApply());
        }
    }

    @Nested
    class RequestVoteReceiver {
        @Test
        void rule1_deniesIfTermIsStale() {
            Node node = node(3);
            Output out = receive(node, N1, new RequestVote(2, N1, 0, 0));
            assertFalse(only(out, N1, RequestVoteResponse.class).voteGranted());
        }

        @Test
        void rule2_grantsWhenNotYetVotedAndCandidateIsUpToDate() {
            Node node = node(0);
            Output out = receive(node, N1, new RequestVote(1, N1, 0, 0));
            assertTrue(only(out, N1, RequestVoteResponse.class).voteGranted());
        }

        @Test
        void rule2_grantsAgainToTheSameCandidate() {
            Node node = node(1, Optional.of(N1));
            assertTrue(only(receive(node, N1, new RequestVote(1, N1, 0, 0)), N1, RequestVoteResponse.class).voteGranted());
        }

        @Test
        void rule2_deniesWhenAlreadyVotedForSomeoneElse() {
            Node node = node(1, Optional.of(N2));
            assertFalse(only(receive(node, N1, new RequestVote(1, N1, 0, 0)), N1, RequestVoteResponse.class).voteGranted());
        }

        @Test
        void rule2_deniesCandidateWithOlderLastTerm_electionRestriction() {
            Node node = node(2, entry(1, 1), entry(2, 2));
            RequestVote longButOld = new RequestVote(3, N1, 10, 1);
            assertFalse(only(receive(node, N1, longButOld), N1, RequestVoteResponse.class).voteGranted());
        }

        @Test
        void rule2_deniesCandidateWithSameLastTermButShorterLog_electionRestriction() {
            Node node = node(2, entry(1, 1), entry(2, 1));
            RequestVote shorter = new RequestVote(3, N1, 1, 1);
            assertFalse(only(receive(node, N1, shorter), N1, RequestVoteResponse.class).voteGranted());
        }

        @Test
        void persistentVoteIsEmittedInTheSameOutputAsTheGrant() {
            Output out = receive(node(0), N1, new RequestVote(1, N1, 0, 0));
            assertEquals(Optional.of(new HardState(1, Optional.of(N1))), out.persist().hardState());
        }
    }

    @Nested
    class AllServers {
        @Test
        void rule1_appliesCommittedEntriesInOrderExactlyOnce() {
            Node node = node(1);
            Output out = receive(node, N1, appendEntries(1, 0, 0, 2, entry(1, 1), entry(2, 1)));
            assertEquals(List.of(entry(1, 1), entry(2, 1)), out.toApply());
            assertTrue(receive(node, N1, appendEntries(1, 2, 1, 2)).toApply().isEmpty());
        }

        @Test
        void rule2_higherTermInAnyRpcMakesTheServerAFollowerInThatTerm() {
            Node node = leader();
            Output out = receive(node, N1, new AppendEntriesResponse(7, false, 0));
            assertEquals(List.of(new RoleChange(Role.FOLLOWER, 7)), out.roleChanges());
            assertEquals(Optional.of(new HardState(7, Optional.empty())), out.persist().hardState());
        }
    }

    @Nested
    class Followers {
        @Test
        void rule1_respondsToRpcsFromCandidatesAndLeaders() {
            Node node = node(0);
            only(receive(node, N1, new RequestVote(1, N1, 0, 0)), N1, RequestVoteResponse.class);
            only(receive(node, N2, new AppendEntries(1, N2, 0, 0, List.of(), 0)), N2, AppendEntriesResponse.class);
        }

        @Test
        void rule2_electionTimeoutWithoutHearingFromALeaderMakesItACandidate() {
            Node node = node(0);
            assertTrue(ticks(node, 4).roleChanges().isEmpty());
            assertEquals(List.of(new RoleChange(Role.CANDIDATE, 1)), node.step(new Tick()).roleChanges());
        }

        @Test
        void rule2_validAppendEntriesAndGrantedVotesResetTheTimer() {
            Node node = node(0);
            ticks(node, 4);
            receive(node, N1, appendEntries(1, 0, 0, 0));
            assertTrue(ticks(node, 4).roleChanges().isEmpty());
            receive(node, N2, new RequestVote(2, N2, 0, 0));
            assertTrue(ticks(node, 4).roleChanges().isEmpty());
        }
    }

    @Nested
    class Candidates {
        @Test
        void rule1_onConversionIncrementsTermVotesForSelfAndRequestsVotes() {
            Node node = node(4, entry(1, 2), entry(2, 4));
            Output out = ticks(node, 5);
            assertEquals(Optional.of(new HardState(5, Optional.of(N0))), out.persist().hardState());
            RequestVote expected = new RequestVote(5, N0, 2, 4);
            assertEquals(expected, only(out, N1, RequestVote.class));
            assertEquals(expected, only(out, N2, RequestVote.class));
        }

        @Test
        void rule1_resetsTheElectionTimer() {
            Node node = node(0);
            ticks(node, 5);
            assertTrue(ticks(node, 4).messages().isEmpty());
            assertEquals(2, node.step(new Tick()).persist().hardState().orElseThrow().term());
        }

        @Test
        void rule2_majorityOfVotesMakesItLeader() {
            Node node = node(0);
            ticks(node, 5);
            assertTrue(receive(node, N1, new RequestVoteResponse(1, false)).roleChanges().isEmpty());
            assertEquals(List.of(new RoleChange(Role.LEADER, 1)),
                    receive(node, N2, new RequestVoteResponse(1, true)).roleChanges());
        }

        @Test
        void rule3_appendEntriesFromANewLeaderMakesItAFollowerButAStaleOneDoesNot() {
            Node node = node(0);
            ticks(node, 5); // candidate in term 1
            assertTrue(receive(node, N1, appendEntries(0, 0, 0, 0)).roleChanges().isEmpty());
            assertEquals(List.of(new RoleChange(Role.FOLLOWER, 1)),
                    receive(node, N1, appendEntries(1, 0, 0, 0)).roleChanges());
        }

        @Test
        void rule4_timeoutStartsANewElectionInANewTerm() {
            Node node = node(0);
            ticks(node, 5);
            Output out = ticks(node, 5);
            assertEquals(2, out.persist().hardState().orElseThrow().term());
            assertEquals(2, only(out, N1, RequestVote.class).term());
        }
    }

    @Nested
    class Leaders {
        @Test
        void rule1_onElectionSendsInitialAppendEntriesWithANoOpThenHeartbeats() {
            Node node = node(0);
            ticks(node, 5);
            Output out = receive(node, N1, new RequestVoteResponse(1, true));
            for (NodeId peer : List.of(N1, N2)) {
                AppendEntries first = only(out, peer, AppendEntries.class);
                assertEquals(List.of(new LogEntry(1, 1, new NoOp())), first.entries());
            }
            assertTrue(node.step(new Tick()).messages().isEmpty());
            Output heartbeat = node.step(new Tick()); // second idle tick = heartbeatTicks
            assertEquals(2, heartbeat.messages().size());
        }

        @Test
        void rule2_appendsClientCommandsToItsLogAndIgnoresThemWhenNotLeader() {
            Node node = leader();
            Output out = node.step(new Propose(Command.ofUtf8("x")));
            assertEquals(List.of(new LogEntry(2, 1, Command.ofUtf8("x"))), out.persist().append());
            assertTrue(node(0).step(new Propose(Command.ofUtf8("x"))).persist().isEmpty());
        }

        @Test
        void rule3_successAdvancesMatchIndexAndFailureDecrementsNextIndexAndRetries() {
            Node node = leader();
            node.step(new Propose(Command.ofUtf8("x")));
            node.step(new Propose(Command.ofUtf8("y")));
            // n1 rejects: nextIndex 1 -> floor 1; retry carries everything from index 1.
            Output retry = receive(node, N1, new AppendEntriesResponse(1, false, 0));
            assertEquals(0, only(retry, N1, AppendEntries.class).prevLogIndex());
            assertEquals(3, only(retry, N1, AppendEntries.class).entries().size());
            // n1 now confirms index 1: the leader sends the remainder starting at index 2.
            Output more = receive(node, N1, new AppendEntriesResponse(1, true, 1));
            assertEquals(1, only(more, N1, AppendEntries.class).prevLogIndex());
        }

        @Test
        void rule4_commitsOnlyWhenAMajorityHasTheEntryAndItIsFromTheCurrentTerm() {
            Node node = leader();
            node.step(new Propose(Command.ofUtf8("x")));
            assertTrue(receive(node, N1, new AppendEntriesResponse(1, true, 0)).toApply().isEmpty());
            Output out = receive(node, N1, new AppendEntriesResponse(1, true, 2));
            assertEquals(2, out.toApply().size()); // NoOp and x, leader + n1 = majority of 3
        }

        @Test
        void rule4_neverCommitsAnOlderTermEntryByCountingReplicas() {
            // n0 holds [1:t1, 2:t2] and wins term 3; the replicated t2 entry stays uncommitted until a t3 entry is.
            Node node = node(2, entry(1, 1), entry(2, 2));
            ticks(node, 5);
            receive(node, N1, new RequestVoteResponse(3, true)); // leader; appends 3:NoOp@3
            Output out = receive(node, N1, new AppendEntriesResponse(3, true, 2));
            assertTrue(out.toApply().isEmpty());
            out = receive(node, N1, new AppendEntriesResponse(3, true, 3));
            assertEquals(3, out.toApply().size());
        }
    }
}

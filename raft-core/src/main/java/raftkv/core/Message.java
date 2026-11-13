package raftkv.core;

import java.util.List;
import java.util.Objects;

/** Raft RPCs; sender and receiver travel in {@link Input.Receive} / {@link Output.Send}. */
public sealed interface Message {

    long term();

    record RequestVote(long term, NodeId candidateId, long lastLogIndex, long lastLogTerm) implements Message {
        public RequestVote {
            nonNegative(term, "term");
            Objects.requireNonNull(candidateId, "candidateId");
            nonNegative(lastLogIndex, "lastLogIndex");
            nonNegative(lastLogTerm, "lastLogTerm");
        }
    }

    record RequestVoteResponse(long term, boolean voteGranted) implements Message {
        public RequestVoteResponse {
            nonNegative(term, "term");
        }
    }

    record AppendEntries(long term, NodeId leaderId, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit) implements Message {
        public AppendEntries {
            nonNegative(term, "term");
            Objects.requireNonNull(leaderId, "leaderId");
            nonNegative(prevLogIndex, "prevLogIndex");
            nonNegative(prevLogTerm, "prevLogTerm");
            nonNegative(leaderCommit, "leaderCommit");
            entries = List.copyOf(entries);
            for (int i = 0; i < entries.size(); i++) {
                if (entries.get(i).index() != prevLogIndex + 1 + i) {
                    throw new IllegalArgumentException("entries must be contiguous after prevLogIndex");
                }
            }
        }
    }

    /** {@code matchIndex} is an addition to the paper: it lets the leader match a reply to its request. */
    record AppendEntriesResponse(long term, boolean success, long matchIndex) implements Message {
        public AppendEntriesResponse {
            nonNegative(term, "term");
            nonNegative(matchIndex, "matchIndex");
        }
    }

    private static void nonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0: " + value);
        }
    }
}

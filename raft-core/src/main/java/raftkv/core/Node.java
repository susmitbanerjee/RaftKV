package raftkv.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.random.RandomGenerator;
import raftkv.core.Input.Propose;
import raftkv.core.Input.Receive;
import raftkv.core.Input.Tick;
import raftkv.core.Message.AppendEntries;
import raftkv.core.Message.AppendEntriesResponse;
import raftkv.core.Message.RequestVote;
import raftkv.core.Message.RequestVoteResponse;
import raftkv.core.Output.RoleChange;
import raftkv.core.Output.Send;
import raftkv.core.Payload.NoOp;

/** Raft server as a deterministic, single-threaded state machine. All side effects are returned in the {@link Output}. */
public final class Node {

    private final NodeId self;
    private final List<NodeId> peers;
    private final int majority;
    private final Options options;
    private final RandomGenerator rng;

    private long currentTerm;
    private Optional<NodeId> votedFor;
    private final List<LogEntry> log;

    private long commitIndex;
    private long lastApplied;
    private Role role = Role.FOLLOWER;
    private int electionElapsed;
    private int electionTimeout;
    private final Set<NodeId> votesGranted = new TreeSet<>();

    private final Map<NodeId, Long> nextIndex = new TreeMap<>();
    private final Map<NodeId, Long> matchIndex = new TreeMap<>();
    private int heartbeatElapsed;

    private Effects fx = new Effects();

    public Node(NodeId self, Set<NodeId> members, Options options, RandomGenerator rng, RestoredState restored) {
        this.self = Objects.requireNonNull(self, "self");
        if (!members.contains(self)) {
            throw new IllegalArgumentException("members must contain self");
        }
        this.peers = members.stream().filter(m -> !m.equals(self)).sorted().toList();
        this.majority = members.size() / 2 + 1;
        this.options = Objects.requireNonNull(options, "options");
        this.rng = Objects.requireNonNull(rng, "rng");
        this.currentTerm = restored.hardState().term();
        this.votedFor = restored.hardState().votedFor();
        this.log = new ArrayList<>(restored.log());
        resetElectionTimer();
    }

    public Output step(Input input) {
        fx = new Effects();
        switch (Objects.requireNonNull(input, "input")) {
            case Tick t -> onTick();
            case Receive r -> onReceive(r.from(), r.message());
            case Propose p -> onPropose(p.command());
        }
        applyCommitted();
        return fx.toOutput();
    }

    private void onTick() {
        if (role == Role.LEADER) {

            if (++heartbeatElapsed >= options.heartbeatTicks()) {
                broadcastAppendEntries();
            }
        } else if (++electionElapsed >= electionTimeout) {

            startElection();
        }
    }

    private void resetElectionTimer() {
        electionElapsed = 0;

        electionTimeout = rng.nextInt(options.electionTimeoutMinTicks(), options.electionTimeoutMaxTicks() + 1);
    }

    private void startElection() {

        currentTerm++;
        votedFor = Optional.of(self);
        fx.hardStateChanged = true;
        votesGranted.clear();
        votesGranted.add(self);
        setRole(Role.CANDIDATE);
        resetElectionTimer();
        if (votesGranted.size() >= majority) {
            becomeLeader();
            return;
        }
        RequestVote request = new RequestVote(currentTerm, self, lastIndex(), lastTerm());
        peers.forEach(peer -> send(peer, request));
    }

    private void onRequestVote(NodeId candidate, RequestVote m) {

        boolean grant = m.term() == currentTerm && mayVoteFor(candidate) && candidateIsUpToDate(m);
        if (grant) {
            votedFor = Optional.of(candidate);
            fx.hardStateChanged = true;
            resetElectionTimer();
        }
        send(candidate, new RequestVoteResponse(currentTerm, grant));
    }

    private boolean mayVoteFor(NodeId candidate) {
        return options.bug() == Bug.TWO_VOTES_PER_TERM || votedFor.isEmpty() || votedFor.get().equals(candidate);
    }

    private boolean candidateIsUpToDate(RequestVote m) {
        return options.bug() == Bug.SKIP_UP_TO_DATE_CHECK
                || m.lastLogTerm() > lastTerm()
                || (m.lastLogTerm() == lastTerm() && m.lastLogIndex() >= lastIndex());
    }

    private void onRequestVoteResponse(NodeId voter, RequestVoteResponse m) {
        if (role != Role.CANDIDATE || m.term() != currentTerm || !m.voteGranted()) {
            return;
        }
        votesGranted.add(voter);
        if (votesGranted.size() >= majority) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        setRole(Role.LEADER);
        for (NodeId peer : peers) {
            nextIndex.put(peer, lastIndex() + 1);
            matchIndex.put(peer, 0L);
        }
        append(new LogEntry(lastIndex() + 1, currentTerm, new NoOp()));
        broadcastAppendEntries();
        advanceCommitIndex();
    }

    private void stepDown(long newTerm) {
        currentTerm = newTerm;
        if (options.bug() != Bug.KEEP_VOTE_ON_NEW_TERM) {
            votedFor = Optional.empty();
        }
        fx.hardStateChanged = true;
        setRole(Role.FOLLOWER);
        resetElectionTimer();
    }

    private void setRole(Role newRole) {
        if (role != newRole) {
            role = newRole;
            fx.roleChanges.add(new RoleChange(newRole, currentTerm));
        }
    }

    private void onReceive(NodeId from, Message message) {
        if (!peers.contains(from) || !senderMatchesClaim(from, message)) {
            return;
        }
        if (message.term() > currentTerm) {
            stepDown(message.term());
        }
        switch (message) {
            case RequestVote m -> onRequestVote(from, m);
            case RequestVoteResponse m -> onRequestVoteResponse(from, m);
            case AppendEntries m -> onAppendEntries(from, m);
            case AppendEntriesResponse m -> onAppendEntriesResponse(from, m);
        }
    }

    private static boolean senderMatchesClaim(NodeId from, Message message) {
        return switch (message) {
            case RequestVote m -> m.candidateId().equals(from);
            case AppendEntries m -> m.leaderId().equals(from);
            case RequestVoteResponse m -> true;
            case AppendEntriesResponse m -> true;
        };
    }

    private void onAppendEntries(NodeId leader, AppendEntries m) {
        if (m.term() < currentTerm) {
            send(leader, new AppendEntriesResponse(currentTerm, false, 0));
            return;
        }
        if (role == Role.LEADER) {
            return;
        }
        setRole(Role.FOLLOWER);
        resetElectionTimer();
        if (m.prevLogIndex() > lastIndex() || termAt(m.prevLogIndex()) != m.prevLogTerm()) {
            send(leader, new AppendEntriesResponse(currentTerm, false, 0));
            return;
        }
        for (LogEntry entry : m.entries()) {
            if (entry.index() <= lastIndex()) {
                if (termAt(entry.index()) == entry.term()) {
                    continue;
                }
                if (options.bug() == Bug.NEVER_TRUNCATE) {
                    continue;
                }
                truncateFrom(entry.index());
            }
            append(entry);
        }
        long lastNew = m.prevLogIndex() + m.entries().size();
        if (m.leaderCommit() > commitIndex) {
            commitIndex = Math.max(commitIndex, Math.min(m.leaderCommit(), lastNew));
        }
        send(leader, new AppendEntriesResponse(currentTerm, true, lastNew));
    }

    private void onAppendEntriesResponse(NodeId follower, AppendEntriesResponse m) {
        if (role != Role.LEADER || m.term() != currentTerm) {
            return;
        }
        if (m.success()) {
            long match = Math.max(matchIndex.get(follower), Math.min(m.matchIndex(), lastIndex()));
            matchIndex.put(follower, match);
            nextIndex.put(follower, match + 1);
            advanceCommitIndex();
            if (match < lastIndex()) {
                sendAppendEntries(follower);
            }
        } else {

            nextIndex.put(follower, Math.max(matchIndex.get(follower) + 1, nextIndex.get(follower) - 1));
            sendAppendEntries(follower);
        }
    }

    private void onPropose(Payload.Command command) {
        if (role != Role.LEADER) {
            return;
        }
        if (options.bug() == Bug.LEADER_OVERWRITES_OWN_LOG && lastIndex() > commitIndex) {
            truncateFrom(commitIndex + 1);
        }
        append(new LogEntry(lastIndex() + 1, currentTerm, command));
        broadcastAppendEntries();
        advanceCommitIndex();
    }

    private void broadcastAppendEntries() {
        heartbeatElapsed = 0;
        peers.forEach(this::sendAppendEntries);
    }

    private void sendAppendEntries(NodeId peer) {
        long prev = Math.min(nextIndex.get(peer) - 1, lastIndex());
        int end = (int) Math.min(lastIndex(), prev + options.maxEntriesPerAppend());
        List<LogEntry> entries = log.subList((int) prev, end);
        send(peer, new AppendEntries(currentTerm, self, prev, termAt(prev), entries, commitIndex));
    }

    /** Commits the highest index replicated on a majority whose entry is from the current term. */
    private void advanceCommitIndex() {
        for (long n = lastIndex(); n > commitIndex; n--) {
            boolean currentTermEntry = termAt(n) == currentTerm;
            if ((currentTermEntry || options.bug() == Bug.COMMIT_OLDER_TERM_BY_COUNTING) && replicatedOnMajority(n)) {
                commitIndex = n;
                return;
            }
        }
    }

    private boolean replicatedOnMajority(long index) {
        int replicas = 1;
        for (NodeId peer : peers) {
            if (matchIndex.get(peer) >= index) {
                replicas++;
            }
        }
        return replicas >= majority;
    }

    private void applyCommitted() {
        long limit = options.bug() == Bug.APPLY_BEFORE_COMMIT ? lastIndex() : commitIndex;
        while (lastApplied < limit) {
            fx.applied.add(log.get((int) ++lastApplied - 1));
        }
    }

    private long lastIndex() {
        return log.size();
    }

    private long lastTerm() {
        return termAt(lastIndex());
    }

    private long termAt(long index) {
        return index == 0 ? 0 : log.get((int) index - 1).term();
    }

    private void append(LogEntry entry) {
        log.add(entry);
        fx.appended.add(entry);
    }

    private void truncateFrom(long index) {
        if (!fx.appended.isEmpty()) {
            throw new IllegalStateException("truncate must precede appends within a step");
        }
        log.subList((int) index - 1, log.size()).clear();
        fx.truncateFrom = OptionalLong.of(index);
    }

    private void send(NodeId to, Message message) {
        fx.messages.add(new Send(to, message));
    }

    private final class Effects {
        final List<Send> messages = new ArrayList<>();
        final List<LogEntry> appended = new ArrayList<>();
        final List<LogEntry> applied = new ArrayList<>();
        final List<RoleChange> roleChanges = new ArrayList<>();
        OptionalLong truncateFrom = OptionalLong.empty();
        boolean hardStateChanged;

        Output toOutput() {
            Optional<HardState> hardState =
                    hardStateChanged ? Optional.of(new HardState(currentTerm, votedFor)) : Optional.empty();
            return new Output(messages, new Persist(hardState, truncateFrom, appended), applied, roleChanges);
        }
    }
}

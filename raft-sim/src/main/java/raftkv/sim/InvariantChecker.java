package raftkv.sim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import raftkv.core.LogEntry;
import raftkv.core.NodeId;
import raftkv.core.Output;
import raftkv.core.Output.RoleChange;
import raftkv.core.Role;
import raftkv.sim.InvariantViolation.Kind;

/** Checks the Raft safety properties after every simulated step. */
final class InvariantChecker {

    private final List<FakeDisk> disks;
    private final Role[] roles;
    private final long[] applied;
    private final Map<Long, NodeId> leaderOfTerm = new HashMap<>();
    private final List<LogEntry> committed = new ArrayList<>();
    private final List<Long> committedInTerm = new ArrayList<>();

    InvariantChecker(List<FakeDisk> disks) {
        this.disks = disks;
        this.roles = new Role[disks.size()];
        this.applied = new long[disks.size()];
        Arrays.fill(roles, Role.FOLLOWER);
    }

    Role role(NodeId id) {
        return roles[id.value()];
    }

    void onCrash(NodeId id) {
        roles[id.value()] = Role.FOLLOWER;
        applied[id.value()] = 0;
    }

    void afterStep(NodeId id, Role roleBefore, Output out) {
        int n = id.value();
        for (RoleChange change : out.roleChanges()) {
            roles[n] = change.role();
        }
        checkLeaderAppendOnly(id, roleBefore, out);
        for (RoleChange change : out.roleChanges()) {
            if (change.role() == Role.LEADER) {
                checkElectionSafety(id, change.term());
                checkLeaderCompleteness(id, change.term());
            }
        }
        checkStateMachineSafety(id, out.toApply());
        if (!out.persist().append().isEmpty() || out.persist().truncateFrom().isPresent()) {
            checkLogMatching(id);
        }
    }

    /** Election Safety: at most one leader per term. */
    private void checkElectionSafety(NodeId id, long term) {
        NodeId previous = leaderOfTerm.putIfAbsent(term, id);
        if (previous != null && !previous.equals(id)) {
            throw new InvariantViolation(Kind.ELECTION_SAFETY, previous + " and " + id + " both lead term " + term);
        }
    }

    /** Leader Append-Only: a leader never overwrites or deletes entries in its log. */
    private void checkLeaderAppendOnly(NodeId id, Role roleBefore, Output out) {
        if (roleBefore == Role.LEADER && roles[id.value()] == Role.LEADER && out.persist().truncateFrom().isPresent()) {
            throw new InvariantViolation(Kind.LEADER_APPEND_ONLY,
                    id + " truncated its own log from index " + out.persist().truncateFrom().getAsLong());
        }
    }

    /** Log Matching: same index and term implies identical logs up through that index. */
    private void checkLogMatching(NodeId id) {
        List<LogEntry> mine = disks.get(id.value()).log();
        for (int other = 0; other < disks.size(); other++) {
            if (other == id.value()) {
                continue;
            }
            List<LogEntry> theirs = disks.get(other).log();
            int common = Math.min(mine.size(), theirs.size());
            int highestMatch = -1;
            for (int i = common - 1; i >= 0; i--) {
                if (mine.get(i).term() == theirs.get(i).term()) {
                    highestMatch = i;
                    break;
                }
            }
            for (int i = 0; i <= highestMatch; i++) {
                if (!mine.get(i).equals(theirs.get(i))) {
                    throw new InvariantViolation(Kind.LOG_MATCHING, id + " and n" + other + " share index "
                            + (highestMatch + 1) + " term " + mine.get(highestMatch).term()
                            + " but differ at index " + (i + 1) + ": " + mine.get(i) + " vs " + theirs.get(i));
                }
            }
        }
    }

    /** State Machine Safety: no two nodes apply different entries at the same index. */
    private void checkStateMachineSafety(NodeId id, List<LogEntry> toApply) {
        int n = id.value();
        for (LogEntry entry : toApply) {
            if (entry.index() != applied[n] + 1) {
                throw new InvariantViolation(Kind.STATE_MACHINE_SAFETY,
                        id + " applied index " + entry.index() + " after " + applied[n]);
            }
            applied[n] = entry.index();
            if (entry.index() <= committed.size()) {
                LogEntry known = committed.get((int) entry.index() - 1);
                if (!known.equals(entry)) {
                    throw new InvariantViolation(Kind.STATE_MACHINE_SAFETY,
                            id + " applied " + entry + " but another node applied " + known + " at that index");
                }
            } else {
                committed.add(entry);
                committedInTerm.add(disks.get(n).hardState().term());
            }
        }
    }

    /** Leader Completeness: every entry committed in an earlier term is in a new leader's log. */
    private void checkLeaderCompleteness(NodeId id, long term) {
        List<LogEntry> log = disks.get(id.value()).log();
        for (int i = 0; i < committed.size(); i++) {
            if (committedInTerm.get(i) < term && (log.size() <= i || !log.get(i).equals(committed.get(i)))) {
                throw new InvariantViolation(Kind.LEADER_COMPLETENESS, id + " became leader of term " + term
                        + " without committed entry " + committed.get(i));
            }
        }
    }
}

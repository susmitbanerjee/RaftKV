package raftkv.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import raftkv.core.Bug;
import raftkv.core.Input;
import raftkv.core.Input.Propose;
import raftkv.core.Input.Receive;
import raftkv.core.Input.Tick;
import raftkv.core.LogEntry;
import raftkv.core.Message;
import raftkv.core.Node;
import raftkv.core.NodeId;
import raftkv.core.Options;
import raftkv.core.Output;
import raftkv.core.Payload.Command;
import raftkv.core.Payload.NoOp;
import raftkv.core.Role;
import raftkv.sim.InvariantViolation.Kind;

/** Figure 8: a leader must not commit an earlier-term entry by counting replicas. */
class Figure8Test {

    @Test
    void correctRaftSurvivesFigure8() {
        ScriptedCluster cluster = new ScriptedCluster(Bug.NONE);
        cluster.playFigure8();
        List<LogEntry> applied = cluster.applied(4);
        assertEquals(3, applied.size());
        assertEquals(new LogEntry(2, 2, new NoOp()), applied.get(1));
        assertEquals(new LogEntry(3, 4, new NoOp()), applied.get(2));
    }

    @Test
    void committingOlderTermEntriesByCountingIsCaught() {
        ScriptedCluster cluster = new ScriptedCluster(Bug.COMMIT_OLDER_TERM_BY_COUNTING);
        InvariantViolation violation = assertThrows(InvariantViolation.class, cluster::playFigure8);
        assertEquals(Kind.LEADER_COMPLETENESS, violation.kind());
    }

    private static final class ScriptedCluster {
        private record Pending(int from, int to, Message message) {}

        private final Options options;
        private final Set<NodeId> members = Set.copyOf(IntStream.range(0, 5).mapToObj(NodeId::new).toList());
        private final Node[] nodes = new Node[5];
        private final List<FakeDisk> disks = new ArrayList<>();
        private final List<List<LogEntry>> applied = new ArrayList<>();
        private final InvariantChecker checker;
        private final List<Pending> pending = new ArrayList<>();

        ScriptedCluster(Bug bug) {
            options = new Options(2, 5, 5, 1, bug);
            for (int i = 0; i < 5; i++) {
                disks.add(new FakeDisk());
                applied.add(new ArrayList<>());
            }
            checker = new InvariantChecker(disks);
            for (int i = 0; i < 5; i++) {
                boot(i);
            }
        }

        List<LogEntry> applied(int node) {
            return applied.get(node);
        }

        void playFigure8() {
            // (a)
            tick(0, 5);
            deliver(0, 1, 2, 3, 4);          // vote requests
            deliverTo(0, 1, 2, 3, 4);        // votes back: leader of term 1 after two grants
            deliver(0, 1, 2, 3, 4);          // NoOp(1) to everyone
            deliverTo(0, 1, 2, 3, 4);        // acks: index 1 commits
            propose(0, "X");
            deliver(0, 1);
            deliver(1, 0);                   // X is on n0 and n1 only
            pending.clear();
            // (b)
            crash(0);
            tick(4, 5);
            deliver(4, 2);
            deliver(4, 3);
            deliver(2, 4);
            deliver(3, 4);                   // n4 leads term 2 and appends NoOp(2) locally
            pending.clear();
            crash(4);
            // (c)
            boot(0);
            tick(0, 5);                      // term 2 candidacy, never delivered
            pending.clear();
            tick(0, 5);                      // term 3
            drop(0, 3);
            drop(0, 4);
            deliver(0, 1);
            deliver(0, 2);
            deliver(1, 0);
            deliver(2, 0);                   // n0 leads term 3, sends NoOp(3)
            deliver(0, 1);
            deliver(1, 0);                   // n1 accepts NoOp(3)
            deliver(0, 2);
            deliver(2, 0);                   // n2 lacks index 2: rejects
            deliver(0, 2);
            deliver(2, 0);                   // X reaches n2: majority of replicas, but term 1
            pending.clear();
            // (d)
            crash(0);
            boot(4);
            tick(4, 5);                      // term 3 candidacy, never delivered
            pending.clear();
            tick(4, 5);                      // term 4
            deliver(4, 2);
            deliver(4, 3);
            deliver(2, 4);
            deliver(3, 4);                   // n4 leads term 4
            for (int round = 0; round < 6; round++) {
                deliver(4, 2, 3);
                deliver(2, 4);
                deliver(3, 4);
            }
        }

        private void boot(int i) {
            nodes[i] = new Node(new NodeId(i), members, options, new SplittableRandom(i), disks.get(i).restore());
        }

        private void crash(int i) {
            nodes[i] = null;
            checker.onCrash(new NodeId(i));
            applied.get(i).clear();
        }

        private void tick(int node, int count) {
            for (int i = 0; i < count; i++) {
                process(node, new Tick());
            }
        }

        private void propose(int node, String command) {
            process(node, new Propose(Command.ofUtf8(command)));
        }

            private void deliver(int from, int... tos) {
            for (int to : tos) {
                List<Pending> batch = pending.stream().filter(p -> p.from == from && p.to == to).toList();
                pending.removeAll(batch);
                batch.forEach(p -> {
                    if (nodes[to] != null) {
                        process(to, new Receive(new NodeId(from), p.message));
                    }
                });
            }
        }

            private void deliverTo(int to, int... froms) {
            for (int from : froms) {
                deliver(from, to);
            }
        }

        private void drop(int from, int to) {
            pending.removeIf(p -> p.from == from && p.to == to);
        }

        private void process(int node, Input input) {
            NodeId id = new NodeId(node);
            Role before = checker.role(id);
            Output out = nodes[node].step(input);
            disks.get(node).apply(out.persist());
            checker.afterStep(id, before, out);
            applied.get(node).addAll(out.toApply());
            out.messages().forEach(s -> pending.add(new Pending(node, s.to().value(), s.message())));
        }
    }
}

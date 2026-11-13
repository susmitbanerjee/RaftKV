package raftkv.sim;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import raftkv.core.Input;
import raftkv.core.Input.Propose;
import raftkv.core.Input.Receive;
import raftkv.core.Input.Tick;
import raftkv.core.LogEntry;
import raftkv.core.Node;
import raftkv.core.NodeId;
import raftkv.core.Output;
import raftkv.core.Output.Send;
import raftkv.core.Payload;
import raftkv.core.Payload.Command;
import raftkv.core.Role;
import raftkv.sim.Event.Deliver;
import raftkv.sim.Event.TickAt;
import raftkv.sim.InvariantViolation.Kind;

/** Deterministic cluster driven by one seeded PRNG and a virtual clock. */
public final class Simulation {

    private final SimConfig config;
    private final SplittableRandom rng;
    private final List<NodeId> ids;
    private final Set<NodeId> members;
    private final Node[] nodes;
    private final List<FakeDisk> disks = new ArrayList<>();
    private final InvariantChecker checker;
    private final PriorityQueue<Event> queue =
            new PriorityQueue<>(Comparator.comparingLong(Event::time).thenComparingLong(Event::seq));
    private final List<Object> trace = new ArrayList<>();
    private final Set<Payload> awaited = new HashSet<>();

    private FaultSchedule faults;
    private Set<Link> links;
    private long now;
    private long seq;
    private int proposals;
    private boolean awaitedCommitted;

    public Simulation(long seed, SimConfig config, FaultSchedule faults) {
        this.config = config;
        this.faults = faults;
        this.rng = new SplittableRandom(seed);
        this.ids = IntStream.range(0, config.nodes()).mapToObj(NodeId::new).toList();
        this.members = Set.copyOf(ids);
        this.nodes = new Node[ids.size()];
        ids.forEach(id -> disks.add(new FakeDisk()));
        this.checker = new InvariantChecker(disks);
        this.links = fullyConnected();
        for (NodeId id : ids) {
            boot(id);
            queue.add(new TickAt(1, seq++, id));
        }
    }

    public void run(int steps) {
        for (int i = 0; i < steps; i++) {
            step();
        }
    }

    public void heal() {
        faults = new FaultSchedule(0, 0, 2, 0, 0, 0, 0);
        links = fullyConnected();
        trace.add("heal");
        ids.stream().filter(id -> nodes[id.value()] == null).forEach(this::boot);
    }

    public boolean awaitLeader(int maxTicks) {
        long deadline = now + maxTicks;
        while (now < deadline) {
            if (currentLeader() != null) {
                return true;
            }
            step();
        }
        return currentLeader() != null;
    }

    public boolean proposeAndAwaitCommit(int maxTicks) {
        long deadline = now + maxTicks;
        long nextProposal = now;
        while (now < deadline && !awaitedCommitted) {
            if (now >= nextProposal && currentLeader() != null) {
                Command command = Command.ofUtf8("await-" + proposals++);
                awaited.add(command);
                process(currentLeader(), new Propose(command));
                nextProposal = now + maxTicks / 4 + 1;
            }
            step();
        }
        return awaitedCommitted;
    }

    /** Leader of the highest term among running nodes, or null. */
    public NodeId currentLeader() {
        long highest = ids.stream().filter(this::isUp).mapToLong(this::term).max().orElse(0);
        return ids.stream()
                .filter(id -> isUp(id) && checker.role(id) == Role.LEADER && term(id) == highest)
                .findFirst().orElse(null);
    }

    public List<Object> trace() {
        return trace;
    }

    private void step() {
        injectFaults();
        Event event = queue.poll();
        now = event.time();
        trace.add(event);
        switch (event) {
            case Deliver d -> {
                if (isUp(d.to()) && links.contains(new Link(d.from(), d.to()))) {
                    process(d.to(), new Receive(d.from(), d.message()));
                }
            }
            case TickAt t -> {
                if (isUp(t.node())) {
                    process(t.node(), new Tick());
                }
                queue.add(new TickAt(now + 1, seq++, t.node()));
            }
        }
    }

    private void process(NodeId id, Input input) {
        Role roleBefore = checker.role(id);
        Output out;
        try {
            out = nodes[id.value()].step(input);
            disks.get(id.value()).apply(out.persist());
        } catch (RuntimeException e) {
            throw new InvariantViolation(Kind.INTERNAL_ERROR, id + " failed on " + input + ": " + e, e);
        }
        checker.afterStep(id, roleBefore, out);
        for (LogEntry entry : out.toApply()) {
            awaitedCommitted |= awaited.contains(entry.payload());
        }
        out.messages().forEach(send -> route(id, send));
    }

    private void route(NodeId from, Send send) {
        if (!links.contains(new Link(from, send.to()))) {
            return;
        }
        int copies = chance(faults.duplicate()) ? 2 : 1;
        for (int i = 0; i < copies; i++) {
            if (!chance(faults.drop())) {
                long delay = 1 + rng.nextInt(faults.maxDelayTicks());
                queue.add(new Deliver(now + delay, seq++, from, send.to(), send.message()));
            }
        }
    }

    private void injectFaults() {
        if (chance(faults.partitionChange())) {
            links = randomLinks();
            trace.add("links " + links.size() + "/" + ids.size() * (ids.size() - 1));
        }
        if (chance(faults.crash())) {
            pick(true).ifPresent(this::crash);
        }
        if (chance(faults.restart())) {
            pick(false).ifPresent(this::boot);
        }
        if (chance(faults.propose())) {
            NodeId target = currentLeader() != null ? currentLeader() : ids.get(rng.nextInt(ids.size()));
            if (isUp(target)) {
                process(target, new Propose(Command.ofUtf8("cmd-" + proposals++)));
            }
        }
    }

    private Optional<NodeId> pick(boolean up) {
        List<NodeId> candidates = ids.stream().filter(id -> isUp(id) == up).toList();
        return candidates.isEmpty() ? Optional.empty()
                : Optional.of(candidates.get(rng.nextInt(candidates.size())));
    }

    private void crash(NodeId id) {
        trace.add("crash " + id);
        nodes[id.value()] = null;
        checker.onCrash(id);
    }

    private void boot(NodeId id) {
        trace.add("boot " + id);
        nodes[id.value()] = new Node(id, members, config.options(), new SplittableRandom(rng.nextLong()),
                disks.get(id.value()).restore());
    }

    private Set<Link> randomLinks() {
        Set<Link> result = new HashSet<>();
        int mode = rng.nextInt(3);
        boolean[] side = new boolean[ids.size()];
        for (int i = 0; i < side.length; i++) {
            side[i] = rng.nextBoolean();
        }
        for (NodeId from : ids) {
            for (NodeId to : ids) {
                boolean connected = switch (mode) {
                    case 0 -> true;
                    case 1 -> side[from.value()] == side[to.value()];
                    default -> rng.nextDouble() < 0.7;
                };
                if (!from.equals(to) && connected) {
                    result.add(new Link(from, to));
                }
            }
        }
        return result;
    }

    private Set<Link> fullyConnected() {
        return ids.stream()
                .flatMap(from -> ids.stream().filter(to -> !to.equals(from)).map(to -> new Link(from, to)))
                .collect(Collectors.toSet());
    }

    private boolean chance(double probability) {
        return probability > 0 && rng.nextDouble() < probability;
    }

    private boolean isUp(NodeId id) {
        return nodes[id.value()] != null;
    }

    private long term(NodeId id) {
        return disks.get(id.value()).hardState().term();
    }
}

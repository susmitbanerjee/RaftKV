package raftkv.sim;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Optional;
import raftkv.sim.InvariantViolation.Kind;

/** Runs a seeded chaos phase, heals, then requires liveness. */
public final class SimRunner {

    static final int LIVENESS_TICKS = 1_000;

    private SimRunner() {}

    public static RunResult run(long seed, int steps, FaultSchedule faultSchedule) {
        return run(seed, steps, faultSchedule, SimConfig.defaults(), System.err);
    }

    public static RunResult runQuietly(long seed, int steps, FaultSchedule faultSchedule, SimConfig config) {
        return run(seed, steps, faultSchedule, config, new PrintStream(OutputStream.nullOutputStream()));
    }

    public static RunResult run(long seed, int steps, FaultSchedule faultSchedule, SimConfig config, PrintStream out) {
        Simulation sim = new Simulation(seed, config, faultSchedule);
        try {
            sim.run(steps);
            sim.heal();
            if (!sim.awaitLeader(LIVENESS_TICKS)) {
                throw new InvariantViolation(Kind.LIVENESS, "no leader within " + LIVENESS_TICKS + " ticks of healing");
            }
            if (!sim.proposeAndAwaitCommit(LIVENESS_TICKS)) {
                throw new InvariantViolation(Kind.LIVENESS, "no command committed within " + LIVENESS_TICKS + " ticks");
            }
            return new RunResult(seed, Optional.empty(), List.of());
        } catch (InvariantViolation violation) {
            report(out, seed, steps, violation, sim.trace());
            return new RunResult(seed, Optional.of(violation), sim.trace());
        }
    }

    private static void report(PrintStream out, long seed, int steps, InvariantViolation violation, List<Object> trace) {
        out.println("INVARIANT VIOLATION seed=" + seed + " steps=" + steps + " " + violation.getMessage());
        for (int i = 0; i < trace.size(); i++) {
            out.println("  " + i + ": " + trace.get(i));
        }
    }
}

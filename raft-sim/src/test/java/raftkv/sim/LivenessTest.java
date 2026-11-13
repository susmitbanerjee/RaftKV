package raftkv.sim;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class LivenessTest {

    @Test
    void leaderIsElectedAndACommandCommitsAfterHealing() {
        IntStream.range(0, 500).parallel().forEach(seed -> {
            Simulation sim = new Simulation(seed, SimConfig.defaults(), FaultSchedule.chaos());
            sim.run(2_000);
            sim.heal();
            assertTrue(sim.awaitLeader(SimRunner.LIVENESS_TICKS), "no leader after heal, seed " + seed);
            assertTrue(sim.proposeAndAwaitCommit(SimRunner.LIVENESS_TICKS), "no commit after heal, seed " + seed);
        });
    }
}

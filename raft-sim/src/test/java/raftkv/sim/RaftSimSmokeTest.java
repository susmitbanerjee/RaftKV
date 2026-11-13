package raftkv.sim;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RaftSimSmokeTest {
    @Test
    void aHealthyClusterElectsALeaderAndCommits() {
        RunResult result = SimRunner.run(1L, 500, FaultSchedule.none());
        assertTrue(result.passed());
    }
}

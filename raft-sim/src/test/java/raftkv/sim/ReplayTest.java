package raftkv.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;
import raftkv.core.Bug;

class ReplayTest {

    @Test
    void sameSeedReplaysIdentically() {
        SimConfig buggy = SimConfig.defaults().withBug(Bug.TWO_VOTES_PER_TERM);
        for (long seed = 0; seed < 200; seed++) {
            RunResult first = SimRunner.runQuietly(seed, RandomSeedsTest.STEPS, FaultSchedule.chaos(), buggy);
            RunResult second = SimRunner.runQuietly(seed, RandomSeedsTest.STEPS, FaultSchedule.chaos(), buggy);
            assertEquals(first.trace(), second.trace(), "trace differs for seed " + seed);
            assertEquals(first.violation().map(Throwable::getMessage), second.violation().map(Throwable::getMessage));
            if (!first.passed()) {
                assertFalse(first.trace().isEmpty());
                return;
            }
        }
        throw new AssertionError("expected TWO_VOTES_PER_TERM to fail within 200 seeds");
    }
}

package raftkv.sim;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import raftkv.core.Bug;

class MutationTest {

    static final int SEED_BUDGET = 100_000;

    @ParameterizedTest
    @EnumSource(value = Bug.class, names = "NONE", mode = EnumSource.Mode.EXCLUDE)
    void simulatorCatchesInjectedBug(Bug bug) {
        SimConfig config = SimConfig.defaults().withBug(bug);
        Optional<RunResult> caught = IntStream.range(0, SEED_BUDGET).parallel()
                .mapToObj(seed -> SimRunner.runQuietly(seed, RandomSeedsTest.STEPS, FaultSchedule.chaos(), config))
                .filter(result -> !result.passed())
                .findAny();
        assertTrue(caught.isPresent(), bug + " survived " + SEED_BUDGET + " seeds");
        System.out.println(bug + " caught by seed " + caught.get().seed() + ": "
                + caught.get().violation().orElseThrow().getMessage().lines().findFirst().orElse(""));
    }
}

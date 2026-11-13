package raftkv.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class RandomSeedsTest {

    static final int STEPS = 3_000;

    @Test
    void tenThousandSeedsWithFaultsKeepEveryInvariant() {
        List<Long> failing = IntStream.range(0, 10_000).parallel()
                .mapToObj(seed -> SimRunner.runQuietly(seed, STEPS, FaultSchedule.chaos(), SimConfig.defaults()))
                .filter(result -> !result.passed())
                .map(RunResult::seed)
                .toList();
        assertEquals(List.of(), failing, "failing seeds");
    }
}

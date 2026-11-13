package raftkv.sim;

import java.util.List;
import java.util.Optional;

/** Outcome of one seeded run. */
public record RunResult(long seed, Optional<InvariantViolation> violation, List<Object> trace) {

    public boolean passed() {
        return violation.isEmpty();
    }
}

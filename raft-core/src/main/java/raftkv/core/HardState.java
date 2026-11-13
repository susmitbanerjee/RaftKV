package raftkv.core;

import java.util.Objects;
import java.util.Optional;

/** The persistent (currentTerm, votedFor) pair. */
public record HardState(long term, Optional<NodeId> votedFor) {

    public HardState {
        if (term < 0) {
            throw new IllegalArgumentException("term must be >= 0: " + term);
        }
        Objects.requireNonNull(votedFor, "votedFor");
    }

    public static HardState initial() {
        return new HardState(0, Optional.empty());
    }
}

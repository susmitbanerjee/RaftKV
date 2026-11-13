package raftkv.core;

import java.util.List;
import java.util.Objects;

/** Persistent state read at boot. */
public record RestoredState(HardState hardState, List<LogEntry> log) {

    public RestoredState {
        Objects.requireNonNull(hardState, "hardState");
        log = List.copyOf(log);
        long previousTerm = 0;
        for (int i = 0; i < log.size(); i++) {
            LogEntry entry = log.get(i);
            if (entry.index() != i + 1 || entry.term() < previousTerm || entry.term() > hardState.term()) {
                throw new IllegalArgumentException("corrupt persisted log at position " + i);
            }
            previousTerm = entry.term();
        }
    }

    public static RestoredState empty() {
        return new RestoredState(HardState.initial(), List.of());
    }
}

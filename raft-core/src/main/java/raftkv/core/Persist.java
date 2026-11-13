package raftkv.core;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Durable changes of one step, applied in order: hard state, truncate, append. Must be fsynced before sending messages. */
public record Persist(Optional<HardState> hardState, OptionalLong truncateFrom, List<LogEntry> append) {

    public Persist {
        append = List.copyOf(append);
    }

    public boolean isEmpty() {
        return hardState.isEmpty() && truncateFrom.isEmpty() && append.isEmpty();
    }
}

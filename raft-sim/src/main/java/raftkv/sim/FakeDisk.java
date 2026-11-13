package raftkv.sim;

import java.util.ArrayList;
import java.util.List;
import raftkv.core.HardState;
import raftkv.core.LogEntry;
import raftkv.core.Persist;
import raftkv.core.RestoredState;

/** In-memory stable storage that survives crashes. */
final class FakeDisk {

    private HardState hardState = HardState.initial();
    private final List<LogEntry> log = new ArrayList<>();

    void apply(Persist persist) {
        persist.hardState().ifPresent(hs -> hardState = hs);
        persist.truncateFrom().ifPresent(index -> log.subList((int) index - 1, log.size()).clear());
        for (LogEntry entry : persist.append()) {
            if (entry.index() != log.size() + 1) {
                throw new IllegalStateException("non-contiguous append: " + entry.index() + " after " + log.size());
            }
            log.add(entry);
        }
    }

    HardState hardState() {
        return hardState;
    }

    List<LogEntry> log() {
        return log;
    }

    RestoredState restore() {
        return new RestoredState(hardState, log);
    }
}

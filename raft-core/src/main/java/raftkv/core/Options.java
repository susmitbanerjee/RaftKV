package raftkv.core;

import java.util.Objects;

/** Timing in ticks, batch size in entries. */
public record Options(int heartbeatTicks, int electionTimeoutMinTicks, int electionTimeoutMaxTicks,
                      int maxEntriesPerAppend, Bug bug) {

    public Options {
        if (heartbeatTicks < 1 || electionTimeoutMinTicks <= heartbeatTicks
                || electionTimeoutMaxTicks < electionTimeoutMinTicks || maxEntriesPerAppend < 1) {
            throw new IllegalArgumentException("invalid options");
        }
        Objects.requireNonNull(bug, "bug");
    }

    public static Options defaults() {
        return new Options(3, 10, 20, 64, Bug.NONE);
    }

    public Options withBug(Bug newBug) {
        return new Options(heartbeatTicks, electionTimeoutMinTicks, electionTimeoutMaxTicks, maxEntriesPerAppend, newBug);
    }
}

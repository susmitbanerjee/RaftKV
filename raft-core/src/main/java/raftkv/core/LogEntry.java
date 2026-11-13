package raftkv.core;

import java.util.Objects;

/** A log entry; index and term start at 1. */
public record LogEntry(long index, long term, Payload payload) {

    public LogEntry {
        if (index < 1 || term < 1) {
            throw new IllegalArgumentException("index and term must be >= 1: " + index + "/" + term);
        }
        Objects.requireNonNull(payload, "payload");
    }
}

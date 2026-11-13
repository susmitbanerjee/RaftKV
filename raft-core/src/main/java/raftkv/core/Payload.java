package raftkv.core;

import java.util.Objects;

/** What a log entry carries. */
public sealed interface Payload {

    /** A client command, opaque to consensus. */
    record Command(Bytes data) implements Payload {
        public Command {
            Objects.requireNonNull(data, "data");
        }

        public static Command ofUtf8(String text) {
            return new Command(Bytes.ofUtf8(text));
        }
    }

    /** Appended by every new leader at the start of its term. */
    record NoOp() implements Payload {}
}

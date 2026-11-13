package raftkv.core;

import java.util.Objects;

/** Inputs accepted by {@link Node#step}. */
public sealed interface Input {

    record Tick() implements Input {}

    record Receive(NodeId from, Message message) implements Input {
        public Receive {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(message, "message");
        }
    }

    record Propose(Payload.Command command) implements Input {
        public Propose {
            Objects.requireNonNull(command, "command");
        }
    }
}

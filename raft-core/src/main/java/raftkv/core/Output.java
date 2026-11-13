package raftkv.core;

import java.util.List;
import java.util.Objects;

/** Everything a step emits. */
public record Output(List<Send> messages, Persist persist, List<LogEntry> toApply, List<RoleChange> roleChanges) {

    public Output {
        messages = List.copyOf(messages);
        Objects.requireNonNull(persist, "persist");
        toApply = List.copyOf(toApply);
        roleChanges = List.copyOf(roleChanges);
    }

    public record Send(NodeId to, Message message) {
        public Send {
            Objects.requireNonNull(to, "to");
            Objects.requireNonNull(message, "message");
        }
    }

    public record RoleChange(Role role, long term) {
        public RoleChange {
            Objects.requireNonNull(role, "role");
        }
    }
}

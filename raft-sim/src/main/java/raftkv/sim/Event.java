package raftkv.sim;

import raftkv.core.Message;
import raftkv.core.NodeId;

/** Item scheduled on the virtual clock, ordered by (time, seq). */
sealed interface Event {

    long time();

    long seq();

    record Deliver(long time, long seq, NodeId from, NodeId to, Message message) implements Event {}

    record TickAt(long time, long seq, NodeId node) implements Event {}
}

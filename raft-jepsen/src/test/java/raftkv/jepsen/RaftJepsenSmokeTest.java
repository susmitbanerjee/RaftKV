package raftkv.jepsen;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import raftkv.server.RaftServer;

class RaftJepsenSmokeTest {
    @Test
    void seesRaftCoreAndServerOnItsClasspath() {
        assertEquals("raft-jepsen", RaftJepsen.MODULE);
        assertEquals("n0", new raftkv.core.NodeId(0).toString());
        assertEquals("raft-server", RaftServer.MODULE);
    }
}

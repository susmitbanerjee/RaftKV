package raftkv.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RaftServerSmokeTest {
    @Test
    void seesRaftCoreOnItsClasspath() {
        assertEquals("raft-server", RaftServer.MODULE);
        assertEquals("n0", new raftkv.core.NodeId(0).toString());
    }
}

package raftkv.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.SplittableRandom;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

class RaftCoreSmokeTest {

    @Test
    void runsOnJava21() {
        assertTrue(Runtime.version().feature() >= 21);
        assertEquals("n3", new NodeId(3).toString());
    }

    @Property
    void sameSeedGivesSameSequence(@ForAll long seed) {
        SplittableRandom a = new SplittableRandom(seed);
        SplittableRandom b = new SplittableRandom(seed);
        for (int i = 0; i < 16; i++) {
            assertEquals(a.nextInt(150, 300), b.nextInt(150, 300));
        }
    }
}

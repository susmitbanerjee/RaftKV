package raftkv.sim;

import raftkv.core.Bug;
import raftkv.core.Options;

/** Cluster shape and node options for a run. */
public record SimConfig(int nodes, Options options) {

    public SimConfig {
        if (nodes < 1) {
            throw new IllegalArgumentException("nodes must be >= 1");
        }
    }

    /** Five nodes, one entry per AppendEntries. */
    public static SimConfig defaults() {
        return new SimConfig(5, new Options(3, 10, 20, 1, Bug.NONE));
    }

    public SimConfig withBug(Bug bug) {
        return new SimConfig(nodes, options.withBug(bug));
    }
}

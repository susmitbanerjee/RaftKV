package raftkv.core;

/** Cluster member identity. */
public record NodeId(int value) implements Comparable<NodeId> {

    public NodeId {
        if (value < 0) {
            throw new IllegalArgumentException("node id must be >= 0: " + value);
        }
    }

    @Override
    public int compareTo(NodeId other) {
        return Integer.compare(value, other.value);
    }

    @Override
    public String toString() {
        return "n" + value;
    }
}

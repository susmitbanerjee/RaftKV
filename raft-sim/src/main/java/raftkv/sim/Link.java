package raftkv.sim;

import raftkv.core.NodeId;

/** Directed channel between two nodes. */
public record Link(NodeId from, NodeId to) {}

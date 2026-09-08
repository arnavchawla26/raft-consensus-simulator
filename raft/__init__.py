"""raft: a from-scratch, in-process simulator for the Raft consensus algorithm.

This package implements the core Raft protocol (leader election + log
replication, Ongaro & Ousterhout 2014) as a pure, deterministic state
machine (see raft.node.RaftNode), driven by a discrete-event network
simulator (see raft.cluster.Simulator) that can inject message delay,
message loss, network partitions, and node crashes/restarts.

It does not implement cluster membership changes (joint consensus) or
log compaction/snapshotting -- see the README for the full list of
simplifications relative to the paper.
"""

__version__ = "0.1.0"

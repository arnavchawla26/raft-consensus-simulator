"""RPC message types exchanged between RaftNode instances.

These mirror Figure 2 of the Raft paper (RequestVote / AppendEntries).
They are plain immutable dataclasses -- the simulator is responsible for
"transmitting" them (with delay/loss/partitions), not the nodes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from .log import LogEntry


@dataclass(frozen=True)
class RequestVoteArgs:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteReply:
    term: int
    vote_granted: bool
    voter_id: int
    candidate_id: int


@dataclass(frozen=True)
class AppendEntriesArgs:
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry] = field(default_factory=list)
    leader_commit: int = 0

    @property
    def is_heartbeat(self) -> bool:
        return len(self.entries) == 0


@dataclass(frozen=True)
class AppendEntriesReply:
    term: int
    success: bool
    follower_id: int
    leader_id: int
    match_index: int = 0

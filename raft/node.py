"""RaftNode: a pure, deterministic implementation of the Raft state machine
for a single server (Figure 2 of the Raft paper).

A RaftNode knows nothing about time, sockets, or other nodes' internals --
it only reacts to RPC arguments/replies handed to it and returns RPC
replies (or None). All timing (election timeouts, heartbeats) and message
delivery live in raft.cluster.Simulator. This separation is what makes the
protocol logic unit-testable without a discrete-event simulator, and makes
the simulator's job "just" scheduling and delivery.

Simplifications relative to the full paper (documented here, not hidden):
  * No cluster membership changes (joint consensus) -- peer set is fixed
    at construction time.
  * No log compaction / snapshotting -- logs grow unboundedly.
  * No persistent-storage layer -- "persistent state" (currentTerm,
    votedFor, log) just lives in memory. A crashed-and-restarted node in
    the simulator keeps this state (as if it had been fsynced), matching
    the paper's crash-recovery model, but there is no on-disk WAL.
  * nextIndex backs off one entry per rejected AppendEntries (the basic
    algorithm from Figure 2), not the faster conflict-term backtracking
    described as an optimization in section 5.3.
"""

from __future__ import annotations

from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set, Tuple

from .log import RaftLog, LogEntry
from .messages import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
)


class Role(Enum):
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()


class RaftNode:
    def __init__(self, node_id: int, peer_ids: List[int]) -> None:
        self.node_id = node_id
        self.peer_ids: List[int] = list(peer_ids)

        # Persistent state (Figure 2).
        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.log = RaftLog()

        # Volatile state on all servers.
        self.commit_index = 0
        self.last_applied = 0
        self.role = Role.FOLLOWER
        self.leader_id: Optional[int] = None

        # Volatile state on candidates.
        self.votes_received: Set[int] = set()

        # Volatile state on leaders (reinitialized on election, Figure 2).
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        # Simulated state machine: entries applied in commit order.
        self.applied_commands: List[Tuple[int, int, Any]] = []  # (index, term, command)

    # -- helpers -----------------------------------------------------

    @property
    def cluster_size(self) -> int:
        return len(self.peer_ids) + 1

    def _quorum(self, count: int) -> bool:
        return count * 2 > self.cluster_size

    def _step_down_to_follower(self, term: int) -> None:
        """Discover a higher term: revert to follower and forget this term's vote."""
        assert term > self.current_term
        self.current_term = term
        self.voted_for = None
        self.role = Role.FOLLOWER
        self.votes_received = set()

    # -- election ------------------------------------------------------

    def start_election(self) -> RequestVoteArgs:
        """Election timeout fired: become a candidate for a new term and vote
        for self. The caller (simulator) is responsible for sending the
        returned RequestVoteArgs to every peer.
        """
        self.current_term += 1
        self.role = Role.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.leader_id = None
        return RequestVoteArgs(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self.log.last_index,
            last_log_term=self.log.last_term(),
        )

    def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        if args.term > self.current_term:
            self._step_down_to_follower(args.term)

        if args.term < self.current_term:
            return RequestVoteReply(
                term=self.current_term, vote_granted=False,
                voter_id=self.node_id, candidate_id=args.candidate_id,
            )

        can_vote = self.voted_for in (None, args.candidate_id)
        log_ok = self.log.candidate_log_is_up_to_date(args.last_log_term, args.last_log_index)
        if can_vote and log_ok:
            self.voted_for = args.candidate_id
            return RequestVoteReply(
                term=self.current_term, vote_granted=True,
                voter_id=self.node_id, candidate_id=args.candidate_id,
            )
        return RequestVoteReply(
            term=self.current_term, vote_granted=False,
            voter_id=self.node_id, candidate_id=args.candidate_id,
        )

    def handle_request_vote_reply(self, reply: RequestVoteReply) -> bool:
        """Returns True iff this reply just made the node become leader."""
        if reply.term > self.current_term:
            self._step_down_to_follower(reply.term)
            return False
        if self.role != Role.CANDIDATE or reply.term != self.current_term:
            return False  # stale reply from an earlier term/election, or already resolved
        if reply.vote_granted:
            self.votes_received.add(reply.voter_id)
            if self._quorum(len(self.votes_received)):
                self._become_leader()
                return True
        return False

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.node_id
        last_index = self.log.last_index
        self.next_index = {pid: last_index + 1 for pid in self.peer_ids}
        self.match_index = {pid: 0 for pid in self.peer_ids}

    # -- log replication -------------------------------------------------

    def client_propose(self, command: Any) -> Optional[LogEntry]:
        """Append a new command to the leader's own log. Returns None if this
        node is not currently the leader. Does NOT replicate by itself --
        the simulator drives replication (see Simulator.propose).
        """
        if self.role != Role.LEADER:
            return None
        return self.log.append(self.current_term, command)

    def make_append_entries_for(self, peer_id: int) -> AppendEntriesArgs:
        """Build the AppendEntries (heartbeat or with entries) this leader
        should currently send to `peer_id`, based on nextIndex[peer_id].
        """
        next_idx = self.next_index[peer_id]
        prev_index = next_idx - 1
        prev_term = self.log.term_at(prev_index)
        entries = self.log.entries_from(next_idx)
        return AppendEntriesArgs(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_index,
            prev_log_term=prev_term,
            entries=entries,
            leader_commit=self.commit_index,
        )

    def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        if args.term > self.current_term:
            self._step_down_to_follower(args.term)

        if args.term < self.current_term:
            return AppendEntriesReply(
                term=self.current_term, success=False,
                follower_id=self.node_id, leader_id=args.leader_id,
            )

        # args.term >= our term and not rejected above => this is a legitimate
        # leader for (at least) the current term. Recognize it and, crucially,
        # step down if we were a candidate (someone else won this term's
        # election) -- Raft 5.2.
        self.role = Role.FOLLOWER
        self.leader_id = args.leader_id

        if args.prev_log_index > 0:
            term_at_prev = self.log.term_at(args.prev_log_index)
            log_missing_or_conflicting = (
                args.prev_log_index > self.log.last_index or term_at_prev != args.prev_log_term
            )
            if log_missing_or_conflicting:
                return AppendEntriesReply(
                    term=self.current_term, success=False,
                    follower_id=self.node_id, leader_id=args.leader_id,
                )

        self._merge_entries(args.prev_log_index, args.entries)

        if args.leader_commit > self.commit_index:
            self.commit_index = min(args.leader_commit, self.log.last_index)

        match_index = args.prev_log_index + len(args.entries)
        return AppendEntriesReply(
            term=self.current_term, success=True,
            follower_id=self.node_id, leader_id=args.leader_id,
            match_index=match_index,
        )

    def _merge_entries(self, prev_log_index: int, entries: List[LogEntry]) -> None:
        """Raft 5.3: append any entries not already in the log; if an
        existing entry conflicts with a new one (same index, different
        term), delete it and everything after it, then append the rest.
        """
        insert_index = prev_log_index + 1
        for offset, entry in enumerate(entries):
            idx = insert_index + offset
            existing = self.log.entry_at(idx)
            if existing is None:
                self.log.append_entries(entries[offset:])
                return
            if existing.term != entry.term:
                self.log.truncate_from(idx)
                self.log.append_entries(entries[offset:])
                return
            # existing.term == entry.term: entries are identical (log index+term
            # uniquely determines the entry, Raft's Log Matching Property) --
            # keep scanning, nothing to do for this position.

    def handle_append_entries_reply(self, reply: AppendEntriesReply) -> None:
        if reply.term > self.current_term:
            self._step_down_to_follower(reply.term)
            return
        if self.role != Role.LEADER or reply.term != self.current_term:
            return
        if reply.success:
            self.match_index[reply.follower_id] = max(
                self.match_index.get(reply.follower_id, 0), reply.match_index
            )
            self.next_index[reply.follower_id] = reply.match_index + 1
            self._advance_commit_index()
        else:
            self.next_index[reply.follower_id] = max(1, self.next_index.get(reply.follower_id, 1) - 1)

    def _advance_commit_index(self) -> None:
        """Raft 5.3/5.4.2: commit index N is advanced to the highest N for
        which a majority of matchIndex[i] >= N AND log[N].term == currentTerm
        (a leader may only directly commit entries from its own term; older
        entries are committed transitively once a later entry commits).
        """
        candidate_match_indices = list(self.match_index.values()) + [self.log.last_index]
        highest_possible = max(candidate_match_indices) if candidate_match_indices else 0
        for n in range(highest_possible, self.commit_index, -1):
            count = sum(1 for m in candidate_match_indices if m >= n)
            if self._quorum(count) and self.log.term_at(n) == self.current_term:
                self.commit_index = n
                break

    # -- state machine -------------------------------------------------

    def apply_committed(self) -> None:
        """Apply any newly-committed entries to the (simulated) state machine,
        in log order. Idempotent -- safe to call after every event.
        """
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.entry_at(self.last_applied)
            assert entry is not None
            self.applied_commands.append((entry.index, entry.term, entry.command))

    def __repr__(self) -> str:
        return (
            f"RaftNode(id={self.node_id}, role={self.role.name}, term={self.current_term}, "
            f"log_len={self.log.last_index}, commit={self.commit_index})"
        )

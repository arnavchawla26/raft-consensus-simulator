"""Simulator: a discrete-event network simulator that hosts a fixed set of
in-process RaftNode instances and drives them with virtual time.

It owns everything RaftNode deliberately doesn't know about:
  * randomized election timeouts and periodic leader heartbeats,
  * message transit delay and message loss,
  * network partitions (cutting arbitrary pairs of links) and node
    crash/restart,
  * a monotonically increasing virtual clock, advanced only by processing
    queued events (so a run is fully reproducible from (seed, script of
    partition/crash/propose calls) regardless of how much real wall-clock
    time it takes).

Nothing here does real I/O or real sleeping -- "network delay" is just a
number of virtual-time units used to order events in a priority queue.
"""

from __future__ import annotations

import heapq
import itertools
import random
from enum import Enum, auto
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Set

from .messages import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
)
from .node import RaftNode, Role


class EventKind(Enum):
    ELECTION_TIMEOUT = auto()
    HEARTBEAT = auto()
    MESSAGE = auto()


class _Event(NamedTuple):
    time: float
    seq: int  # tie-breaker so the heap never has to compare payloads
    kind: EventKind
    node_id: int
    payload: Any
    gen: int  # timer "generation" at schedule time, for timeout-cancellation


class Simulator:
    def __init__(
        self,
        node_ids: Iterable[int],
        seed: int = 0,
        election_timeout_range: "tuple[float, float]" = (150.0, 300.0),
        heartbeat_interval: float = 50.0,
        min_delay: float = 1.0,
        max_delay: float = 10.0,
        drop_rate: float = 0.0,
    ) -> None:
        node_ids = list(node_ids)
        if len(node_ids) < 3:
            raise ValueError("a Raft cluster needs at least 3 nodes to tolerate any failure")
        if len(set(node_ids)) != len(node_ids):
            raise ValueError("node_ids must be unique")

        self.rng = random.Random(seed)
        self.now: float = 0.0
        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.drop_rate = drop_rate

        self.nodes: Dict[int, RaftNode] = {
            nid: RaftNode(nid, [p for p in node_ids if p != nid]) for nid in node_ids
        }
        self.crashed: Set[int] = set()
        self.link_down: Set[frozenset] = set()

        self._heap: List[_Event] = []
        self._seq = itertools.count()
        self._timer_gen: Dict[int, int] = {nid: 0 for nid in node_ids}

        # Observability, used by tests and the CLI -- not needed by the
        # protocol itself.
        self.leader_history: List[tuple] = []  # (term, node_id), append-only
        self.messages_sent = 0
        self.messages_dropped = 0
        self.event_count = 0

        for nid in node_ids:
            self._schedule_election_timeout(nid)

    # -- fault injection controls ---------------------------------------

    def crash(self, node_id: int) -> None:
        self.crashed.add(node_id)

    def restart(self, node_id: int) -> None:
        """Bring a crashed node back. It keeps its persistent state (term,
        voted_for, log) as Raft's crash-recovery model assumes, but resumes
        as a fresh follower with a newly-scheduled election timeout.
        """
        self.crashed.discard(node_id)
        node = self.nodes[node_id]
        node.role = Role.FOLLOWER
        node.leader_id = None
        node.votes_received = set()
        self._schedule_election_timeout(node_id)

    def is_crashed(self, node_id: int) -> bool:
        return node_id in self.crashed

    def partition(self, group_a: Iterable[int], group_b: Iterable[int]) -> None:
        """Cut every link between group_a and group_b (messages either way
        are dropped). Does not affect links within a group. Calling this
        multiple times with different groupings composes (e.g. to build a
        3-way split).
        """
        group_a = list(group_a)
        group_b = list(group_b)
        for a in group_a:
            for b in group_b:
                if a != b:
                    self.link_down.add(frozenset((a, b)))

    def heal_partition(self) -> None:
        self.link_down.clear()

    def _can_deliver(self, src: int, dest: int) -> bool:
        if src in self.crashed or dest in self.crashed:
            return False
        if frozenset((src, dest)) in self.link_down:
            return False
        return True

    # -- client interface -------------------------------------------------

    def find_leader(self) -> Optional[int]:
        for nid, node in self.nodes.items():
            if nid not in self.crashed and node.role == Role.LEADER:
                return nid
        return None

    def propose(self, command: Any, leader_id: Optional[int] = None) -> bool:
        """Submit a command to `leader_id` (or, if omitted, whichever node
        `find_leader()` returns -- ambiguous if the cluster is currently
        partitioned into multiple "leaders" that haven't discovered each
        other's higher term yet, exactly as a real client with no special
        knowledge of the partition would be ambiguous). Returns whether a
        leader was found/specified to accept it, not whether/when it
        commits -- call run_for/run_until and then check commit_index for
        that.
        """
        if leader_id is None:
            leader_id = self.find_leader()
        if leader_id is None or leader_id not in self.nodes:
            return False
        entry = self.nodes[leader_id].client_propose(command)
        if entry is None:
            return False
        self._replicate_to_all(leader_id)
        return True

    # -- event scheduling -------------------------------------------------

    def _push(self, time: float, kind: EventKind, node_id: int, payload: Any = None, gen: int = 0) -> None:
        heapq.heappush(self._heap, _Event(time, next(self._seq), kind, node_id, payload, gen))

    def _bump_gen(self, node_id: int) -> int:
        self._timer_gen[node_id] += 1
        return self._timer_gen[node_id]

    def _schedule_election_timeout(self, node_id: int) -> None:
        gen = self._bump_gen(node_id)
        timeout = self.rng.uniform(*self.election_timeout_range)
        self._push(self.now + timeout, EventKind.ELECTION_TIMEOUT, node_id, gen=gen)

    def _send_message(self, src: int, dest: int, payload: Any) -> None:
        if not self._can_deliver(src, dest):
            self.messages_dropped += 1
            return
        if self.rng.random() < self.drop_rate:
            self.messages_dropped += 1
            return
        self.messages_sent += 1
        delay = self.rng.uniform(self.min_delay, self.max_delay)
        self._push(self.now + delay, EventKind.MESSAGE, dest, payload=payload)

    def _replicate_to_all(self, leader_id: int) -> None:
        node = self.nodes[leader_id]
        if node.role != Role.LEADER or leader_id in self.crashed:
            return
        for peer in node.peer_ids:
            args = node.make_append_entries_for(peer)
            self._send_message(leader_id, peer, args)

    def _send_heartbeats(self, node_id: int, term: int) -> None:
        node = self.nodes.get(node_id)
        if node is None or node_id in self.crashed:
            return
        if node.role != Role.LEADER or node.current_term != term:
            return  # no longer leader for this term -- stop the heartbeat chain
        self._replicate_to_all(node_id)
        self._push(self.now + self.heartbeat_interval, EventKind.HEARTBEAT, node_id, payload=term)

    # -- event processing -------------------------------------------------

    def _process_event(self, event: _Event) -> None:
        self.event_count += 1
        if event.kind == EventKind.ELECTION_TIMEOUT:
            self._on_election_timeout(event)
        elif event.kind == EventKind.HEARTBEAT:
            self._send_heartbeats(event.node_id, event.payload)
        elif event.kind == EventKind.MESSAGE:
            self._on_message(event)

    def _on_election_timeout(self, event: _Event) -> None:
        node_id = event.node_id
        if node_id in self.crashed:
            return
        if event.gen != self._timer_gen[node_id]:
            return  # stale: superseded by a reset (heartbeat/vote/etc.) since scheduling
        node = self.nodes[node_id]
        if node.role == Role.LEADER:
            return  # leaders don't run election timeouts
        args = node.start_election()
        self._schedule_election_timeout(node_id)  # in case this election doesn't resolve
        for peer in node.peer_ids:
            self._send_message(node_id, peer, args)

    def _on_message(self, event: _Event) -> None:
        dest = event.node_id
        if dest in self.crashed:
            return
        node = self.nodes[dest]
        msg = event.payload

        if isinstance(msg, RequestVoteArgs):
            reply = node.handle_request_vote(msg)
            if reply.vote_granted:
                self._schedule_election_timeout(dest)
            self._send_message(dest, msg.candidate_id, reply)

        elif isinstance(msg, RequestVoteReply):
            became_leader = node.handle_request_vote_reply(msg)
            if became_leader:
                self.leader_history.append((node.current_term, dest))
                self._send_heartbeats(dest, node.current_term)

        elif isinstance(msg, AppendEntriesArgs):
            reply = node.handle_append_entries(msg)
            if reply.term <= msg.term:
                # msg.term was not stale -- this is a legitimate leader for
                # (at least) our current term, so reset our election clock.
                self._schedule_election_timeout(dest)
            node.apply_committed()
            self._send_message(dest, msg.leader_id, reply)

        elif isinstance(msg, AppendEntriesReply):
            node.handle_append_entries_reply(msg)
            node.apply_committed()

    # -- driving the clock -------------------------------------------------

    def run_until(self, end_time: float) -> None:
        while self._heap and self._heap[0].time <= end_time:
            event = heapq.heappop(self._heap)
            self.now = event.time
            self._process_event(event)
        self.now = max(self.now, end_time)

    def run_for(self, duration: float) -> None:
        self.run_until(self.now + duration)

    def run_until_stable_leader(self, max_time: float, check_interval: float = 10.0) -> Optional[int]:
        """Advance time until some term has a leader that has held the role
        continuously for at least one `check_interval`, or max_time elapses.
        Returns that leader's node_id, or None on timeout.
        """
        end = self.now + max_time
        last_leader_term = None
        while self.now < end:
            self.run_for(check_interval)
            leader_id = self.find_leader()
            if leader_id is not None:
                term = self.nodes[leader_id].current_term
                if term == last_leader_term:
                    return leader_id
                last_leader_term = term
            else:
                last_leader_term = None
        return self.find_leader()

    # -- introspection used by tests/CLI ------------------------------------

    def commit_indices(self) -> Dict[int, int]:
        return {nid: n.commit_index for nid, n in self.nodes.items()}

    def log_lengths(self) -> Dict[int, int]:
        return {nid: n.log.last_index for nid, n in self.nodes.items()}

    def roles(self) -> Dict[int, str]:
        return {nid: n.role.name for nid, n in self.nodes.items()}

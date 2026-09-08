"""Checks for the safety properties Raft is supposed to guarantee (Figure 3
of the paper), evaluated against the current state of a Simulator. These
are used both by the scenario runner (to report pass/fail) and by the
randomized property tests in tests/test_safety_properties.py, which is
where they actually earn their keep -- running them once at the end of a
single scripted scenario mostly just confirms the happy path; running them
after many seeds of randomized fault injection is what would actually catch
a broken commit-index or log-matching implementation.

  * Election Safety: at most one leader can be elected in a given term.
  * Leader Append-Only: a leader never overwrites or deletes entries in its
    own log; it only appends new ones.
  * Log Matching: if two logs contain an entry with the same index and
    term, then the logs are identical in all entries up through that index.
  * Leader Completeness (checked indirectly, see below): if a log entry is
    committed in a given term, that entry is present in the logs of the
    leaders of all higher-numbered terms.
  * State Machine Safety: if a node has applied a particular log entry at a
    given index, no other node will ever apply a different log entry for
    the same index.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from .cluster import Simulator
from .log import LogEntry


@dataclass
class SafetyReport:
    election_safety_ok: bool = True
    election_safety_violations: List[str] = field(default_factory=list)

    log_matching_ok: bool = True
    log_matching_violations: List[str] = field(default_factory=list)

    committed_never_diverge_ok: bool = True
    committed_never_diverge_violations: List[str] = field(default_factory=list)

    state_machine_safety_ok: bool = True
    state_machine_safety_violations: List[str] = field(default_factory=list)

    @property
    def all_ok(self) -> bool:
        return (
            self.election_safety_ok
            and self.log_matching_ok
            and self.committed_never_diverge_ok
            and self.state_machine_safety_ok
        )

    def summary_lines(self) -> List[str]:
        lines = [
            f"election safety:          {'OK' if self.election_safety_ok else 'VIOLATED'}",
            f"log matching:             {'OK' if self.log_matching_ok else 'VIOLATED'}",
            f"committed entries stable: {'OK' if self.committed_never_diverge_ok else 'VIOLATED'}",
            f"state machine safety:     {'OK' if self.state_machine_safety_ok else 'VIOLATED'}",
        ]
        for bucket in (
            self.election_safety_violations,
            self.log_matching_violations,
            self.committed_never_diverge_violations,
            self.state_machine_safety_violations,
        ):
            lines.extend(f"  ! {v}" for v in bucket)
        return lines


def check_election_safety(leader_history: List[Tuple[int, int]]) -> Tuple[bool, List[str]]:
    """At most one distinct node may ever be recorded as leader for a given term."""
    leaders_by_term: Dict[int, set] = {}
    for term, node_id in leader_history:
        leaders_by_term.setdefault(term, set()).add(node_id)
    violations = [
        f"term {term} had multiple leaders: {sorted(node_ids)}"
        for term, node_ids in leaders_by_term.items()
        if len(node_ids) > 1
    ]
    return (len(violations) == 0, violations)


def check_log_matching(logs: Dict[int, List[LogEntry]]) -> Tuple[bool, List[str]]:
    """Raft's Log Matching Property: if two logs contain an entry with the
    same index and term, the logs are identical in every entry up through
    that index. We check this two ways for each pair of nodes: (a) any
    same-index/same-term pair must have byte-identical content, and (b)
    once two logs' terms diverge at some index, they must never re-agree on
    term at a later index (a later coincidental term match without an
    unbroken consistent prefix would itself be a violation).
    """
    violations: List[str] = []
    node_ids = sorted(logs)
    for a_idx in range(len(node_ids)):
        for b_idx in range(a_idx + 1, len(node_ids)):
            a, b = node_ids[a_idx], node_ids[b_idx]
            log_a, log_b = logs[a], logs[b]
            min_len = min(len(log_a), len(log_b))
            diverged_at: int = None  # type: ignore[assignment]
            for i in range(min_len):
                if log_a[i].term == log_b[i].term:
                    if log_a[i] != log_b[i]:
                        violations.append(
                            f"nodes {a} and {b} both have index {log_a[i].index} at term "
                            f"{log_a[i].term} but different entries: {log_a[i]!r} vs {log_b[i]!r}"
                        )
                    elif diverged_at is not None:
                        violations.append(
                            f"nodes {a} and {b} diverged at index {diverged_at + 1} but "
                            f"re-matched term at index {log_a[i].index} without a consistent "
                            "prefix -- violates the Log Matching Property"
                        )
                else:
                    if diverged_at is None:
                        diverged_at = i
    return (len(violations) == 0, violations)


def check_committed_entries_never_diverge(
    logs: Dict[int, List[LogEntry]], commit_indices: Dict[int, int]
) -> Tuple[bool, List[str]]:
    """Wherever two nodes have both committed index i, the entry at i must
    be identical on both -- this is the practical, checkable form of Leader
    Completeness + State Machine Safety at the log level (as opposed to the
    applied-state-machine level, checked separately below).
    """
    violations: List[str] = []
    node_ids = sorted(logs)
    for a_idx in range(len(node_ids)):
        for b_idx in range(a_idx + 1, len(node_ids)):
            a, b = node_ids[a_idx], node_ids[b_idx]
            common_commit = min(commit_indices.get(a, 0), commit_indices.get(b, 0))
            for i in range(1, common_commit + 1):
                entry_a = logs[a][i - 1] if i - 1 < len(logs[a]) else None
                entry_b = logs[b][i - 1] if i - 1 < len(logs[b]) else None
                if entry_a != entry_b:
                    violations.append(
                        f"nodes {a} and {b} both committed index {i} but disagree: "
                        f"{entry_a!r} vs {entry_b!r}"
                    )
    return (len(violations) == 0, violations)


def check_state_machine_safety(applied: Dict[int, List[Tuple[int, int, object]]]) -> Tuple[bool, List[str]]:
    """Wherever two nodes have both applied a command at index i, it must be
    the same command.
    """
    violations: List[str] = []
    by_index: Dict[int, Dict[int, Tuple[int, object]]] = {}  # index -> node_id -> (term, command)
    for node_id, entries in applied.items():
        for index, term, command in entries:
            by_index.setdefault(index, {})[node_id] = (term, command)
    for index, per_node in by_index.items():
        distinct = set(per_node.values())
        if len(distinct) > 1:
            violations.append(f"index {index} applied differently across nodes: {per_node}")
    return (len(violations) == 0, violations)


def check_all_safety_properties(sim: Simulator) -> SafetyReport:
    report = SafetyReport()

    report.election_safety_ok, report.election_safety_violations = check_election_safety(sim.leader_history)

    logs = {nid: node.log.all_entries() for nid, node in sim.nodes.items()}
    report.log_matching_ok, report.log_matching_violations = check_log_matching(logs)

    commit_indices = sim.commit_indices()
    report.committed_never_diverge_ok, report.committed_never_diverge_violations = (
        check_committed_entries_never_diverge(logs, commit_indices)
    )

    applied = {nid: node.applied_commands for nid, node in sim.nodes.items()}
    report.state_machine_safety_ok, report.state_machine_safety_violations = check_state_machine_safety(applied)

    return report

"""Unit tests for the safety-property checker functions themselves, using
hand-built (good and deliberately corrupted) data. This matters because
test_safety_properties.py only proves the checkers pass on states the real
simulator produces -- it can't tell a checker that's actually broken (e.g.
one that always returns True) from one that works, since a correct
implementation never produces a violation to catch. These tests feed each
checker inputs it MUST flag, so a checker that silently stopped checking
anything would fail loudly here.
"""

from raft.log import LogEntry
from raft.safety import (
    check_committed_entries_never_diverge,
    check_election_safety,
    check_log_matching,
    check_state_machine_safety,
)


def test_election_safety_passes_on_one_leader_per_term():
    ok, violations = check_election_safety([(1, 5), (2, 3), (3, 3)])
    assert ok and violations == []


def test_election_safety_flags_two_leaders_same_term():
    ok, violations = check_election_safety([(1, 5), (1, 3)])
    assert ok is False
    assert len(violations) == 1
    assert "term 1" in violations[0]


def test_log_matching_passes_on_identical_prefixes():
    log_a = [LogEntry(term=1, index=1, command="x"), LogEntry(term=2, index=2, command="y")]
    log_b = [LogEntry(term=1, index=1, command="x"), LogEntry(term=2, index=2, command="y")]
    ok, violations = check_log_matching({1: log_a, 2: log_b})
    assert ok and violations == []


def test_log_matching_flags_same_index_same_term_different_command():
    log_a = [LogEntry(term=1, index=1, command="x")]
    log_b = [LogEntry(term=1, index=1, command="DIFFERENT")]
    ok, violations = check_log_matching({1: log_a, 2: log_b})
    assert ok is False
    assert len(violations) == 1


def test_log_matching_flags_divergence_then_impossible_reconvergence():
    # index 1 disagrees on term (1 vs 2) -- a real divergence point -- but
    # index 2 claims to agree on term again. Real Raft can never produce
    # this (a consistency check would have prevented it), so the checker
    # must flag it as a Log Matching Property violation.
    log_a = [LogEntry(term=1, index=1, command="a"), LogEntry(term=9, index=2, command="shared")]
    log_b = [LogEntry(term=2, index=1, command="b"), LogEntry(term=9, index=2, command="shared")]
    ok, violations = check_log_matching({1: log_a, 2: log_b})
    assert ok is False
    assert any("re-matched" in v for v in violations)


def test_log_matching_allows_normal_divergence_of_uncommitted_suffixes():
    # entries after a term mismatch are simply *absent or different term* on
    # both sides -- not a violation, just an uncommitted fork that hasn't
    # been resolved by AppendEntries yet.
    log_a = [LogEntry(term=1, index=1, command="a"), LogEntry(term=3, index=2, command="x")]
    log_b = [LogEntry(term=1, index=1, command="a"), LogEntry(term=4, index=2, command="y")]
    ok, violations = check_log_matching({1: log_a, 2: log_b})
    assert ok and violations == []


def test_committed_entries_never_diverge_passes_when_equal():
    log_a = [LogEntry(term=1, index=1, command="a"), LogEntry(term=1, index=2, command="b")]
    log_b = [LogEntry(term=1, index=1, command="a"), LogEntry(term=1, index=2, command="b")]
    ok, violations = check_committed_entries_never_diverge({1: log_a, 2: log_b}, {1: 2, 2: 2})
    assert ok and violations == []


def test_committed_entries_never_diverge_flags_mismatch_within_common_commit_range():
    log_a = [LogEntry(term=1, index=1, command="a")]
    log_b = [LogEntry(term=1, index=1, command="CORRUPTED")]
    ok, violations = check_committed_entries_never_diverge({1: log_a, 2: log_b}, {1: 1, 2: 1})
    assert ok is False
    assert len(violations) == 1


def test_committed_entries_never_diverge_ignores_beyond_common_commit_index():
    # node 2 hasn't committed index 2 yet, so a difference there doesn't count
    log_a = [LogEntry(term=1, index=1, command="a"), LogEntry(term=1, index=2, command="b")]
    log_b = [LogEntry(term=1, index=1, command="a"), LogEntry(term=1, index=2, command="DIFFERENT-UNCOMMITTED")]
    ok, violations = check_committed_entries_never_diverge({1: log_a, 2: log_b}, {1: 2, 2: 1})
    assert ok and violations == []


def test_state_machine_safety_passes_when_consistent():
    applied = {1: [(1, 1, "a"), (2, 1, "b")], 2: [(1, 1, "a")]}
    ok, violations = check_state_machine_safety(applied)
    assert ok and violations == []


def test_state_machine_safety_flags_divergent_application():
    applied = {1: [(1, 1, "a")], 2: [(1, 1, "DIFFERENT")]}
    ok, violations = check_state_machine_safety(applied)
    assert ok is False
    assert len(violations) == 1


def test_state_machine_safety_ignores_indices_only_one_node_has_applied():
    applied = {1: [(1, 1, "a"), (2, 1, "b")], 2: [(1, 1, "a")]}
    ok, violations = check_state_machine_safety(applied)
    assert ok  # node 2 just hasn't applied index 2 yet -- not a conflict

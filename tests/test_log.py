from raft.log import RaftLog


def test_empty_log():
    log = RaftLog()
    assert len(log) == 0
    assert log.last_index == 0
    assert log.last_term() == 0
    assert log.term_at(0) == 0
    assert log.entry_at(1) is None
    assert log.entries_from(1) == []


def test_append_assigns_sequential_indices():
    log = RaftLog()
    e1 = log.append(term=1, command="a")
    e2 = log.append(term=1, command="b")
    e3 = log.append(term=2, command="c")
    assert (e1.index, e2.index, e3.index) == (1, 2, 3)
    assert log.last_index == 3
    assert log.last_term() == 2


def test_term_at_out_of_range_is_zero():
    log = RaftLog()
    log.append(term=5, command="a")
    assert log.term_at(0) == 0
    assert log.term_at(1) == 5
    assert log.term_at(2) == 0  # past the end
    assert log.term_at(-1) == 0


def test_entries_from():
    log = RaftLog()
    for i in range(5):
        log.append(term=1, command=i)
    assert [e.command for e in log.entries_from(3)] == [2, 3, 4]
    assert [e.command for e in log.entries_from(1)] == [0, 1, 2, 3, 4]
    assert log.entries_from(6) == []
    assert [e.command for e in log.entries_from(0)] == [0, 1, 2, 3, 4]  # clamps to 1


def test_truncate_from():
    log = RaftLog()
    for i in range(5):
        log.append(term=1, command=i)
    log.truncate_from(3)
    assert log.last_index == 2
    assert [e.command for e in log.all_entries()] == [0, 1]


def test_truncate_from_zero_clears_everything():
    log = RaftLog()
    log.append(term=1, command="a")
    log.truncate_from(0)
    assert len(log) == 0


def test_append_entries_batch():
    log = RaftLog()
    from raft.log import LogEntry

    log.append_entries([LogEntry(term=1, index=1, command="a"), LogEntry(term=1, index=2, command="b")])
    assert log.last_index == 2
    assert log.entry_at(2).command == "b"


def test_candidate_log_up_to_date_by_term():
    log = RaftLog()
    log.append(term=3, command="a")
    # candidate with a strictly higher last term wins regardless of index
    assert log.candidate_log_is_up_to_date(candidate_last_term=4, candidate_last_index=0) is True
    # candidate with a strictly lower last term loses regardless of index
    assert log.candidate_log_is_up_to_date(candidate_last_term=2, candidate_last_index=100) is False


def test_candidate_log_up_to_date_by_index_when_terms_tie():
    log = RaftLog()
    log.append(term=3, command="a")
    log.append(term=3, command="b")  # last_index=2, last_term=3
    assert log.candidate_log_is_up_to_date(candidate_last_term=3, candidate_last_index=2) is True  # equal
    assert log.candidate_log_is_up_to_date(candidate_last_term=3, candidate_last_index=3) is True  # ahead
    assert log.candidate_log_is_up_to_date(candidate_last_term=3, candidate_last_index=1) is False  # behind


def test_candidate_log_up_to_date_against_empty_voter_log():
    log = RaftLog()
    assert log.candidate_log_is_up_to_date(candidate_last_term=0, candidate_last_index=0) is True
    assert log.candidate_log_is_up_to_date(candidate_last_term=1, candidate_last_index=1) is True


def test_log_equality_and_repr():
    a, b = RaftLog(), RaftLog()
    a.append(term=1, command="x")
    b.append(term=1, command="x")
    assert a == b
    b.append(term=1, command="y")
    assert a != b
    assert "RaftLog(" in repr(a)

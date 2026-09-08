from raft.log import LogEntry
from raft.messages import AppendEntriesArgs, AppendEntriesReply
from raft.node import RaftNode, Role


def make_node(node_id=1, peers=(2, 3, 4, 5)):
    return RaftNode(node_id, list(peers))


def test_heartbeat_on_empty_log_succeeds():
    node = make_node(1)
    args = AppendEntriesArgs(term=1, leader_id=2, prev_log_index=0, prev_log_term=0, entries=[], leader_commit=0)
    reply = node.handle_append_entries(args)
    assert reply.success is True
    assert reply.term == 1
    assert node.leader_id == 2
    assert node.role == Role.FOLLOWER


def test_rejects_append_entries_with_stale_term():
    node = make_node(1)
    node.current_term = 5
    args = AppendEntriesArgs(term=2, leader_id=2, prev_log_index=0, prev_log_term=0)
    reply = node.handle_append_entries(args)
    assert reply.success is False
    assert reply.term == 5


def test_steps_up_to_higher_term_and_recognizes_new_leader():
    node = make_node(1)
    node.current_term = 2
    args = AppendEntriesArgs(term=5, leader_id=9, prev_log_index=0, prev_log_term=0)
    reply = node.handle_append_entries(args)
    assert reply.success is True
    assert node.current_term == 5
    assert node.leader_id == 9


def test_candidate_steps_down_on_append_entries_same_term():
    node = make_node(1)
    node.start_election()  # term=1, role=CANDIDATE
    args = AppendEntriesArgs(term=1, leader_id=2, prev_log_index=0, prev_log_term=0)
    reply = node.handle_append_entries(args)
    assert reply.success is True
    assert node.role == Role.FOLLOWER
    assert node.leader_id == 2


def test_rejects_when_prev_log_index_beyond_end_of_log():
    node = make_node(1)
    node.log.append(term=1, command="a")
    args = AppendEntriesArgs(term=1, leader_id=2, prev_log_index=5, prev_log_term=1)
    reply = node.handle_append_entries(args)
    assert reply.success is False


def test_rejects_when_prev_log_term_mismatches():
    node = make_node(1)
    node.log.append(term=1, command="a")
    args = AppendEntriesArgs(term=2, leader_id=2, prev_log_index=1, prev_log_term=99)
    reply = node.handle_append_entries(args)
    assert reply.success is False


def test_appends_new_entries_to_empty_log():
    node = make_node(1)
    entries = [LogEntry(term=1, index=1, command="a"), LogEntry(term=1, index=2, command="b")]
    args = AppendEntriesArgs(term=1, leader_id=2, prev_log_index=0, prev_log_term=0, entries=entries)
    reply = node.handle_append_entries(args)
    assert reply.success is True
    assert reply.match_index == 2
    assert [e.command for e in node.log.all_entries()] == ["a", "b"]


def test_truncates_conflicting_suffix_and_appends_new_entries():
    node = make_node(1)
    node.log.append(term=1, command="a")
    node.log.append(term=1, command="stale-b")
    node.log.append(term=1, command="stale-c")
    new_entries = [LogEntry(term=2, index=2, command="fresh-b"), LogEntry(term=2, index=3, command="fresh-c")]
    args = AppendEntriesArgs(term=2, leader_id=2, prev_log_index=1, prev_log_term=1, entries=new_entries)
    reply = node.handle_append_entries(args)
    assert reply.success is True
    assert [e.command for e in node.log.all_entries()] == ["a", "fresh-b", "fresh-c"]


def test_does_not_truncate_when_entries_already_match():
    node = make_node(1)
    node.log.append(term=1, command="a")
    node.log.append(term=2, command="b")
    # re-sending the very same entry (retransmit) must be a no-op, not a truncate+reappend
    entries = [LogEntry(term=2, index=2, command="b")]
    args = AppendEntriesArgs(term=2, leader_id=2, prev_log_index=1, prev_log_term=1, entries=entries)
    reply = node.handle_append_entries(args)
    assert reply.success is True
    assert [e.command for e in node.log.all_entries()] == ["a", "b"]


def test_partial_overlap_appends_only_the_missing_tail():
    node = make_node(1)
    node.log.append(term=1, command="a")  # follower already has index 1
    entries = [
        LogEntry(term=1, index=1, command="a"),
        LogEntry(term=1, index=2, command="b"),
        LogEntry(term=1, index=3, command="c"),
    ]
    args = AppendEntriesArgs(term=1, leader_id=2, prev_log_index=0, prev_log_term=0, entries=entries)
    reply = node.handle_append_entries(args)
    assert reply.success is True
    assert [e.command for e in node.log.all_entries()] == ["a", "b", "c"]


def test_leader_commit_advances_but_never_past_local_log():
    node = make_node(1)
    node.log.append(term=1, command="a")
    node.log.append(term=1, command="b")
    args = AppendEntriesArgs(term=1, leader_id=2, prev_log_index=2, prev_log_term=1, entries=[], leader_commit=10)
    reply = node.handle_append_entries(args)
    assert reply.success is True
    assert node.commit_index == 2  # clamped to log length, not 10


def test_commit_index_never_decreases():
    node = make_node(1)
    node.log.append(term=1, command="a")
    node.commit_index = 1
    args = AppendEntriesArgs(term=1, leader_id=2, prev_log_index=1, prev_log_term=1, entries=[], leader_commit=0)
    node.handle_append_entries(args)
    assert node.commit_index == 1


def test_leader_handles_successful_reply_advances_match_and_next_index():
    leader = make_node(1, peers=(2, 3, 4))
    for cmd in ("a", "b", "c"):
        leader.log.append(term=1, command=cmd)
    leader.current_term = 1
    leader.role = Role.LEADER
    leader.next_index = {2: 4, 3: 4, 4: 4}
    leader.match_index = {2: 0, 3: 0, 4: 0}
    reply = AppendEntriesReply(term=1, success=True, follower_id=2, leader_id=1, match_index=3)
    leader.handle_append_entries_reply(reply)
    assert leader.match_index[2] == 3
    assert leader.next_index[2] == 4


def test_leader_handles_failed_reply_decrements_next_index_but_not_below_one():
    leader = make_node(1, peers=(2,))
    leader.current_term = 1
    leader.role = Role.LEADER
    leader.next_index = {2: 1}
    leader.match_index = {2: 0}
    reply = AppendEntriesReply(term=1, success=False, follower_id=2, leader_id=1)
    leader.handle_append_entries_reply(reply)
    assert leader.next_index[2] == 1  # floor of 1, never 0 or negative


def test_leader_steps_down_on_higher_term_reply():
    leader = make_node(1, peers=(2,))
    leader.current_term = 1
    leader.role = Role.LEADER
    reply = AppendEntriesReply(term=9, success=False, follower_id=2, leader_id=1)
    leader.handle_append_entries_reply(reply)
    assert leader.role == Role.FOLLOWER
    assert leader.current_term == 9


def test_commit_index_advances_only_with_majority_match_and_current_term():
    leader = make_node(1, peers=(2, 3, 4, 5))
    leader.current_term = 2
    leader.role = Role.LEADER
    for cmd in ("a", "b", "c"):
        leader.log.append(term=2, command=cmd)
    leader.next_index = {p: 4 for p in (2, 3, 4, 5)}
    leader.match_index = {p: 0 for p in (2, 3, 4, 5)}

    # only one follower acked index 3 so far -- no majority (2 of 5) yet
    leader.handle_append_entries_reply(AppendEntriesReply(term=2, success=True, follower_id=2, leader_id=1, match_index=3))
    assert leader.commit_index == 0

    # a second follower acks -- now 3 of 5 (leader + 2 followers) have index 3: majority
    leader.handle_append_entries_reply(AppendEntriesReply(term=2, success=True, follower_id=3, leader_id=1, match_index=3))
    assert leader.commit_index == 3


def test_leader_cannot_directly_commit_an_entry_from_an_earlier_term():
    leader = make_node(1, peers=(2, 3, 4))
    leader.log.append(term=1, command="stale")  # index 1, term 1 (from a previous leader)
    leader.current_term = 5  # this leader's term
    leader.role = Role.LEADER
    leader.next_index = {2: 2, 3: 2, 4: 2}
    leader.match_index = {2: 0, 3: 0, 4: 0}

    leader.handle_append_entries_reply(AppendEntriesReply(term=5, success=True, follower_id=2, leader_id=1, match_index=1))
    leader.handle_append_entries_reply(AppendEntriesReply(term=5, success=True, follower_id=3, leader_id=1, match_index=1))
    # 3 of 4 nodes have index 1 -- majority, but log[1].term (1) != currentTerm (5): must NOT commit
    assert leader.commit_index == 0

    # once the leader appends (and replicates) something from its own term
    # and a majority (leader + 2 of 3 followers here) has it, both entries
    # commit together transitively.
    leader.log.append(term=5, command="fresh")
    leader.handle_append_entries_reply(AppendEntriesReply(term=5, success=True, follower_id=2, leader_id=1, match_index=2))
    leader.handle_append_entries_reply(AppendEntriesReply(term=5, success=True, follower_id=3, leader_id=1, match_index=2))
    assert leader.commit_index == 2


def test_apply_committed_applies_in_order_and_is_idempotent():
    node = make_node(1)
    for cmd in ("a", "b", "c"):
        node.log.append(term=1, command=cmd)
    node.commit_index = 2
    node.apply_committed()
    assert node.applied_commands == [(1, 1, "a"), (2, 1, "b")]
    node.apply_committed()  # no-op, nothing new committed
    assert node.applied_commands == [(1, 1, "a"), (2, 1, "b")]
    node.commit_index = 3
    node.apply_committed()
    assert node.applied_commands == [(1, 1, "a"), (2, 1, "b"), (3, 1, "c")]


def test_non_leader_client_propose_returns_none():
    node = make_node(1)
    assert node.client_propose("x") is None


def test_leader_client_propose_appends_at_current_term():
    node = make_node(1)
    node.current_term = 3
    node.role = Role.LEADER
    entry = node.client_propose("x")
    assert entry.term == 3
    assert entry.index == 1


def test_make_append_entries_for_peer_reflects_next_index():
    leader = make_node(1, peers=(2,))
    for cmd in ("a", "b", "c"):
        leader.log.append(term=1, command=cmd)
    leader.current_term = 1
    leader.role = Role.LEADER
    leader.next_index = {2: 2}
    args = leader.make_append_entries_for(2)
    assert args.prev_log_index == 1
    assert args.prev_log_term == 1
    assert [e.command for e in args.entries] == ["b", "c"]


def test_make_append_entries_is_heartbeat_when_follower_is_caught_up():
    leader = make_node(1, peers=(2,))
    leader.log.append(term=1, command="a")
    leader.current_term = 1
    leader.role = Role.LEADER
    leader.next_index = {2: 2}  # follower already has index 1
    args = leader.make_append_entries_for(2)
    assert args.is_heartbeat is True

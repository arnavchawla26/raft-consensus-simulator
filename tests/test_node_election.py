from raft.messages import RequestVoteArgs, RequestVoteReply
from raft.node import RaftNode, Role


def make_node(node_id=1, peers=(2, 3, 4, 5)):
    return RaftNode(node_id, list(peers))


def test_start_election_increments_term_and_votes_for_self():
    node = make_node(1)
    args = node.start_election()
    assert node.role == Role.CANDIDATE
    assert node.current_term == 1
    assert node.voted_for == 1
    assert node.votes_received == {1}
    assert args.term == 1
    assert args.candidate_id == 1
    assert args.last_log_index == 0
    assert args.last_log_term == 0


def test_grants_vote_when_eligible():
    node = make_node(1)
    args = RequestVoteArgs(term=1, candidate_id=2, last_log_index=0, last_log_term=0)
    reply = node.handle_request_vote(args)
    assert reply.vote_granted is True
    assert reply.term == 1
    assert node.voted_for == 2
    assert node.current_term == 1


def test_denies_vote_if_already_voted_for_someone_else_this_term():
    node = make_node(1)
    node.handle_request_vote(RequestVoteArgs(term=1, candidate_id=2, last_log_index=0, last_log_term=0))
    reply = node.handle_request_vote(RequestVoteArgs(term=1, candidate_id=3, last_log_index=0, last_log_term=0))
    assert reply.vote_granted is False
    assert node.voted_for == 2  # unchanged


def test_grants_vote_again_to_same_candidate_same_term_is_idempotent():
    node = make_node(1)
    r1 = node.handle_request_vote(RequestVoteArgs(term=1, candidate_id=2, last_log_index=0, last_log_term=0))
    r2 = node.handle_request_vote(RequestVoteArgs(term=1, candidate_id=2, last_log_index=0, last_log_term=0))
    assert r1.vote_granted and r2.vote_granted


def test_denies_vote_for_stale_term():
    node = make_node(1)
    node.current_term = 5
    reply = node.handle_request_vote(RequestVoteArgs(term=3, candidate_id=2, last_log_index=0, last_log_term=0))
    assert reply.vote_granted is False
    assert reply.term == 5


def test_denies_vote_when_candidate_log_is_behind():
    node = make_node(1)
    node.log.append(term=1, command="a")
    node.log.append(term=2, command="b")  # voter's log: last_term=2, last_index=2
    reply = node.handle_request_vote(RequestVoteArgs(term=3, candidate_id=2, last_log_index=1, last_log_term=1))
    assert reply.vote_granted is False


def test_steps_down_and_resets_vote_on_higher_term_request_vote():
    node = make_node(1)
    node.current_term = 2
    node.voted_for = 1
    node.role = Role.CANDIDATE
    reply = node.handle_request_vote(RequestVoteArgs(term=5, candidate_id=2, last_log_index=0, last_log_term=0))
    assert node.current_term == 5
    assert node.role == Role.FOLLOWER
    assert reply.vote_granted is True
    assert node.voted_for == 2


def test_candidate_becomes_leader_on_majority_votes():
    node = make_node(1, peers=(2, 3, 4, 5))
    node.start_election()
    assert node.handle_request_vote_reply(RequestVoteReply(term=1, vote_granted=True, voter_id=2, candidate_id=1)) is False
    became_leader = node.handle_request_vote_reply(RequestVoteReply(term=1, vote_granted=True, voter_id=3, candidate_id=1))
    assert became_leader is True  # 3 of 5 votes (self + 2 + 3) is a majority
    assert node.role == Role.LEADER
    assert node.leader_id == 1


def test_leader_election_reinitializes_next_and_match_index():
    node = make_node(1, peers=(2, 3))
    node.log.append(term=1, command="a")
    node.log.append(term=1, command="b")
    node.start_election()
    node.handle_request_vote_reply(RequestVoteReply(term=1, vote_granted=True, voter_id=2, candidate_id=1))
    assert node.role == Role.LEADER
    assert node.next_index == {2: 3, 3: 3}  # last_index + 1
    assert node.match_index == {2: 0, 3: 0}


def test_duplicate_or_stale_votes_do_not_double_count_or_resurrect_leadership():
    node = make_node(1, peers=(2, 3, 4, 5))
    node.start_election()
    node.handle_request_vote_reply(RequestVoteReply(term=1, vote_granted=True, voter_id=2, candidate_id=1))
    node.handle_request_vote_reply(RequestVoteReply(term=1, vote_granted=True, voter_id=3, candidate_id=1))
    assert node.role == Role.LEADER
    # a stale reply from a *previous* election term must not affect a leader
    became_leader_again = node.handle_request_vote_reply(
        RequestVoteReply(term=1, vote_granted=True, voter_id=4, candidate_id=1)
    )
    assert became_leader_again is False
    assert node.role == Role.LEADER


def test_vote_reply_with_higher_term_steps_down_candidate():
    node = make_node(1, peers=(2, 3, 4, 5))
    node.start_election()  # term=1
    became_leader = node.handle_request_vote_reply(
        RequestVoteReply(term=7, vote_granted=False, voter_id=2, candidate_id=1)
    )
    assert became_leader is False
    assert node.role == Role.FOLLOWER
    assert node.current_term == 7
    assert node.voted_for is None


def test_non_candidate_ignores_vote_reply():
    node = make_node(1)
    reply = RequestVoteReply(term=1, vote_granted=True, voter_id=2, candidate_id=1)
    became_leader = node.handle_request_vote_reply(reply)  # still a follower, never started an election
    assert became_leader is False
    assert node.role == Role.FOLLOWER


def test_three_node_cluster_majority_is_two():
    node = make_node(1, peers=(2, 3))
    node.start_election()
    became_leader = node.handle_request_vote_reply(RequestVoteReply(term=1, vote_granted=True, voter_id=2, candidate_id=1))
    assert became_leader is True  # self + 1 = 2 of 3, a majority

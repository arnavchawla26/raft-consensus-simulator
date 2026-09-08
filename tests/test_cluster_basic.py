import pytest

from raft.cluster import Simulator
from raft.node import Role


def test_rejects_clusters_smaller_than_three():
    with pytest.raises(ValueError):
        Simulator(node_ids=[1, 2])


def test_rejects_duplicate_node_ids():
    with pytest.raises(ValueError):
        Simulator(node_ids=[1, 1, 2])


def test_elects_exactly_one_leader():
    sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=0)
    sim.run_until_stable_leader(max_time=5000)
    leaders = [nid for nid, n in sim.nodes.items() if n.role == Role.LEADER]
    assert len(leaders) == 1


def test_proposal_replicates_and_commits_on_all_nodes():
    sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=1)
    sim.run_until_stable_leader(max_time=5000)
    for i in range(5):
        assert sim.propose(f"cmd{i}") is True
        sim.run_for(150)
    sim.run_for(500)
    commits = sim.commit_indices()
    assert all(c == 5 for c in commits.values())
    for node in sim.nodes.values():
        assert [c for _, _, c in node.applied_commands] == [f"cmd{i}" for i in range(5)]


def test_propose_with_no_leader_returns_false():
    sim = Simulator(node_ids=[1, 2, 3])
    # before any election completes there may be no leader yet
    if sim.find_leader() is None:
        assert sim.propose("x") is False


def test_propose_to_explicit_non_leader_is_rejected():
    sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=2)
    sim.run_until_stable_leader(max_time=5000)
    leader = sim.find_leader()
    follower = next(nid for nid in sim.nodes if nid != leader)
    assert sim.propose("x", leader_id=follower) is False


def test_crash_removes_node_from_find_leader():
    sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=3)
    sim.run_until_stable_leader(max_time=5000)
    leader = sim.find_leader()
    sim.crash(leader)
    assert sim.find_leader() is None or sim.find_leader() != leader


def test_crashed_node_drops_all_messages():
    sim = Simulator(node_ids=[1, 2, 3], seed=4)
    sim.crash(2)
    sim.run_for(2000)
    # node 2 must never have become a candidate/leader or changed term while crashed
    assert sim.nodes[2].current_term == 0
    assert sim.nodes[2].role == Role.FOLLOWER


def test_restart_resumes_participation():
    sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=5)
    sim.run_until_stable_leader(max_time=5000)
    victim = next(nid for nid in sim.nodes if nid != sim.find_leader())
    sim.crash(victim)
    sim.run_for(1000)
    sim.restart(victim)
    sim.propose("after-restart")
    sim.run_for(2000)
    assert sim.nodes[victim].commit_index >= 1


def test_partition_blocks_cross_group_messages():
    sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=6)
    sim.partition([1, 2], [3, 4, 5])
    assert sim._can_deliver(1, 2) is True
    assert sim._can_deliver(3, 4) is True
    assert sim._can_deliver(1, 3) is False
    assert sim._can_deliver(2, 5) is False


def test_heal_partition_restores_delivery():
    sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=7)
    sim.partition([1], [2, 3, 4, 5])
    sim.heal_partition()
    assert sim._can_deliver(1, 3) is True


def test_minority_partition_leader_cannot_commit():
    sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=8)
    leader = sim.run_until_stable_leader(max_time=5000)
    minority, majority = [leader], [n for n in sim.nodes if n != leader]
    sim.partition(minority, majority)
    sim.propose("x", leader_id=leader)
    sim.run_for(3000)
    assert sim.nodes[leader].commit_index == 0


def test_majority_partition_can_still_elect_and_commit():
    sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=9)
    leader = sim.run_until_stable_leader(max_time=5000)
    minority, majority = [leader], [n for n in sim.nodes if n != leader]
    sim.partition(minority, majority)
    sim.run_for(3000)
    new_leader = next((n for n in majority if sim.nodes[n].role == Role.LEADER), None)
    assert new_leader is not None
    assert sim.propose("y", leader_id=new_leader) is True
    sim.run_for(1000)
    assert sim.nodes[new_leader].commit_index >= 1


def test_run_for_advances_clock_monotonically():
    sim = Simulator(node_ids=[1, 2, 3])
    t0 = sim.now
    sim.run_for(1000)
    assert sim.now >= t0 + 1000 - 1e-9


def test_run_until_stable_leader_returns_none_on_timeout_with_absurd_config():
    # every message dropped -> no election can ever complete
    sim = Simulator(node_ids=[1, 2, 3], seed=10, drop_rate=1.0)
    leader = sim.run_until_stable_leader(max_time=2000, check_interval=200)
    assert leader is None


def test_deterministic_given_same_seed():
    def run(seed):
        sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=seed)
        sim.run_until_stable_leader(max_time=5000)
        for i in range(3):
            sim.propose(f"cmd{i}")
            sim.run_for(200)
        return sim.leader_history, sim.commit_indices(), sim.log_lengths()

    a = run(123)
    b = run(123)
    assert a == b


def test_different_seeds_can_elect_different_leaders():
    leaders = set()
    for seed in range(15):
        sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=seed)
        leader = sim.run_until_stable_leader(max_time=5000)
        leaders.add(leader)
    # with randomized timeouts across enough seeds, more than one node should
    # win at least once (this is a sanity check on the RNG wiring, not a
    # hard protocol guarantee)
    assert len(leaders) > 1

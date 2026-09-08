import pytest

from raft.scenarios import SCENARIOS, run_scenario


@pytest.mark.parametrize("name", sorted(SCENARIOS))
@pytest.mark.parametrize("seed", [0, 1, 2, 3, 4])
def test_every_scenario_passes_safety_across_a_few_seeds(name, seed):
    result = run_scenario(name, seed=seed)
    assert result.safety.all_ok, result.safety.summary_lines()


def test_basic_election_elects_a_leader():
    result = run_scenario("basic-election", seed=0)
    assert "LEADER" in result.final_roles.values()


def test_leader_failure_reelection_uses_two_distinct_terms():
    result = run_scenario("leader-failure-reelection", seed=0)
    terms = [term for term, _ in result.leader_history]
    assert len(terms) >= 2
    assert terms == sorted(terms)  # terms are strictly increasing over time
    assert len(set(terms)) == len(terms)


def test_leader_failure_reelection_commits_progress_after_crash():
    result = run_scenario("leader-failure-reelection", seed=0)
    assert all(c >= 4 for c in result.final_commit_indices.values())


def test_network_partition_minority_leader_never_commits_during_split():
    # The scenario isolates the *initial* leader as a lone-node minority and
    # lets the remaining majority elect a new leader and keep committing --
    # so during the split we expect the majority's commit index to advance
    # while the isolated old leader's stays at 0, not that nothing commits
    # anywhere (that would defeat the point of a *majority* partition).
    import ast

    result = run_scenario("network-partition-heal", seed=0)
    initial_leader = result.leader_history[0][1]
    during_split_lines = [l for l in result.narrative if l.startswith("commits during partition")]
    assert during_split_lines
    commits_during_split = ast.literal_eval(during_split_lines[0].split(": ", 1)[1])
    assert commits_during_split[initial_leader] == 0
    assert any(v > 0 for nid, v in commits_during_split.items() if nid != initial_leader)


def test_network_partition_converges_after_heal():
    result = run_scenario("network-partition-heal", seed=0)
    assert len(set(result.final_log_lengths.values())) == 1
    leaders = [nid for nid, role in result.final_roles.items() if role == "LEADER"]
    assert len(leaders) == 1


def test_slow_follower_eventually_fully_catches_up():
    result = run_scenario("slow-follower-catchup", seed=0)
    assert len(set(result.final_log_lengths.values())) == 1
    assert all(c == list(result.final_commit_indices.values())[0] for c in result.final_commit_indices.values())


def test_full_split_makes_no_progress_during_the_split():
    import ast

    result = run_scenario("full-split-no-progress", seed=0)
    during_split_lines = [l for l in result.narrative if l.startswith("commit indices during split")]
    assert during_split_lines
    commits_during_split = ast.literal_eval(during_split_lines[0].split(": ", 1)[1])
    assert all(v == 0 for v in commits_during_split.values())


def test_full_split_eventually_commits_after_heal_and_one_more_write():
    result = run_scenario("full-split-no-progress", seed=0)
    assert all(c >= 1 for c in result.final_commit_indices.values())


def test_run_scenario_rejects_unknown_name():
    with pytest.raises(KeyError):
        run_scenario("not-a-real-scenario")


def test_scenarios_respect_n_nodes_override():
    result = run_scenario("basic-election", seed=0, n_nodes=7)
    assert len(result.node_ids) == 7
    assert len(result.final_roles) == 7

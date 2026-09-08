"""The real bug-catchers: randomized fault injection across many seeds,
checking Raft's safety properties after (and periodically during) each run.
Unlike the scripted scenario tests, these don't assert a particular
narrative -- they throw a random mix of proposals/partitions/crashes/
restarts at the cluster and then demand that the four safety properties
(raft.safety) never break, regardless of what chaos occurred.
"""

import random

import pytest

from raft.cluster import Simulator
from raft.safety import check_all_safety_properties

N_RANDOM_SEEDS = 40


def _random_fault_injection_run(seed: int, n_nodes: int = 5, steps: int = 60):
    rng = random.Random(seed + 1_000_000)  # separate stream from the simulator's own RNG
    node_ids = list(range(1, n_nodes + 1))
    sim = Simulator(node_ids, seed=seed, drop_rate=0.1)

    command_counter = 0
    for _ in range(steps):
        action = rng.choice(["run", "run", "run", "propose", "partition", "heal", "crash", "restart"])
        if action == "run":
            sim.run_for(rng.uniform(20, 150))
        elif action == "propose":
            sim.propose(f"cmd{command_counter}")
            command_counter += 1
        elif action == "partition":
            group_a = rng.sample(node_ids, k=rng.randint(1, n_nodes - 1))
            group_b = [n for n in node_ids if n not in group_a]
            sim.partition(group_a, group_b)
        elif action == "heal":
            sim.heal_partition()
        elif action == "crash":
            candidates = [n for n in node_ids if n not in sim.crashed]
            if candidates:
                sim.crash(rng.choice(candidates))
        elif action == "restart":
            if sim.crashed:
                sim.restart(rng.choice(sorted(sim.crashed)))

        # Check safety properties after every step, not just at the end --
        # a bug that produces a transient violation which later "heals" (for
        # example a leader wrongly rolling back a commit) would be invisible
        # to an end-of-run-only check.
        report = check_all_safety_properties(sim)
        assert report.all_ok, (
            f"seed={seed} step={_} action={action} violated safety: {report.summary_lines()}"
        )

    # finish by healing everything and letting the cluster settle, then check once more
    sim.heal_partition()
    for crashed_id in list(sim.crashed):
        sim.restart(crashed_id)
    sim.run_for(5000)
    return sim


@pytest.mark.parametrize("seed", range(N_RANDOM_SEEDS))
def test_safety_properties_hold_under_random_fault_injection(seed):
    sim = _random_fault_injection_run(seed)
    report = check_all_safety_properties(sim)
    assert report.all_ok, report.summary_lines()


def test_settled_cluster_eventually_converges_logs_after_chaos():
    """A softer liveness-flavored check on top of the safety checks: after
    healing every partition/crash and giving the cluster plenty of time, all
    nodes' logs should converge to the same length and content (not just
    "no divergence found in the committed prefix").
    """
    sim = _random_fault_injection_run(seed=99, steps=40)
    log_lengths = {nid: n.log.last_index for nid, n in sim.nodes.items()}
    assert len(set(log_lengths.values())) == 1, f"logs did not converge: {log_lengths}"
    logs = [tuple(n.log.all_entries()) for n in sim.nodes.values()]
    assert len(set(logs)) == 1, "converged-length logs still differ in content"


def test_only_one_leader_can_ever_be_recorded_per_term_across_many_seeds():
    for seed in range(N_RANDOM_SEEDS):
        sim = _random_fault_injection_run(seed, steps=25)
        leaders_by_term = {}
        for term, node_id in sim.leader_history:
            leaders_by_term.setdefault(term, set()).add(node_id)
        for term, ids in leaders_by_term.items():
            assert len(ids) == 1, f"seed={seed} term={term} had multiple leaders: {ids}"

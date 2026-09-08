"""Named, reproducible scenarios exercising the simulator's fault-injection
knobs. Each scenario builds a Simulator, drives it through a script of
proposals/partitions/crashes, and returns a ScenarioResult summarizing what
happened plus the safety-property checks (see raft.safety) evaluated
against the final state. The CLI's `run` subcommand wraps these.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .cluster import Simulator
from .safety import SafetyReport, check_all_safety_properties


@dataclass
class ScenarioResult:
    name: str
    seed: int
    node_ids: List[int]
    narrative: List[str] = field(default_factory=list)
    final_roles: Dict[int, str] = field(default_factory=dict)
    final_commit_indices: Dict[int, int] = field(default_factory=dict)
    final_log_lengths: Dict[int, int] = field(default_factory=dict)
    leader_history: List[tuple] = field(default_factory=list)
    messages_sent: int = 0
    messages_dropped: int = 0
    safety: Optional[SafetyReport] = None

    def finalize(self, sim: Simulator) -> "ScenarioResult":
        self.final_roles = sim.roles()
        self.final_commit_indices = sim.commit_indices()
        self.final_log_lengths = sim.log_lengths()
        self.leader_history = list(sim.leader_history)
        self.messages_sent = sim.messages_sent
        self.messages_dropped = sim.messages_dropped
        self.safety = check_all_safety_properties(sim)
        return self


def _default_nodes(n: int) -> List[int]:
    return list(range(1, n + 1))


def basic_election(seed: int = 0, n_nodes: int = 5) -> ScenarioResult:
    """A healthy cluster should converge on exactly one leader per term."""
    node_ids = _default_nodes(n_nodes)
    sim = Simulator(node_ids, seed=seed)
    result = ScenarioResult(name="basic_election", seed=seed, node_ids=node_ids)

    leader = sim.run_until_stable_leader(max_time=5000)
    result.narrative.append(f"elected leader={leader} at term={sim.nodes[leader].current_term if leader else None}")
    return result.finalize(sim)


def leader_failure_and_reelection(seed: int = 0, n_nodes: int = 5) -> ScenarioResult:
    """Commit some entries, crash the leader, verify a new leader (higher
    term) is elected and keeps committing; then restart the old leader and
    verify it catches up to the same log via AppendEntries backfill.
    """
    node_ids = _default_nodes(n_nodes)
    sim = Simulator(node_ids, seed=seed)
    result = ScenarioResult(name="leader_failure_and_reelection", seed=seed, node_ids=node_ids)

    leader1 = sim.run_until_stable_leader(max_time=5000)
    result.narrative.append(f"initial leader={leader1} term={sim.nodes[leader1].current_term}")

    for cmd in ("set x=1", "set y=2"):
        sim.propose(cmd)
    sim.run_for(300)
    result.narrative.append(f"committed before crash: {sim.commit_indices()}")

    sim.crash(leader1)
    result.narrative.append(f"crashed leader {leader1}")

    leader2 = sim.run_until_stable_leader(max_time=5000)
    result.narrative.append(f"new leader={leader2} term={sim.nodes[leader2].current_term if leader2 else None}")

    for cmd in ("set z=3", "set w=4"):
        sim.propose(cmd)
    sim.run_for(300)
    result.narrative.append(f"committed after re-election: {sim.commit_indices()}")

    sim.restart(leader1)
    sim.run_for(3000)
    result.narrative.append(f"after restarting old leader: logs={sim.log_lengths()}")

    return result.finalize(sim)


def network_partition_and_heal(seed: int = 0, n_nodes: int = 5) -> ScenarioResult:
    """Split the cluster into a minority (with the current leader) and a
    majority. The minority leader can't commit anything (no quorum); the
    majority elects a new leader and makes progress. Healing the partition
    should converge everyone onto the majority's log.
    """
    node_ids = _default_nodes(n_nodes)
    sim = Simulator(node_ids, seed=seed)
    result = ScenarioResult(name="network_partition_and_heal", seed=seed, node_ids=node_ids)

    leader1 = sim.run_until_stable_leader(max_time=5000)
    minority = [leader1]
    majority = [nid for nid in node_ids if nid != leader1]
    sim.partition(minority, majority)
    result.narrative.append(f"partitioned: minority={minority} majority={majority}")

    sim.propose("minority-write", leader_id=leader1)  # isolated old leader -- must never commit
    sim.run_for(3000)
    majority_leader = next((nid for nid in majority if sim.nodes[nid].role.name == "LEADER"), None)
    result.narrative.append(f"majority elected leader={majority_leader}")
    if majority_leader is not None:
        for cmd in ("majority-write-1", "majority-write-2"):
            sim.propose(cmd, leader_id=majority_leader)
        sim.run_for(1000)
    result.narrative.append(f"commits during partition: {sim.commit_indices()}")

    sim.heal_partition()
    sim.run_for(3000)
    result.narrative.append(f"after heal: roles={sim.roles()} logs={sim.log_lengths()}")

    return result.finalize(sim)


def slow_follower_catchup(seed: int = 0, n_nodes: int = 5) -> ScenarioResult:
    """One follower experiences much higher delay/drop than the rest of the
    cluster while the leader accepts many client writes; verify it still
    eventually catches up once given enough time (nextIndex back-off + retry
    on every heartbeat).
    """
    node_ids = _default_nodes(n_nodes)
    sim = Simulator(node_ids, seed=seed, drop_rate=0.3, max_delay=40.0)
    result = ScenarioResult(name="slow_follower_catchup", seed=seed, node_ids=node_ids)

    leader = sim.run_until_stable_leader(max_time=8000)
    result.narrative.append(f"leader={leader}")

    for i in range(20):
        sim.propose(f"cmd{i}")
        sim.run_for(60)
    sim.run_for(5000)  # let retries/backoff work through the lossy link

    result.narrative.append(f"log lengths: {sim.log_lengths()}")
    result.narrative.append(f"commit indices: {sim.commit_indices()}")
    return result.finalize(sim)


def full_split_no_progress(seed: int = 0, n_nodes: int = 4) -> ScenarioResult:
    """An even split (no side has a majority) must make zero progress --
    demonstrating that Raft favors safety over liveness. n_nodes defaults to
    4 so a clean 2-2 split is possible.
    """
    node_ids = _default_nodes(n_nodes)
    sim = Simulator(node_ids, seed=seed)
    result = ScenarioResult(name="full_split_no_progress", seed=seed, node_ids=node_ids)

    sim.run_for(2000)  # let an initial leader emerge (or not -- doesn't matter)
    half = len(node_ids) // 2
    group_a, group_b = node_ids[:half], node_ids[half:]
    sim.partition(group_a, group_b)
    result.narrative.append(f"even split: {group_a} | {group_b}")

    sim.propose("should-never-commit")
    sim.run_for(5000)
    result.narrative.append(f"commit indices during split: {sim.commit_indices()}")

    sim.heal_partition()
    sim.run_for(3000)
    result.narrative.append(
        f"after heal (entry from the split may still be uncommitted -- Raft 5.4.2, "
        f"figure 8: a leader can't directly commit an entry from an earlier term): "
        f"commits={sim.commit_indices()} logs={sim.log_lengths()}"
    )

    # A leader can only commit an old-term entry *transitively*, by first
    # committing something from its own current term. Proposing one more
    # command now should unblock the earlier one too.
    sim.propose("post-heal-write")
    sim.run_for(3000)
    result.narrative.append(f"after one more write post-heal: commits={sim.commit_indices()}")

    return result.finalize(sim)


SCENARIOS = {
    "basic-election": (basic_election, "Elect a single stable leader in a healthy 5-node cluster."),
    "leader-failure-reelection": (
        leader_failure_and_reelection,
        "Crash the leader mid-stream, verify re-election and eventual catch-up.",
    ),
    "network-partition-heal": (
        network_partition_and_heal,
        "Isolate the leader in a minority partition, then heal it.",
    ),
    "slow-follower-catchup": (
        slow_follower_catchup,
        "A lossy/high-latency follower link should still converge eventually.",
    ),
    "full-split-no-progress": (
        full_split_no_progress,
        "An even network split must make zero commit progress (safety > liveness).",
    ),
}


def run_scenario(name: str, seed: int = 0, **kwargs: Any) -> ScenarioResult:
    if name not in SCENARIOS:
        raise KeyError(f"unknown scenario {name!r}; choices: {sorted(SCENARIOS)}")
    fn, _ = SCENARIOS[name]
    return fn(seed=seed, **kwargs)

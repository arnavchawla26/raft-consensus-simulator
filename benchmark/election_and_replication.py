#!/usr/bin/env python3
"""Two small measured (not assumed) experiments on top of the simulator:

1. election_time_vs_cluster_size: does a bigger cluster take measurably
   longer (in virtual time / messages) to elect a stable leader?
2. commit_latency_vs_drop_rate: how much does lossy network conditions slow
   down committing a batch of sequential proposals?

Both report real numbers averaged over many seeds -- run this yourself
(`python benchmark/election_and_replication.py`) rather than trusting the
numbers quoted in the README, which are just one snapshot.
"""

from __future__ import annotations

import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from raft.cluster import Simulator  # noqa: E402

N_SEEDS = 30


def election_time_vs_cluster_size(cluster_sizes=(3, 5, 7, 9, 11)) -> None:
    print("=== election time & message count vs. cluster size ===")
    print(f"{'nodes':>6} {'mean elect time':>18} {'mean messages':>15} {'mean rounds/term reached':>26}")
    for n in cluster_sizes:
        times, messages, terms = [], [], []
        for seed in range(N_SEEDS):
            node_ids = list(range(1, n + 1))
            sim = Simulator(node_ids, seed=seed)
            leader = sim.run_until_stable_leader(max_time=10_000, check_interval=20)
            if leader is None:
                continue
            times.append(sim.now)
            messages.append(sim.messages_sent)
            terms.append(sim.nodes[leader].current_term)
        print(
            f"{n:>6} {statistics.mean(times):>18.1f} {statistics.mean(messages):>15.1f} "
            f"{statistics.mean(terms):>26.2f}"
        )


def commit_latency_vs_drop_rate(drop_rates=(0.0, 0.1, 0.3, 0.5), n_nodes=5, n_commands=20) -> None:
    print("\n=== virtual time to commit N sequential proposals vs. message-drop rate ===")
    print(
        "NOTE: only seeds that fully converge within the time budget are averaged; at high "
        "drop rates most seeds DON'T converge in budget, so the 'mean' columns below are "
        "biased toward the (unrepresentative) fast outliers -- the 'converged' column is the "
        "real headline number at high drop rates, not the timing averages next to it."
    )
    print(f"{'drop_rate':>10} {'converged':>11} {'mean time to commit all':>26} {'mean messages sent':>20}")
    for drop_rate in drop_rates:
        times, messages = [], []
        n_seeds_run = 0
        for seed in range(N_SEEDS):
            node_ids = list(range(1, n_nodes + 1))
            sim = Simulator(node_ids, seed=seed, drop_rate=drop_rate)
            leader = sim.run_until_stable_leader(max_time=10_000, check_interval=20)
            if leader is None:
                continue
            n_seeds_run += 1
            for i in range(n_commands):
                sim.propose(f"cmd{i}")
                sim.run_for(60)
            # give stragglers a generous window to finish committing
            deadline = sim.now + 20_000
            while sim.now < deadline and min(sim.commit_indices().values()) < n_commands:
                sim.run_for(200)
            if min(sim.commit_indices().values()) < n_commands:
                continue  # didn't converge within the budget at this drop rate/seed
            times.append(sim.now)
            messages.append(sim.messages_sent)
        converged_str = f"{len(times)}/{n_seeds_run}"
        if times:
            print(
                f"{drop_rate:>10.1f} {converged_str:>11} {statistics.mean(times):>26.1f} "
                f"{statistics.mean(messages):>20.1f}"
            )
        else:
            print(f"{drop_rate:>10.1f} {converged_str:>11} {'no seed converged in budget':>26} {'-':>20}")


if __name__ == "__main__":
    election_time_vs_cluster_size()
    commit_latency_vs_drop_rate()

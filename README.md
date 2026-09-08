# raft-consensus-simulator

A from-scratch, in-process implementation and discrete-event simulator for
the [Raft consensus algorithm](https://raft.github.io/raft.pdf) (Ongaro &
Ousterhout, 2014) — leader election and log replication, with injectable
network partitions, message delay/loss, and node crashes/restarts, plus a
suite of randomized tests that check Raft's safety properties hold no
matter what chaos gets thrown at the cluster.

No external Raft library, no real sockets, no real threads — `RaftNode` is
a pure state machine driven entirely by function calls, and `Simulator` is
a single-threaded priority-queue-based event loop that "sends" messages by
scheduling their delivery at `now + random_delay`. A run is fully
deterministic given `(seed, script of proposals/faults)`, regardless of how
much real wall-clock time it takes to execute.

## What it actually implements

- **Leader election**: randomized election timeouts, `RequestVote` RPCs,
  the up-to-date-log check (§5.4.1), majority-vote-triggered transition to
  leader, and the Election Safety property (at most one leader per term).
- **Log replication**: `AppendEntries` RPCs (heartbeats and real entries),
  the prev-log-index/term consistency check, log truncation on conflict and
  the Log Matching Property (§5.3), and leader-side `nextIndex`/`matchIndex`
  tracking with the standard "back off one entry per rejection" retry.
- **Commit rules, including the easy-to-get-wrong part**: a leader only
  advances `commitIndex` when a majority has replicated an entry *from the
  leader's own current term* (§5.4.2, Figure 8) — older-term entries commit
  only transitively, once a same-term entry after them commits. There's a
  scenario and a unit test specifically for this, because it's the part of
  Raft that looks like a bug if you don't know to expect it (see
  "Methodology" below).
- **Fault injection**: `Simulator.partition(group_a, group_b)` /
  `heal_partition()` to cut/restore arbitrary sets of links, `crash(id)` /
  `restart(id)` (a restarted node keeps its persistent state — term, vote,
  log — exactly as Raft's crash-recovery model assumes), and per-message
  configurable delay range + drop probability.
- **Safety-property checking** (`raft/safety.py`): Election Safety, Log
  Matching, "committed entries never diverge" (the checkable log-level form
  of Leader Completeness + State Machine Safety), and State Machine Safety
  at the applied-command level — each independently unit-tested against
  hand-built violations (not just checked to pass on good data, which would
  say nothing about whether the checker actually checks anything).

## What it deliberately does NOT implement

- **Cluster membership changes** (joint consensus, §6) — the peer set is
  fixed for the lifetime of a `Simulator`.
- **Log compaction / snapshotting** (§7) — logs grow unboundedly; fine for
  a simulator, not fine for a real long-running server.
- **A real persistence/WAL layer** — "persistent state" just lives in
  process memory. A crashed-and-restarted node keeps it (modeling an fsync
  that already happened), but there's no on-disk log to actually crash-test
  against (see [b-tree-kv-store](https://github.com/arnavchawla26/b-tree-kv-store)
  in this account's other projects for that kind of test, applied to a
  storage engine instead).
- **Fast conflict-term backtracking** for `nextIndex` (the optimization
  mentioned in §5.3) — this uses the basic "decrement by one and retry"
  algorithm from Figure 2. Correct, just not the fastest possible catch-up
  for a follower that's far behind.
- **Real networking** — everything is in-process; "sending a message" is
  scheduling a future event on a priority queue.

## Project layout

```
raft/
  log.py         RaftLog: the 1-indexed append/truncate log
  messages.py    RequestVote/AppendEntries RPC dataclasses
  node.py        RaftNode: the pure Raft state machine (Figure 2)
  cluster.py     Simulator: discrete-event clock, network, fault injection
  safety.py      Independent checkers for Raft's four safety properties
  scenarios.py   Five named, reproducible scenarios built on Simulator
  cli.py         `raftsim` command-line entry point
tests/           160 tests (see "Testing" below)
benchmark/       Two measured (not assumed) experiments, see "Benchmarks"
```

## Install & run

```bash
pip install -e ".[dev]"

raftsim list-scenarios
raftsim run basic-election --seed 1
raftsim run leader-failure-reelection --seed 7
raftsim run network-partition-heal --seed 3
raftsim run slow-follower-catchup --seed 0
raftsim run full-split-no-progress --seed 2

# run one scenario across many seeds and report the pass rate
raftsim sweep basic-election --seeds 50
```

Sample output (`raftsim run leader-failure-reelection --seed 0`):

```
scenario: leader_failure_and_reelection  seed: 0  nodes: [1, 2, 3, 4, 5]
narrative:
  - initial leader=... term=1
  - committed before crash: {...}
  - crashed leader ...
  - new leader=... term=2
  - committed after re-election: {...}
  - after restarting old leader: logs={...}
final roles:          {...}
final commit indices: {...}
final log lengths:    {...}
leader history (term, node_id): [(1, ...), (2, ...)]
messages sent: ...  dropped: ...
safety properties:
  election safety:          OK
  log matching:             OK
  committed entries stable: OK
  state machine safety:     OK
=> PASS
```

Or use it as a library:

```python
from raft.cluster import Simulator

sim = Simulator(node_ids=[1, 2, 3, 4, 5], seed=42)
leader = sim.run_until_stable_leader(max_time=5000)
sim.propose("set x = 1")
sim.run_for(500)
print(sim.commit_indices())   # {1: 1, 2: 1, 3: 1, 4: 1, 5: 1}
```

## Testing

160 tests, `pytest -q`:

- `test_log.py` (11) — the log data structure in isolation.
- `test_node_election.py` (13) / `test_node_append_entries.py` (22) — the
  `RaftNode` state machine, unit-tested per-RPC with hand-built
  args/replies (no simulator involved), including the transitive-commit
  case above.
- `test_cluster_basic.py` (17) — the `Simulator`'s clock, fault injection,
  and determinism-given-the-same-seed.
- `test_safety_checkers.py` (12) — adversarial unit tests for the safety
  checkers themselves, feeding each one hand-built data it *must* flag.
  This exists because `test_safety_properties.py` only proves the checkers
  pass on states the real simulator produces; it can't distinguish a
  correct checker from one that's silently broken (e.g. always returns
  `True`), since a correct implementation never produces a violation to
  catch. These tests do produce violations, on purpose.
- `test_safety_properties.py` (42) — the actual bug-catcher: 40 seeds of
  randomized fault injection (random mix of proposals, partitions, heals,
  crashes, restarts) with all four safety properties checked *after every
  single step*, not just at the end, so a transient violation that later
  "heals" itself (e.g. a leader wrongly rolling back a commit) can't hide.
  Plus one log-convergence check and one leader-uniqueness check run across
  all 40 seeds again.
- `test_scenarios.py` (35) / `test_cli.py` (8) — the five named scenarios
  (each across 5 seeds) and the `raftsim` CLI, including a real subprocess
  invocation (`python -m raft.cli ...`) to catch packaging/import mistakes
  that calling `main()` in-process wouldn't.

## Benchmarks

`python benchmark/election_and_replication.py` runs two experiments, real
measurements averaged over 30 seeds each (numbers below are one snapshot —
run it yourself; there's no committed data file, just this script):

```
=== election time & message count vs. cluster size ===
 nodes    mean elect time   mean messages   mean rounds/term reached
     3              234.7             8.4                       1.00
     5              222.7            17.6                       1.03
     7              215.3            27.6                       1.03
     9              210.7            37.9                       1.03
    11              208.7            46.7                       1.03
```

Time-to-elect a stable leader does **not** grow with cluster size in this
range (it's flat to slightly *down*, likely just noise from the randomized
timeout draws) — only the message count grows, linearly, exactly as
expected since every node's timeout/vote fans out to `n-1` peers. Splitting
votes (more than one term needed) is rare regardless of size: ~1.03 mean
terms-to-elect even at 11 nodes.

```
=== virtual time to commit N sequential proposals vs. message-drop rate ===
 drop_rate   converged    mean time to commit all   mean messages sent
       0.0       30/30                     1422.7                369.6
       0.1       29/30                     1492.4                326.8
       0.3       29/30                     1953.1                265.5
       0.5       10/30                     4102.0                278.5
```

The honest reading of this table: commit latency clearly gets worse as
drop rate rises (1422 → 4102 virtual-time units from 0% to 50% loss), but
the "mean messages sent" column is **not** a clean trend — read it, then
disregard it. At `drop_rate=0.5` only 10 of 30 seeds finished all 20
commits within the time budget at all; the other 20 are silently excluded
from that row's averages, which biases both columns toward whichever
seeds got lucky enough to converge quickly. The real headline number at
high drop rates is the `converged` column, not the timing next to it. This
is the kind of thing that looks like a confusing benchmark result until
you check *how many runs actually finished* before trusting an average
across them.

## Methodology notes

- **The transitive-commit rule was the one part of this project that
  produced a result looking like a bug until traced by hand.** Early in
  writing the `full-split-no-progress` scenario, healing an even network
  split and giving the cluster plenty of time left an already-replicated
  log entry stuck at `commit_index=0` on every node. That's not a bug —
  it's Raft §5.4.2 / Figure 8: a leader can only directly commit entries
  from its *own* current term, so a lone old-term entry that survived a
  chaotic split needs one more write from the new leader's term before it
  (and everything before it) can commit. The scenario now demonstrates
  this explicitly (propose once during the split, heal, observe it stays
  uncommitted, propose one more time, observe both commit together), and
  `test_leader_cannot_directly_commit_an_entry_from_an_earlier_term` pins
  down the exact mechanism at the unit level.
- **Two test-design mistakes were caught pre-push by tracing actual
  simulator output, not by assuming what "should" happen**: an early
  version of the same unit test tried to fake a majority by manually
  setting only one follower's `matchIndex` to the new index and expected a
  4-node cluster (needs 3 of 4) to commit off of it — it doesn't, because
  it's not actually a majority once you count correctly. And an early
  version of the partition scenario test asserted every node's commit
  index stays 0 during a network split — true for a genuine even split,
  false for a *minority* split, where the majority side is specifically
  supposed to keep committing (that's the whole point of majority quorum);
  the test now checks the isolated old leader's commit index specifically,
  and separately asserts the majority side *did* make progress.
- **Randomized, per-step safety checking (not just scripted scenarios)
  is what actually stresses the commit-index/log-matching logic.** The
  five named scenarios are useful for readability (each tells a specific
  story) but they don't explore much of the state space by themselves;
  `test_safety_properties.py` throws a random mix of every fault type at
  the cluster, 40 seeds deep, and checks after every step — this is
  modeled on the randomized model-based testing approach used for the
  B+Tree split/merge logic in this account's
  [b-tree-kv-store](https://github.com/arnavchawla26/b-tree-kv-store)
  project, adapted here to distributed-systems safety invariants instead
  of a single data structure's invariants.

## Verification performed before pushing

- Fresh-venv `pip install -e ".[dev]"`, full `pytest -q` (160/160 passed),
  `pyflakes raft tests` (clean).
- Cloned the pushed repo fresh, repeated the install + test + pyflakes
  cycle there, and ran `raftsim run <scenario>` for all five scenarios on
  the clone to confirm the CLI output matches what's shown above.
- `diff -rq` between the local working copy and the fresh clone.

## Status

v1, complete and tested in a single pass: leader election, log replication,
the transitive-commit subtlety, five fault-injection scenarios, safety-
property checkers with their own adversarial tests, randomized multi-seed
property testing, a CLI, and a benchmark script. Possible future work
(not started): cluster membership changes, log compaction/snapshotting,
and the §5.3 fast-backtracking `nextIndex` optimization.

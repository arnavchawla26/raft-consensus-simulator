"""raftsim: command-line entry point.

    raftsim list-scenarios
    raftsim run basic-election --seed 1
    raftsim run leader-failure-reelection --seed 7 --nodes 5
    raftsim sweep basic-election --seeds 20
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from .scenarios import SCENARIOS, run_scenario


def _print_result(result, verbose: bool = False) -> None:
    print(f"scenario: {result.name}  seed: {result.seed}  nodes: {result.node_ids}")
    if verbose:
        print("narrative:")
        for line in result.narrative:
            print(f"  - {line}")
    print(f"final roles:          {result.final_roles}")
    print(f"final commit indices: {result.final_commit_indices}")
    print(f"final log lengths:    {result.final_log_lengths}")
    print(f"leader history (term, node_id): {result.leader_history}")
    print(f"messages sent: {result.messages_sent}  dropped: {result.messages_dropped}")
    print("safety properties:")
    for line in result.safety.summary_lines():
        print(f"  {line}")
    print(f"=> {'PASS' if result.safety.all_ok else 'FAIL'}")


def cmd_list_scenarios(args: argparse.Namespace) -> int:
    for name, (_, description) in sorted(SCENARIOS.items()):
        print(f"{name:28s} {description}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    kwargs = {}
    if args.nodes is not None:
        kwargs["n_nodes"] = args.nodes
    result = run_scenario(args.scenario, seed=args.seed, **kwargs)
    _print_result(result, verbose=not args.quiet)
    return 0 if result.safety.all_ok else 1


def cmd_sweep(args: argparse.Namespace) -> int:
    """Run the same scenario across many seeds and report the pass rate --
    a quick way to build confidence beyond one lucky/unlucky seed.
    """
    failures: List[int] = []
    for seed in range(args.seeds):
        kwargs = {}
        if args.nodes is not None:
            kwargs["n_nodes"] = args.nodes
        result = run_scenario(args.scenario, seed=seed, **kwargs)
        if not result.safety.all_ok:
            failures.append(seed)
            print(f"seed {seed}: FAIL")
            for line in result.safety.summary_lines():
                if line.strip().startswith("!"):
                    print(f"    {line}")
        elif args.verbose:
            print(f"seed {seed}: PASS")
    total = args.seeds
    print(f"\n{total - len(failures)}/{total} seeds passed safety checks for {args.scenario!r}")
    if failures:
        print(f"failing seeds: {failures}")
    return 0 if not failures else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="raftsim", description="Raft consensus simulator")
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list-scenarios", help="List available scenarios")
    p_list.set_defaults(func=cmd_list_scenarios)

    p_run = sub.add_parser("run", help="Run a single scenario once")
    p_run.add_argument("scenario", choices=sorted(SCENARIOS))
    p_run.add_argument("--seed", type=int, default=0)
    p_run.add_argument("--nodes", type=int, default=None, help="override the scenario's default cluster size")
    p_run.add_argument("--quiet", action="store_true", help="skip the step-by-step narrative")
    p_run.set_defaults(func=cmd_run)

    p_sweep = sub.add_parser("sweep", help="Run a scenario across many seeds and report the pass rate")
    p_sweep.add_argument("scenario", choices=sorted(SCENARIOS))
    p_sweep.add_argument("--seeds", type=int, default=20)
    p_sweep.add_argument("--nodes", type=int, default=None)
    p_sweep.add_argument("--verbose", action="store_true")
    p_sweep.set_defaults(func=cmd_sweep)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

import subprocess
import sys

from raft.cli import build_parser, main


def test_build_parser_run_defaults():
    parser = build_parser()
    args = parser.parse_args(["run", "basic-election"])
    assert args.scenario == "basic-election"
    assert args.seed == 0
    assert args.nodes is None


def test_main_run_returns_zero_on_pass(capsys):
    code = main(["run", "basic-election", "--seed", "0", "--quiet"])
    out = capsys.readouterr().out
    assert code == 0
    assert "PASS" in out
    assert "scenario: basic_election" in out


def test_main_run_prints_narrative_unless_quiet(capsys):
    main(["run", "basic-election", "--seed", "0"])
    out = capsys.readouterr().out
    assert "narrative:" in out


def test_main_list_scenarios(capsys):
    code = main(["list-scenarios"])
    out = capsys.readouterr().out
    assert code == 0
    assert "basic-election" in out
    assert "leader-failure-reelection" in out


def test_main_sweep_reports_pass_rate(capsys):
    code = main(["sweep", "basic-election", "--seeds", "5"])
    out = capsys.readouterr().out
    assert code == 0
    assert "5/5 seeds passed" in out


def test_main_run_rejects_unknown_scenario():
    parser = build_parser()
    try:
        parser.parse_args(["run", "not-a-scenario"])
        assert False, "expected argparse to reject an unknown scenario choice"
    except SystemExit:
        pass


def test_cli_entry_point_via_subprocess():
    # a real subprocess invocation, exercising python -m raft.cli end to end
    # (catches import errors / packaging mistakes unit tests calling main()
    # directly wouldn't notice)
    proc = subprocess.run(
        [sys.executable, "-m", "raft.cli", "run", "basic-election", "--seed", "1", "--quiet"],
        capture_output=True, text=True, timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    assert "PASS" in proc.stdout


def test_cli_run_returns_nonzero_when_safety_report_fails(monkeypatch):
    # force a fake "failed" result through cmd_run to prove the exit code is
    # actually wired to result.safety.all_ok, rather than always returning 0.
    import raft.cli as cli_module
    from raft.safety import SafetyReport

    class _FakeResult:
        name = "fake"
        seed = 0
        node_ids = [1, 2, 3]
        narrative = []
        final_roles = {}
        final_commit_indices = {}
        final_log_lengths = {}
        leader_history = []
        messages_sent = 0
        messages_dropped = 0
        safety = SafetyReport(election_safety_ok=False, election_safety_violations=["forced failure"])

    monkeypatch.setattr(cli_module, "run_scenario", lambda name, seed=0, **kw: _FakeResult())
    code = main(["run", "basic-election", "--seed", "0", "--quiet"])
    assert code == 1

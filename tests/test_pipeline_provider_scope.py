"""Exercise provider-only collection through the real pipeline scripts."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
LEAGUES = "NFL NBA CFB NCAAB NHL EPL LALIGA BUNDESLIGA SERIEA LIGUE1".split()
PAID = "NBA,NHL,EPL,LALIGA,BUNDESLIGA,SERIEA,LIGUE1"
RESEARCH = ["NBA", "NCAAB"]


@pytest.fixture(params=["powershell", "bash"])
def pipeline(request, tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    config = tmp_path / "config"
    config.mkdir()
    (config / "paper_trial.yml").touch()
    environment = {
        key: os.environ[key]
        for key in ("PATH", "PATHEXT", "COMSPEC", "SYSTEMROOT", "TEMP", "TMP")
        if key in os.environ
    }

    if request.param == "powershell":
        shell = shutil.which("powershell")
        if shell is None:
            pytest.skip("Windows PowerShell is required for the native task contract")
        for script in ("pipeline.ps1", "ingest_all.ps1", "predict.ps1"):
            shutil.copyfile(SCRIPTS / script, scripts / script)
        stub = tmp_path / "provider_stub.py"
        stub.write_text(
            "import os, sys\n"
            "args = sys.argv[1:]\n"
            "if args[:2] == ['run', 'python']:\n"
            "    args = args[2:]\n"
            "with open('provider_calls.tsv', 'a', encoding='utf-8') as trace:\n"
            "    print(*args, sep='\\t', file=trace)\n"
            "if os.getenv('PROVIDER_TEST_FAIL_QUALITY') == '1' and args == "
            "['-m', 'src.data.quality', '--leagues', 'NBA,NHL,EPL,LALIGA,BUNDESLIGA,SERIEA,LIGUE1']:\n"
            "    sys.exit(1)\n"
            "if os.getenv('PROVIDER_TEST_FAIL_SCHEDULE') == '1' and args[:2] == "
            "['-m', 'src.data.audit_nba_schedule']:\n"
            "    sys.exit(23)\n"
            "failed = os.getenv('PROVIDER_TEST_FAIL_STEP')\n"
            "if failed == 'scores' and args[:2] == ['-m', 'src.data.ingest_scores']:\n"
            "    sys.exit(17)\n"
            "if failed == 'prediction' and args[:2] == ['-m', 'src.predict.runner'] "
            "and '--paper-only' not in args:\n"
            "    sys.exit(17)\n"
            "if failed in ('publish', 'revoke') and args[:3] == "
            "['-m', 'src.predict.publishable_bets', failed]:\n"
            "    sys.exit(17)\n",
            encoding="utf-8",
        )
        wrapper = '@echo off\n"%PROVIDER_TEST_PYTHON%" "%PROVIDER_TEST_STUB%" %*\n'
        for name in ("python.cmd", "poetry.cmd"):
            (tmp_path / name).write_text(wrapper, encoding="utf-8")
        environment.update(
            PATH=os.pathsep.join((str(tmp_path), str(Path(environment["SYSTEMROOT"]) / "System32"))),
            PROVIDER_TEST_PYTHON=sys.executable,
            PROVIDER_TEST_STUB=str(stub),
        )
        command = [
            shell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
            str(scripts / "pipeline.ps1"),
        ]
        flags = {
            "focus": "-ResearchProviderLeagues", "skip": "-SkipOdds", "soccer": "-SoccerOnly",
            "hourly": "-SkipTraining", "skip_benchmark": "-SkipBenchmark",
            "skip_paper": "-SkipPaper", "skip_prediction": "-SkipPrediction",
        }
    else:
        git_bash = Path("C:/Program Files/Git/bin/bash.exe")
        shell = str(git_bash) if os.name == "nt" and git_bash.is_file() else shutil.which("bash")
        if shell is None:
            pytest.skip("Bash is required for the Linux pipeline contract")
        (scripts / "pipeline.sh").write_text(
            (SCRIPTS / "pipeline.sh").read_text(encoding="utf-8"), encoding="utf-8", newline="\n",
        )
        stub = f"""python() {{
    local IFS=$'\t'
    printf '%s\\n' "$*" >> provider_calls.tsv
    if [[ "${{PROVIDER_TEST_FAIL_QUALITY:-}}" == 1 && "$*" == $'-m\\tsrc.data.quality\\t--leagues\\t{PAID}' ]]; then
        return 1
    fi
    if [[ "${{PROVIDER_TEST_FAIL_SCHEDULE:-}}" == 1 && "${{2:-}}" == src.data.audit_nba_schedule ]]; then
        return 23
    fi
    return 0
}}
export -f python
"$BASH" scripts/pipeline.sh "$@"
"""
        command = [shell, "--noprofile", "--norc", "-c", stub, "pipeline-test"]
        flags = {
            "focus": "--research-provider-leagues", "skip": "--skip-odds", "soccer": "--soccer-only",
            "hourly": "--skip-training", "skip_benchmark": "--skip-benchmark",
            "skip_paper": "--skip-paper", "skip_prediction": "--skip-prediction",
        }

    def run(*options, fail_quality=False, fail_schedule=False, fail_step="", daily=False, local_focus=False):
        if not daily:
            options += ("skip_benchmark",)
        if local_focus:
            (config / "research_provider_leagues.local").touch()
        result = subprocess.run(
            [*command, *(flags[option] for option in options)], cwd=tmp_path,
            env={
                **environment,
                "PROVIDER_TEST_FAIL_QUALITY": str(int(fail_quality)),
                "PROVIDER_TEST_FAIL_SCHEDULE": str(int(fail_schedule)),
                "PROVIDER_TEST_FAIL_STEP": fail_step,
            },
            capture_output=True, text=True, timeout=45,
        )
        trace = tmp_path / "provider_calls.tsv"
        calls = [line.split("\t") for line in trace.read_text(encoding="utf-8").splitlines()] if trace.exists() else []
        return result, calls

    return run


def commands(calls, module):
    return [call[2:] for call in calls if call[:2] == ["-m", module]]


@pytest.mark.parametrize(
    "options,local_focus,expected_odds,expected_scores",
    [
        ((), False, ["NBA"] + [league for league in LEAGUES if league != "NBA"], LEAGUES),
        (("focus",), False, RESEARCH, RESEARCH),
        ((), True, RESEARCH, RESEARCH),
        (("focus", "skip"), False, [], []),
    ],
)
def test_provider_scope_preserves_pipeline(pipeline, options, local_focus, expected_odds, expected_scores):
    result, calls = pipeline(*options, local_focus=local_focus)
    assert result.returncode == 0, result.stdout + result.stderr
    odds = commands(calls, "src.data.ingest_odds")
    assert [args[:4] for args in odds] == [
        ["--league", league, "--market", "h2h,totals"] for league in expected_odds
    ]
    scores = commands(calls, "src.data.ingest_scores")
    assert [args[:2] for args in scores] == ([["--leagues", ",".join(expected_scores)]] if expected_scores else [])
    assert commands(calls, "src.data.ingest_manager") == [["--leagues", *LEAGUES]]
    predictions = commands(calls, "src.predict.runner")
    assert [args[1] for args in predictions if "--paper-only" not in args] == [
        league for league in LEAGUES for _ in range(3)
    ]
    assert commands(calls, "src.predict.paper_ledger") == [["update"]]
    assert commands(calls, "src.data.audit_nba_schedule") == []
    assert ["--leagues", PAID] in commands(calls, "src.data.quality")
    assert commands(calls, "src.predict.publishable_bets")[0][0] == "publish"
    if odds:
        assert "--require-fresh" in odds[0]
        assert "--receipt-output" in odds[0]
        assert calls[1] == ["-m", "src.data.ingest_odds", *odds[0]]
        assert calls[2] == ["-m", "src.predict.runner", "--leagues", "NBA", "--model-type", "random_forest", "--paper-only"]
    else:
        assert all("--paper-only" not in args for args in predictions)


def test_focused_collection_keeps_release_revocation(pipeline):
    result, calls = pipeline("focus", fail_quality=True)
    assert result.returncode == 1, result.stdout + result.stderr
    assert [args[0] for args in commands(calls, "src.predict.publishable_bets")] == ["revoke"]
    assert len(commands(calls, "src.data.ingest_odds")) == 2


def test_conflicting_scope_exits_before_any_commands(pipeline):
    result, calls = pipeline("focus", "soccer")
    assert result.returncode == 64
    assert "cannot be combined" in result.stderr
    assert calls == []


def test_workspace_provider_scope_keeps_explicit_soccer_collection(pipeline):
    result, calls = pipeline("soccer", "skip_prediction", local_focus=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert [args[1] for args in commands(calls, "src.data.ingest_odds")] == LEAGUES[5:]
    assert [args[:2] for args in commands(calls, "src.data.ingest_scores")] == [
        ["--leagues", ",".join(LEAGUES[5:])]
    ]


def test_daily_schedule_audit_runs_after_quote_timing_and_is_diagnostic_only(pipeline):
    result, calls = pipeline("focus", daily=True, fail_schedule=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Next-day NBA schedule audit could not run." in result.stdout
    assert commands(calls, "src.data.audit_nba_schedule") == [["--next-eastern-day", "--fetch", "--save"]]
    modules = [call[1] for call in calls if call[0] == "-m"]
    assert (
        modules.index("src.predict.paper_ledger")
        < modules.index("src.predict.publishable_bets")
        < modules.index("scripts.nba_quote_timing")
        < modules.index("src.data.audit_nba_schedule")
        < modules.index("src.models.train_betting")
    )


@pytest.mark.parametrize("pipeline", ["powershell"], indirect=True)
@pytest.mark.parametrize(
    "failed_step,expected_exit", [("scores", 1), ("prediction", 1), ("revoke", 17), ("publish", 17)],
)
def test_windows_daily_schedule_audit_survives_fatal_pipeline_step(pipeline, failed_step, expected_exit):
    result, calls = pipeline(
        "focus", daily=True, fail_step=failed_step, fail_quality=failed_step == "revoke", fail_schedule=True,
    )
    assert result.returncode == expected_exit, result.stdout + result.stderr
    assert "Next-day NBA schedule audit could not run." in result.stdout
    assert commands(calls, "src.data.audit_nba_schedule") == [["--next-eastern-day", "--fetch", "--save"]]
    assert [call[1] for call in calls[-2:]] == ["scripts.nba_quote_timing", "src.data.audit_nba_schedule"]


@pytest.mark.parametrize("flag", ["skip", "soccer", "hourly", "skip_paper"])
def test_schedule_audit_is_skipped_without_daily_paper_and_live_odds(pipeline, flag):
    result, calls = pipeline(flag, "skip_prediction", daily=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert commands(calls, "src.data.audit_nba_schedule") == []

"""Exercise the deployed paper cron, command dispatch and health contract."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
PAPER_JOBS = [
    ("paper_close", "*/15", "src.predict.paper_close_poll --collect --batch-scheduled"),
    ("paper_entry", "5,20,35,50", "src.predict.paper_entry_poll --collect"),
    ("paper_events", "10,25,40,55", "src.data.ingest_nba_events --collect --observe-paper"),
]


@pytest.fixture
def worker_shell(tmp_path):
    git_bash = Path("C:/Program Files/Git/bin/bash.exe")
    bash = str(git_bash) if os.name == "nt" and git_bash.is_file() else shutil.which("bash")
    if bash is None:
        pytest.skip("Bash is required for the worker shell contract")
    shell_environment = {key: os.environ[key] for key in ("PATH", "SYSTEMROOT", "TEMP", "TMP") if key in os.environ}
    environment = {
        **shell_environment,
        "MODELS_DIR": str(tmp_path / "models"),
        "LOG_DIR": str(tmp_path / "logs"),
        "HEARTBEAT_DIR": str(tmp_path / "heartbeat"),
        "WORKER_TEST_ROOT": str(tmp_path),
        "ODDS_API_KEY": "fake-key-must-not-enable-live-odds",
        **{f"WORKER_{job.upper()}_ENABLED": "0" for job, _, _ in PAPER_JOBS},
    }

    def run(script, *args, **overrides):
        return subprocess.run(
            [bash, "--noprofile", "--norc", "-c", script, "worker-test", *args],
            env={**environment, **overrides},
            capture_output=True,
            text=True,
            timeout=20,
        )

    return run


@pytest.fixture
def install_cron(tmp_path):
    entrypoint = (SCRIPTS / "worker_entrypoint.sh").read_text(encoding="utf-8")
    runner = (SCRIPTS / "run_job.sh").read_text(encoding="utf-8")
    (tmp_path / "run_job.sh").write_text(runner, encoding="utf-8", newline="\n")
    install = entrypoint.split('echo "[worker] crontab installed:"', 1)[0].replace(
        "CRON_FILE=/etc/cron.d/sports", 'CRON_FILE="$WORKER_TEST_ROOT/cron"'
    ).replace(
        "ENV_FILE=/app/scripts/.worker_env", 'ENV_FILE="$WORKER_TEST_ROOT/worker.env"'
    ).replace(
        "/app/scripts/run_job.sh --validate-live-odds-jobs",
        '"$BASH" "$WORKER_TEST_ROOT/run_job.sh" --validate-live-odds-jobs',
    )
    return "chown() { :; }\n" + install


@pytest.fixture
def dispatch():
    return (SCRIPTS / "run_job.sh").read_text(encoding="utf-8").replace(
        "ENV_FILE=/app/scripts/.worker_env", 'ENV_FILE="$WORKER_TEST_ROOT/worker.env"'
    ).replace("cd /app || exit 1", 'cd "$WORKER_TEST_ROOT" || exit 1')


@pytest.mark.parametrize("job,schedule,module", PAPER_JOBS)
def test_opt_in_paper_job_dispatch_and_health(worker_shell, tmp_path, install_cron, dispatch, job, schedule, module):
    flag = f"WORKER_{job.upper()}_ENABLED"
    result = worker_shell(install_cron, **{flag: "1"})
    assert result.returncode == 0, result.stderr
    cron = (tmp_path / "cron").read_text(encoding="utf-8")
    assert [line.split(" >>", 1)[0] for line in cron.splitlines() if "run_job.sh paper_" in line] == [
        f"{schedule} * * * * app /app/scripts/run_job.sh {job}"
    ]

    mocked_timeout = (
        'timeout() { test -f worker.env || return 99; '
        'printf "%s\n" "$*"; return "$WORKER_TEST_STATUS"; }\n'
    )
    result = worker_shell(mocked_timeout + dispatch, job, WORKER_TEST_STATUS="0")
    assert result.returncode == 0, result.stderr
    logs = list((tmp_path / "logs").glob(f"{job}_*.log"))
    assert len(logs) == 1
    assert f"120s python -m {module}" in logs[0].read_text(encoding="utf-8").splitlines()
    assert (tmp_path / "heartbeat" / f"{job}.ok").exists()

    (tmp_path / "heartbeat" / f"{job}.ok").unlink()
    result = worker_shell(mocked_timeout + dispatch, job, WORKER_TEST_STATUS="17")
    assert result.returncode == 17, result.stderr
    health = "pgrep() { :; }\n" + (SCRIPTS / "worker_health.sh").read_text(encoding="utf-8")
    assert worker_shell(health).returncode == 0
    result = worker_shell(health, **{flag: "1"})
    assert result.returncode == 1
    assert f"FAIL: {job} last run failed" in result.stderr


@pytest.mark.parametrize(
    "opt_in,enabled",
    [
        (None, set()),
        ("", set()),
        ("train", {"train"}),
        ("predict", {"predict"}),
        ("predict,train", {"predict", "train"}),
        ("train,predict", {"predict", "train"}),
    ],
)
def test_only_explicit_jobs_get_live_odds(worker_shell, tmp_path, install_cron, dispatch, opt_in, enabled):
    overrides = {"WORKER_LIVE_ODDS_JOBS": opt_in} if opt_in is not None else {}
    result = worker_shell(install_cron, **overrides)
    assert result.returncode == 0, result.stderr
    assert 'ODDS_API_KEY=fake-key-must-not-enable-live-odds' in (
        tmp_path / "worker.env"
    ).read_text(encoding="utf-8")
    cron = (tmp_path / "cron").read_text(encoding="utf-8")
    assert "15 * * * *       app /app/scripts/run_job.sh predict" in cron
    assert "0 9 * * *          app /app/scripts/run_job.sh train" in cron

    stub = """pipeline_stub() {
    printf 'pipeline-arguments'
    for arg in "$@"; do printf ' %s' "$arg"; done
    printf '\\n'
}
"""
    for job, flags in [
        ("predict", ["--skip-training"]),
        ("train", []),
        ("benchmark", ["--skip-odds", "--skip-prediction"]),
    ]:
        result = worker_shell(stub + dispatch.replace("/app/scripts/pipeline.sh", "pipeline_stub"), job)
        assert result.returncode == 0, result.stderr
        log, = (tmp_path / "logs").glob(f"{job}_*.log")
        if job != "benchmark" and job not in enabled:
            flags.append("--skip-odds")
        assert " ".join(["pipeline-arguments", *flags]) in log.read_text(encoding="utf-8").splitlines()


@pytest.mark.parametrize("invalid", ["predcit", "train,", "train, predict", "predict,predict", "paper_close"])
def test_invalid_live_odds_opt_in_fails_closed(worker_shell, tmp_path, install_cron, dispatch, invalid):
    result = worker_shell(install_cron, WORKER_LIVE_ODDS_JOBS=invalid)
    assert result.returncode == 64, result.stderr
    assert "Invalid WORKER_LIVE_ODDS_JOBS" in result.stderr
    assert not (tmp_path / "cron").exists()

    stub = 'pipeline_stub() { echo unexpected-pipeline; }\n'
    result = worker_shell(
        stub + dispatch.replace("/app/scripts/pipeline.sh", "pipeline_stub"),
        "predict",
        WORKER_LIVE_ODDS_JOBS=invalid,
    )
    assert result.returncode == 64
    log, = (tmp_path / "logs").glob("predict_*.log")
    lines = log.read_text(encoding="utf-8").splitlines()
    assert any("Invalid WORKER_LIVE_ODDS_JOBS" in line for line in lines)
    assert "unexpected-pipeline" not in lines
    assert (tmp_path / "heartbeat" / "predict.fail").exists()

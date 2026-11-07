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


@pytest.mark.parametrize("job,schedule,module", PAPER_JOBS)
def test_opt_in_paper_job_dispatch_and_health(worker_shell, tmp_path, job, schedule, module):
    flag = f"WORKER_{job.upper()}_ENABLED"
    entrypoint = (SCRIPTS / "worker_entrypoint.sh").read_text(encoding="utf-8")
    install_cron = entrypoint.split('echo "[worker] crontab installed:"', 1)[0].replace(
        "CRON_FILE=/etc/cron.d/sports", 'CRON_FILE="$WORKER_TEST_ROOT/cron"'
    ).replace("ENV_FILE=/app/scripts/.worker_env", 'ENV_FILE="$WORKER_TEST_ROOT/worker.env"')
    result = worker_shell("chown() { :; }\n" + install_cron, **{flag: "1"})
    assert result.returncode == 0, result.stderr
    cron = (tmp_path / "cron").read_text(encoding="utf-8")
    assert [line.split(" >>", 1)[0] for line in cron.splitlines() if "run_job.sh paper_" in line] == [
        f"{schedule} * * * * app /app/scripts/run_job.sh {job}"
    ]

    dispatch = (SCRIPTS / "run_job.sh").read_text(encoding="utf-8").replace(
        "ENV_FILE=/app/scripts/.worker_env", 'ENV_FILE="$WORKER_TEST_ROOT/unset.env"'
    ).replace("cd /app || exit 1", 'cd "$WORKER_TEST_ROOT" || exit 1')
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

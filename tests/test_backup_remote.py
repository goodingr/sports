"""Exercise the backup shell entry points against a local AWS CLI stand-in."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BUCKET = "test-backups"
PREFIX = "trial"

MOCK_AWS = r"""
import fnmatch
import json
import os
import shutil
import sys
from pathlib import Path
from urllib.parse import urlsplit

args = sys.argv[1:]
args = args[args.index("s3") + 1:]
operation, *options = args
with open(os.environ["BACKUP_TEST_AWS_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(args) + "\n")

root = Path(os.environ["BACKUP_TEST_S3"]).resolve()

def path(value):
    if not value.startswith("s3://"):
        return Path(value)
    parsed = urlsplit(value)
    target = (root / parsed.netloc / parsed.path.lstrip("/")).resolve()
    if not target.is_relative_to(root):
        raise ValueError("invalid test object key")
    return target

def fail():
    print("injected AWS test failure", file=sys.stderr)
    raise SystemExit(17)

if operation == "sync":
    source, destination = options[:2]
    uploading = destination.startswith("s3://")
    fail_upload = os.environ.get("BACKUP_TEST_FAIL_UPLOAD")
    if fail_upload == "1" and uploading:
        fail()
    source, destination = path(source), path(destination)
    if not source.is_dir():
        fail()
    excludes = [options[i + 1] for i, value in enumerate(options) if value == "--exclude"]
    copied = set()
    destination.mkdir(parents=True, exist_ok=True)
    for item in source.rglob("*"):
        if not item.is_file():
            continue
        relative = item.relative_to(source).as_posix()
        if any(fnmatch.fnmatch(relative, pattern) for pattern in excludes):
            continue
        if fail_upload == "models" and uploading and relative.startswith("models/"):
            fail()
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target)
        copied.add(relative)
    if "--delete" in options:
        for item in destination.rglob("*"):
            if item.is_file() and item.relative_to(destination).as_posix() not in copied:
                item.unlink()
elif operation == "cp":
    source, destination = map(path, options[:2])
    if not source.is_file():
        fail()
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
elif operation == "ls":
    source = path(options[0])
    if not source.is_dir():
        fail()
    if "--recursive" in options:
        for item in sorted(source.rglob("*")):
            if item.is_file():
                print("2026-01-01 00:00:00", item.stat().st_size, item.relative_to(source).as_posix())
    else:
        for item in sorted(source.iterdir()):
            if item.is_dir():
                print("                           PRE", item.name + "/")
            else:
                print("2026-01-01 00:00:00", item.stat().st_size, item.name)
elif operation == "rm":
    target = path(options[0])
    if target.is_dir():
        shutil.rmtree(target)
    elif target.exists():
        target.unlink()
else:
    raise ValueError("unexpected AWS operation")
"""


def _bash() -> str:
    if os.name == "nt":
        git_bash = Path("C:/Program Files/Git/bin/bash.exe")
        if git_bash.exists():
            return str(git_bash)
    result = shutil.which("bash")
    if result is None:
        pytest.skip("Bash is required for the remote-backup shell contract")
    return result


@pytest.fixture
def remote(tmp_path):
    executable_dir = tmp_path / "mock bin"
    executable_dir.mkdir()
    implementation = executable_dir / "aws_mock.py"
    implementation.write_text(MOCK_AWS, encoding="utf-8")
    launcher = executable_dir / "aws"
    launcher.write_text(
        '#!/usr/bin/env bash\nexec "$BACKUP_TEST_PYTHON" "$BACKUP_TEST_AWS" "$@"\n',
        encoding="utf-8",
    )
    launcher.chmod(0o755)
    s3_root = tmp_path / "s3"
    archive = s3_root / BUCKET / PREFIX
    archive.mkdir(parents=True)
    backups = tmp_path / "local backup store"
    backups.mkdir()
    models = tmp_path / "live models"
    models.mkdir()
    (models / "model.pkl").write_bytes(b"live-model-must-not-replace-snapshot")
    environment = {
        **os.environ,
        "PATH": str(executable_dir) + os.pathsep + os.environ["PATH"],
        "BACKUP_TEST_PYTHON": sys.executable,
        "BACKUP_TEST_AWS": str(implementation),
        "BACKUP_TEST_S3": str(s3_root),
        "BACKUP_TEST_AWS_LOG": str(tmp_path / "aws.jsonl"),
        "BACKUP_DIR": str(backups),
        "MODELS_DIR": str(models),
        "BACKUP_S3_BUCKET": BUCKET,
        "BACKUP_S3_PREFIX": PREFIX,
        "BACKUP_S3_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "test-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
        "REMOTE_BACKUP_RETENTION_DAYS": "30",
    }
    environment.pop("BACKUP_S3_ENDPOINT", None)
    return archive, backups, environment


def _run(script: str, environment: dict[str, str], *arguments: str):
    return subprocess.run(
        [_bash(), "--noprofile", "--norc", str(PROJECT_ROOT / "scripts" / script), *arguments],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_worker_refuses_to_start_with_restore_recovery_files(remote):
    _, _, environment = remote
    marker = Path(environment["MODELS_DIR"]) / ".restore-interrupted"
    marker.mkdir()
    result = _run("worker_entrypoint.sh", environment)
    assert result.returncode == 1
    assert "interrupted restore requires recovery" in result.stderr


def test_worker_passes_local_backup_retention_into_cron_environment(remote, tmp_path):
    _, _, environment = remote
    captured_env = tmp_path / "worker.env"
    entrypoint = (PROJECT_ROOT / "scripts/worker_entrypoint.sh").read_text(encoding="utf-8")
    # Execute the real environment capture, stopping before installation of system cron.
    prefix, _ = entrypoint.split(
        "# ----------------------------------------------------------------------------\n# Build crontab", 1
    )
    prefix = prefix.replace(
        "ENV_FILE=/app/scripts/.worker_env", 'ENV_FILE="$BACKUP_TEST_ENV_FILE"'
    )
    command = (
        "chown() { :; }\n"
        + prefix
        + '\nunset BACKUP_RETENTION_DAYS\n. "$ENV_FILE"\n'
        + 'printf "%s" "${BACKUP_RETENTION_DAYS:-missing}"\n'
    )
    result = subprocess.run(
        [_bash(), "--noprofile", "--norc", "-c", command],
        env={
            **environment,
            "BACKUP_RETENTION_DAYS": "13",
            "BACKUP_TEST_ENV_FILE": str(captured_env),
            "LOG_DIR": str(tmp_path / "logs"),
            "HEARTBEAT_DIR": str(tmp_path / "heartbeat"),
        },
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == "13"
    assert "BACKUP_TEST_ENV_FILE" not in captured_env.read_text(encoding="utf-8")


def _database(path: Path, value: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE evidence (body TEXT)")
        connection.execute("INSERT INTO evidence VALUES (?)", (value,))


def _snapshot(path: Path, value: str, *, manifested: bool = True):
    path.mkdir(parents=True)
    _database(path / "betting.db", value)
    (path / "models").mkdir()
    (path / "models" / "model.pkl").write_bytes(value.encode())
    if manifested:
        files = {}
        for item in (path / "betting.db", path / "models" / "model.pkl"):
            content = item.read_bytes()
            files[item.relative_to(path).as_posix()] = {
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        (path / "manifest.json").write_text(
            json.dumps(
                {
                    "version": 1,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "files": files,
                }
            ),
            encoding="utf-8",
        )


def _pointer(path: Path) -> dict[str, str]:
    return dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)


def _publish_pointer(archive: Path, date: str, generation: str):
    text = f"snapshot_date={date}\nsnapshot_prefix={date}/{generation}\n"
    (archive / "latest.txt").write_text(text, encoding="utf-8")
    (archive / date / "latest.txt").write_text(text, encoding="utf-8")


def _restored_value(path: Path) -> str:
    with sqlite3.connect(path) as connection:
        return connection.execute("SELECT body FROM evidence").fetchone()[0]


def test_upload_uses_immutable_generations_and_frozen_models(remote):
    archive, backups, environment = remote
    first = backups / "2026-11-07_01-00-00_aaaa"
    _snapshot(first, "first")
    result = _run("backup_to_s3.sh", environment)
    assert result.returncode == 0, result.stdout + result.stderr
    first_pointer = _pointer(archive / "latest.txt")
    first_remote = archive / first_pointer["snapshot_prefix"]
    first_content = (first_remote / "betting.db").read_bytes()
    assert (first_remote / "models" / "model.pkl").read_bytes() == b"first"
    second = backups / "2026-11-07_01-00-01_bbbb"
    _snapshot(second, "second")
    result = _run("backup_to_s3.sh", environment)
    assert result.returncode == 0, result.stdout + result.stderr
    second_pointer = _pointer(archive / "latest.txt")
    assert second_pointer["snapshot_prefix"] != first_pointer["snapshot_prefix"]
    assert (first_remote / "betting.db").read_bytes() == first_content
    assert (
        archive / second_pointer["snapshot_prefix"] / "models/model.pkl"
    ).read_bytes() == b"second"
    assert _pointer(archive / second_pointer["snapshot_date"] / "latest.txt") == second_pointer


def test_failed_upload_preserves_latest_pointers(remote):
    archive, backups, environment = remote
    _snapshot(backups / "2026-11-07_01-00-00_aaaa", "first")
    result = _run("backup_to_s3.sh", environment)
    assert result.returncode == 0, result.stdout + result.stderr
    pointer_path = archive / "latest.txt"
    old_pointer = pointer_path.read_bytes()
    date = _pointer(pointer_path)["snapshot_date"]
    old_daily_pointer = (archive / date / "latest.txt").read_bytes()
    _snapshot(backups / "2026-11-07_01-00-01_bbbb", "second")
    result = _run("backup_to_s3.sh", {**environment, "BACKUP_TEST_FAIL_UPLOAD": "models"})
    assert result.returncode != 0
    assert pointer_path.read_bytes() == old_pointer
    assert (archive / date / "latest.txt").read_bytes() == old_daily_pointer


@pytest.mark.parametrize("explicit_date", [False, True])
def test_restore_resolves_generation_and_replaces_models_after_validation(
    remote, tmp_path, explicit_date
):
    archive, _, environment = remote
    date, generation = "2026-11-07", "generation-one"
    _snapshot(archive / date / generation, "restored")
    _publish_pointer(archive, date, generation)
    target = tmp_path / "restored data" / "betting.db"
    models = tmp_path / "restored models"
    models.mkdir()
    (models / "obsolete.pkl").write_bytes(b"old")
    arguments = ["--target-data", str(target), "--target-models", str(models), "--force"]
    if explicit_date:
        arguments += ["--date", date]
    result = _run("restore_from_s3.sh", environment, *arguments)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _restored_value(target) == "restored"
    assert (models / "model.pkl").read_bytes() == b"restored"
    assert not (models / "obsolete.pkl").exists()


@pytest.mark.parametrize("damage", ["database", "missing_model", "corrupt_model"])
def test_invalid_archive_does_not_touch_existing_targets(remote, tmp_path, damage):
    archive, _, environment = remote
    date, generation = "2026-11-07", "broken"
    source = archive / date / generation
    _snapshot(source, "new")
    _publish_pointer(archive, date, generation)
    if damage == "database":
        (source / "betting.db").write_bytes(b"corrupt")
    elif damage == "missing_model":
        (source / "models/model.pkl").unlink()
    else:
        (source / "models/model.pkl").write_bytes(b"wrong-model")
    target = tmp_path / "current.db"
    _database(target, "original")
    original_database = target.read_bytes()
    models = tmp_path / "current-models"
    models.mkdir()
    (models / "model.pkl").write_bytes(b"original-model")
    result = _run(
        "restore_from_s3.sh",
        environment,
        "--target-data",
        str(target),
        "--target-models",
        str(models),
        "--force",
    )
    assert result.returncode != 0
    assert target.read_bytes() == original_database
    assert (models / "model.pkl").read_bytes() == b"original-model"


@pytest.mark.parametrize("explicit_date", [False, True])
def test_legacy_flat_snapshots_and_old_latest_pointer_are_restorable(
    remote, tmp_path, explicit_date
):
    archive, _, environment = remote
    date = "2026-11-07"
    _snapshot(archive / date, "legacy", manifested=False)
    (archive / "latest.txt").write_text(f"snapshot_date={date}\n", encoding="utf-8")
    target, models = tmp_path / "legacy.db", tmp_path / "legacy-models"
    arguments = ["--target-data", str(target), "--target-models", str(models)]
    if explicit_date:
        arguments += ["--date", date]
    result = _run("restore_from_s3.sh", environment, *arguments)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _restored_value(target) == "legacy"
    assert (models / "model.pkl").read_bytes() == b"legacy"


def test_restore_refuses_existing_database_without_force(remote, tmp_path):
    archive, _, environment = remote
    _snapshot(archive / "2026-11-07", "new", manifested=False)
    (archive / "latest.txt").write_text("snapshot_date=2026-11-07\n", encoding="utf-8")
    target = tmp_path / "existing.db"
    _database(target, "existing")
    result = _run(
        "restore_from_s3.sh",
        environment,
        "--target-data",
        str(target),
        "--target-models",
        str(tmp_path / "models"),
    )
    assert result.returncode != 0
    assert _restored_value(target) == "existing"

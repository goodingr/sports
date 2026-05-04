"""End-to-end tests for scripts/backup_data.py.

We import the script as a module rather than shelling out so the test runs
on Windows dev boxes without a /usr/bin/env on PATH.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "backup_data.py"


def _load_backup_module():
    spec = importlib.util.spec_from_file_location("backup_data_under_test", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["backup_data_under_test"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def backup_module(monkeypatch, tmp_path):
    # Prevent the module from picking up the dev box's real ./backups dir.
    monkeypatch.setenv("BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.setenv("SPORTS_DB_PATH", str(tmp_path / "betting.db"))
    monkeypatch.setenv("BACKUP_RETENTION_DAYS", "3")
    return _load_backup_module()


def _seed_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE rows (id INTEGER PRIMARY KEY, body TEXT);
            INSERT INTO rows (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c');
            """
        )


def test_backup_writes_readable_snapshot(backup_module, tmp_path):
    db_path = tmp_path / "betting.db"
    backup_root = tmp_path / "backups"
    _seed_db(db_path)

    snapshot = backup_module.create_backup(db_path, backup_root, extras=[])

    assert snapshot.exists()
    snap_db = snapshot / "betting.db"
    assert snap_db.exists()

    # The online-backup output must round-trip. Crucially, the source DB
    # is left unmodified (no copy of -wal/-shm needed).
    with sqlite3.connect(snap_db) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        rows = conn.execute("SELECT id, body FROM rows ORDER BY id").fetchall()
    assert rows == [(1, "a"), (2, "b"), (3, "c")]


def test_backup_copies_extras_when_present(backup_module, tmp_path):
    db_path = tmp_path / "betting.db"
    backup_root = tmp_path / "backups"
    _seed_db(db_path)

    extra_present = tmp_path / "predictions_master.parquet"
    extra_present.write_bytes(b"PAR1")
    extra_missing = tmp_path / "does_not_exist.parquet"

    snapshot = backup_module.create_backup(
        db_path, backup_root, extras=[extra_present, extra_missing]
    )

    assert (snapshot / "predictions_master.parquet").read_bytes() == b"PAR1"
    assert not (snapshot / "does_not_exist.parquet").exists()


def test_backup_aborts_when_source_missing(backup_module, tmp_path):
    db_path = tmp_path / "betting.db"  # never created
    backup_root = tmp_path / "backups"

    with pytest.raises(FileNotFoundError):
        backup_module.create_backup(db_path, backup_root, extras=[])


def test_cleanup_keeps_only_retention_count(backup_module, tmp_path):
    backup_root = tmp_path / "backups"
    backup_root.mkdir()
    # Create five fake snapshots with strictly increasing mtimes.
    for i in range(5):
        d = backup_root / f"snap-{i:02d}"
        d.mkdir()
        (d / "betting.db").write_text("x")
        # Stagger the mtimes so the ordering is deterministic.
        ts = 1_700_000_000 + i * 60
        import os
        os.utime(d, (ts, ts))

    backup_module.cleanup_old_backups(backup_root, retention_days=2)

    surviving = sorted(p.name for p in backup_root.iterdir() if p.is_dir())
    # Newest two must remain; older three must be pruned.
    assert surviving == ["snap-03", "snap-04"]

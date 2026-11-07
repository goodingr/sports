"""End-to-end tests for scripts/backup_data.py.

We import the script as a module rather than shelling out so the test runs
on Windows dev boxes without a /usr/bin/env on PATH.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sqlite3
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
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
        conn.execute("PRAGMA journal_mode = WAL")
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


def test_main_preserves_project_raw_when_warehouse_is_outside_project(backup_module, tmp_path, monkeypatch):
    project = tmp_path / "project"
    marker = project / "data" / "raw" / "odds" / "original.json"
    marker.parent.mkdir(parents=True)
    marker.write_bytes(b'{"fetched_at":"2025-11-25T00:30:00Z"}')
    db_path = tmp_path / "external-warehouse" / "betting.db"
    _seed_db(db_path)
    decoy = db_path.parent / "raw" / "decoy.json"
    decoy.parent.mkdir()
    decoy.write_bytes(b"warehouse sibling")
    models = project / "models"
    models.mkdir()
    (models / "model.pkl").write_bytes(b"frozen model")
    monkeypatch.setattr(backup_module, "_PROJECT_ROOT", project)
    monkeypatch.setenv("SPORTS_DB_PATH", str(db_path))
    monkeypatch.setenv("MODELS_DIR", str(models))

    assert backup_module.main() == 0

    snapshot = next((tmp_path / "backups").iterdir())
    with zipfile.ZipFile(snapshot / "source_evidence.zip") as archive:
        assert archive.read("raw/odds/original.json") == marker.read_bytes()
        assert "raw/decoy.json" not in archive.namelist()


def test_backup_aborts_when_source_missing(backup_module, tmp_path):
    db_path = tmp_path / "betting.db"  # never created
    backup_root = tmp_path / "backups"

    with pytest.raises(FileNotFoundError):
        backup_module.create_backup(db_path, backup_root, extras=[])
    assert not backup_root.exists()


def test_failed_sidecar_copy_never_publishes_snapshot(backup_module, tmp_path, monkeypatch):
    db_path = tmp_path / "betting.db"
    backup_root = tmp_path / "backups"
    _seed_db(db_path)
    extra = tmp_path / "predictions_master.parquet"
    extra.write_bytes(b"PAR1")

    def failed_copy(source, destination):
        Path(destination).write_bytes(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(backup_module.shutil, "copy2", failed_copy)
    with pytest.raises(OSError, match="disk full"):
        backup_module.create_backup(db_path, backup_root, extras=[extra])
    assert list(backup_root.iterdir()) == []


def test_corrupt_source_never_publishes_or_prunes_last_good_backup(
    backup_module, tmp_path, monkeypatch
):
    db_path = tmp_path / "betting.db"
    backup_root = tmp_path / "backups"
    models = tmp_path / "models"
    models.mkdir()
    (models / "model.pkl").write_bytes(b"good-model")
    _seed_db(db_path)
    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute("CREATE INDEX rows_body ON rows(body)")
        conn.commit()
    good = backup_module.create_backup(db_path, backup_root, [], models_dir=models)
    good_bytes = (good / "betting.db").read_bytes()
    # A wrong index root page is copied by SQLite's backup API but fails integrity_check.
    with closing(sqlite3.connect(db_path)) as conn:
        table_page = conn.execute(
            "SELECT rootpage FROM sqlite_master WHERE name = 'rows'"
        ).fetchone()[0]
        conn.execute("PRAGMA writable_schema = ON")
        conn.execute(
            "UPDATE sqlite_master SET rootpage = ? WHERE name = 'rows_body'", (table_page,)
        )
        conn.commit()
    monkeypatch.setenv("BACKUP_RETENTION_DAYS", "1")
    monkeypatch.setenv("MODELS_DIR", str(models))
    monkeypatch.setattr(backup_module, "_extra_files_to_copy", lambda: [])

    assert backup_module.main() == 1

    assert list(backup_root.iterdir()) == [good]
    assert (good / "betting.db").read_bytes() == good_bytes
    with closing(sqlite3.connect(good / "betting.db")) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchall() == [("ok",)]


def test_missing_configured_models_never_publishes_or_prunes_last_good_backup(
    backup_module, tmp_path, monkeypatch
):
    db_path = tmp_path / "betting.db"
    backup_root = tmp_path / "backups"
    _seed_db(db_path)
    good = backup_module.create_backup(db_path, backup_root, [])
    monkeypatch.setenv("BACKUP_RETENTION_DAYS", "1")
    monkeypatch.setenv("MODELS_DIR", str(tmp_path / "missing-models"))
    monkeypatch.setattr(backup_module, "_extra_files_to_copy", lambda: [])

    assert backup_module.main() == 1

    assert list(backup_root.iterdir()) == [good]


def test_concurrent_backups_publish_unique_complete_snapshots(backup_module, tmp_path):
    db_path = tmp_path / "betting.db"
    backup_root = tmp_path / "backups"
    _seed_db(db_path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        snapshots = list(
            pool.map(
                lambda _: backup_module.create_backup(db_path, backup_root, extras=[]), range(2)
            )
        )
    assert snapshots[0] != snapshots[1]
    assert sorted(backup_root.iterdir()) == sorted(snapshots)
    for snapshot in snapshots:
        assert not snapshot.name.startswith(".")
        assert (snapshot / "manifest.json").is_file()
        with sqlite3.connect(snapshot / "betting.db") as conn:
            assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_manifest_covers_database_extras_and_frozen_models(backup_module, tmp_path):
    db_path = tmp_path / "betting.db"
    _seed_db(db_path)
    extra = tmp_path / "predictions_master.parquet"
    extra.write_bytes(b"PAR1")
    models = tmp_path / "models"
    (models / "nested").mkdir(parents=True)
    model = models / "nested" / "candidate.pkl"
    model.write_bytes(b"trained model")
    snapshot = backup_module.create_backup(
        db_path, tmp_path / "backups", extras=[extra], models_dir=models
    )
    model.write_bytes(b"next training run")

    manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    assert manifest["created_at"].endswith("+00:00")
    assert set(manifest["files"]) == {
        "betting.db",
        "predictions_master.parquet",
        "models/nested/candidate.pkl",
    }
    for filename, metadata in manifest["files"].items():
        contents = (snapshot / filename).read_bytes()
        assert metadata == {
            "size_bytes": len(contents),
            "sha256": hashlib.sha256(contents).hexdigest(),
        }
    assert (snapshot / "models/nested/candidate.pkl").read_bytes() == b"trained model"


@pytest.mark.parametrize("change", ["contents", "added_path", "removed_path"])
def test_model_changes_during_copy_never_publish_a_snapshot(
    backup_module, tmp_path, monkeypatch, change
):
    db_path = tmp_path / "betting.db"
    _seed_db(db_path)
    models = tmp_path / "models"
    models.mkdir()
    model = models / "candidate.pkl"
    model.write_bytes(b"old")
    original_mtime = model.stat().st_mtime_ns
    copytree = backup_module.shutil.copytree

    def copy_then_change(source, destination):
        result = copytree(source, destination)
        if change == "contents":
            model.write_bytes(b"new")
            os.utime(model, ns=(original_mtime, original_mtime + 1_000_000))
        elif change == "added_path":
            (models / "new.pkl").write_bytes(b"new model")
        else:
            model.unlink()
        return result

    monkeypatch.setattr(backup_module.shutil, "copytree", copy_then_change)
    backup_root = tmp_path / "backups"
    with pytest.raises(ValueError, match="models changed during backup"):
        backup_module.create_backup(db_path, backup_root, extras=[], models_dir=models)
    assert list(backup_root.iterdir()) == []


def test_restore_marker_prevents_publishing_models(backup_module, tmp_path):
    db_path = tmp_path / "betting.db"
    _seed_db(db_path)
    models = tmp_path / "models"
    models.mkdir()
    (models / ".restore-in-progress").write_text("interrupted restore", encoding="utf-8")
    (models / "candidate.pkl").write_bytes(b"model")
    backup_root = tmp_path / "backups"

    with pytest.raises(ValueError, match="interrupted restore"):
        backup_module.create_backup(db_path, backup_root, extras=[], models_dir=models)
    assert list(backup_root.iterdir()) == []


def test_cleanup_keeps_only_retention_count(backup_module, tmp_path):
    backup_root = tmp_path / "backups"
    backup_root.mkdir()
    # Create five fake snapshots with strictly increasing mtimes.
    for i in range(5):
        d = backup_root / f"2026-04-26_18-00-{i:02d}"
        d.mkdir()
        (d / "betting.db").write_text("x")
        # Stagger the mtimes so the ordering is deterministic.
        ts = 1_700_000_000 + i * 60
        os.utime(d, (ts, ts))

    backup_module.cleanup_old_backups(backup_root, retention_days=2)

    surviving = sorted(p.name for p in backup_root.iterdir() if p.is_dir())
    # Newest two must remain; older three must be pruned.
    assert surviving == ["2026-04-26_18-00-03", "2026-04-26_18-00-04"]


def test_cleanup_preserves_staging_unknown_and_malformed_directories(backup_module, tmp_path):
    backup_root = tmp_path / "backups"
    backup_root.mkdir()
    for name in [".staging", "unrelated", "2026-04-26_18-00-01", "2026-04-26_18-00-02"]:
        directory = backup_root / name
        directory.mkdir()
        (directory / "betting.db").write_bytes(b"db")
    malformed = backup_root / "2026-04-26_18-00-00"
    malformed.mkdir()
    (malformed / "betting.db").write_bytes(b"db")
    (malformed / "manifest.json").write_text("invalid", encoding="utf-8")
    for number, name in enumerate(["2026-04-26_18-00-01", "2026-04-26_18-00-02"]):
        os.utime(backup_root / name, (1_700_000_000 + number, 1_700_000_000 + number))

    backup_module.cleanup_old_backups(backup_root, retention_days=1)
    assert sorted(path.name for path in backup_root.iterdir()) == [
        ".staging",
        "2026-04-26_18-00-00",
        "2026-04-26_18-00-02",
        "unrelated",
    ]


@pytest.mark.parametrize("retention", [0, -1])
def test_nonpositive_retention_is_rejected_without_backing_up(
    backup_module, tmp_path, monkeypatch, retention
):
    monkeypatch.setenv("BACKUP_RETENTION_DAYS", str(retention))
    assert backup_module.main() == 1
    assert not (tmp_path / "backups").exists()
    with pytest.raises(ValueError, match="positive"):
        backup_module.cleanup_old_backups(tmp_path / "backups", retention)

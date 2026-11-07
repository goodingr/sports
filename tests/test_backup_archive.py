from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from scripts import backup_archive as archive


def _database(path: Path, value: str) -> None:
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE evidence (body TEXT)")
        conn.execute("INSERT INTO evidence VALUES (?)", (value,))
        conn.commit()


def _archive(path: Path) -> Path:
    path.mkdir()
    _database(path / "betting.db", "restored")
    (path / "models").mkdir()
    (path / "models/model.pkl").write_bytes(b"frozen-model")
    (path / "models/manifest.json").write_text('{"model":"metadata"}', encoding="utf-8")
    (path / "predictions_master.parquet").write_bytes(b"PAR1-frozen")
    archive.write_manifest(path)
    return path


def test_restore_uses_sqlite_transactions_with_existing_wal(tmp_path):
    source = _archive(tmp_path / "source")
    target = tmp_path / "live.db"
    _database(target, "old")
    with closing(sqlite3.connect(target)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("INSERT INTO evidence VALUES ('in-old-wal')")
        writer.commit()
        assert Path(str(target) + "-wal").stat().st_size > 0

        archive.install_archive(source, target, tmp_path / "models", force=True)
        assert writer.execute("SELECT body FROM evidence").fetchall() == [("restored",)]
    with closing(sqlite3.connect(target)) as conn:
        assert conn.execute("SELECT body FROM evidence").fetchall() == [("restored",)]
    assert (tmp_path / "predictions_master.parquet").read_bytes() == b"PAR1-frozen"
    assert (tmp_path / "models/manifest.json").read_bytes() == (
        source / "models/manifest.json"
    ).read_bytes()


def test_failed_database_restore_rolls_back_models_and_extras(tmp_path, monkeypatch):
    source = _archive(tmp_path / "source")
    target = tmp_path / "live.db"
    _database(target, "old")
    models = tmp_path / "models"
    models.mkdir()
    (models / "model.pkl").write_bytes(b"old-model")
    (models / "stale.pkl").write_bytes(b"keep-until-restore-succeeds")
    extra = tmp_path / "predictions_master.parquet"
    extra.write_bytes(b"old-predictions")

    def fail_restore(*args):
        raise OSError("injected database restore failure")

    monkeypatch.setattr(archive, "_restore_database", fail_restore)
    with pytest.raises(OSError, match="injected"):
        archive.install_archive(source, target, models, force=True)
    with closing(sqlite3.connect(target)) as conn:
        assert conn.execute("SELECT body FROM evidence").fetchall() == [("old",)]
    assert (models / "model.pkl").read_bytes() == b"old-model"
    assert (models / "stale.pkl").read_bytes() == b"keep-until-restore-succeeds"
    assert not (models / "manifest.json").exists()
    assert not list(models.glob(".restore-*"))
    assert extra.read_bytes() == b"old-predictions"
    assert not list(tmp_path.glob(".restore-*"))


def test_corrupt_nested_model_manifest_is_rejected_before_install(tmp_path):
    source = _archive(tmp_path / "source")
    (source / "models/manifest.json").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        archive.install_archive(source, tmp_path / "live.db", tmp_path / "models", force=False)
    assert not (tmp_path / "live.db").exists()
    assert not (tmp_path / "models").exists()


@pytest.mark.parametrize(
    "prefix", ["2026-11-06/../../elsewhere", "/outside", "2026-11-07/generation"]
)
def test_backup_pointer_rejects_unsafe_or_mismatched_paths(tmp_path, prefix):
    pointer = tmp_path / "latest.txt"
    pointer.write_text(f"snapshot_date=2026-11-06\nsnapshot_prefix={prefix}\n", encoding="utf-8")
    with pytest.raises(ValueError):
        archive.pointer_prefix(pointer)


def test_force_recovers_a_corrupt_target_and_preserves_damaged_bytes(tmp_path):
    source = _archive(tmp_path / "source")
    target = tmp_path / "live.db"
    target.write_bytes(b"not a SQLite database")
    archive.install_archive(source, target, tmp_path / "models", force=True)
    with closing(sqlite3.connect(target)) as conn:
        assert conn.execute("SELECT body FROM evidence").fetchall() == [("restored",)]
    saved = list(tmp_path.glob(".live.db.damaged-*/live.db"))
    assert len(saved) == 1
    assert saved[0].read_bytes() == b"not a SQLite database"


def test_restore_supports_file_and_directory_model_layout_changes(tmp_path):
    source = _archive(tmp_path / "source")
    (source / "models/new-directory").mkdir()
    (source / "models/new-directory/weights.pkl").write_bytes(b"new weights")
    archive.write_manifest(source)
    models = tmp_path / "models"
    models.mkdir()
    (models / "new-directory").write_bytes(b"previous flat artifact")
    (models / "model.pkl").mkdir()
    (models / "model.pkl/old.pkl").write_bytes(b"previous nested artifact")
    archive.install_archive(source, tmp_path / "live.db", models, force=True)
    assert (models / "new-directory/weights.pkl").read_bytes() == b"new weights"
    assert (models / "model.pkl").read_bytes() == b"frozen-model"


def test_interrupted_sqlite_copy_keeps_old_database_and_models(tmp_path, monkeypatch):
    source = _archive(tmp_path / "source")
    with closing(sqlite3.connect(source / "betting.db")) as conn:
        conn.execute("CREATE TABLE payload (body BLOB)")
        conn.execute("INSERT INTO payload VALUES (zeroblob(6000000))")
        conn.commit()
    archive.write_manifest(source)
    target = tmp_path / "live.db"
    _database(target, "old")
    models = tmp_path / "models"
    models.mkdir()
    (models / "model.pkl").write_bytes(b"old-model")

    class InterruptedConnection(sqlite3.Connection):
        def backup(self, destination, **kwargs):
            def abort(status, remaining, total):
                assert remaining > 0
                raise OSError("injected interruption during database copy")

            kwargs["progress"] = abort
            return super().backup(destination, **kwargs)

    connect = sqlite3.connect
    monkeypatch.setattr(
        archive.sqlite3,
        "connect",
        lambda *args, **kwargs: connect(*args, **kwargs, factory=InterruptedConnection),
    )
    with pytest.raises(OSError, match="interruption"):
        archive.install_archive(source, target, models, force=True)
    with closing(connect(target)) as conn:
        assert conn.execute("SELECT body FROM evidence").fetchall() == [("old",)]
    assert (models / "model.pkl").read_bytes() == b"old-model"
    assert not list(models.glob(".restore-*"))

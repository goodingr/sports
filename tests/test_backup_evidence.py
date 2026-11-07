import hashlib
import json
import sqlite3
import zipfile

import pytest

from scripts import backup_archive, backup_data, backup_evidence


def _source(tmp_path):
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    observation = raw / "observation.json"
    observation.write_bytes(b'{"captured_at":"2025-11-25T00:30:00Z","price":-110}')
    research = tmp_path / "reports" / "research"
    research.mkdir(parents=True)
    (research / "experiment.py").write_bytes(b"frozen_parameters = [1, 2]\n")
    benchmarks = tmp_path / "reports" / "betting_benchmarks"
    benchmarks.mkdir()
    (benchmarks / "after_raw_recovery.json").write_bytes(b'{"examined":true}')
    (benchmarks / "predictions.parquet").write_bytes(b"PAR1")
    return observation, [
        ("raw", raw), ("reports/research", research), ("reports/betting_benchmarks", benchmarks)
    ]


def _snapshot(tmp_path):
    observation, roots = _source(tmp_path)
    db = tmp_path / "data" / "betting.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE evidence (body TEXT)")
        conn.execute("INSERT INTO evidence VALUES ('record')")
    models = tmp_path / "models"
    models.mkdir()
    (models / "model.pkl").write_bytes(b"model")
    snapshot = backup_data.create_backup(
        db, tmp_path / "snapshots", [], models_dir=models, evidence_roots=roots
    )
    return snapshot, observation


def test_snapshot_and_restore_preserve_original_evidence_bytes(tmp_path):
    snapshot, observation = _snapshot(tmp_path)
    bundle = snapshot / backup_evidence.EVIDENCE_ZIP
    outer = json.loads((snapshot / "manifest.json").read_text())
    assert outer["files"][bundle.name]["sha256"] == hashlib.sha256(bundle.read_bytes()).hexdigest()
    with zipfile.ZipFile(bundle) as archive:
        assert archive.read("raw/observation.json") == observation.read_bytes()
        assert archive.read("reports/research/experiment.py") == b"frozen_parameters = [1, 2]\n"
        assert archive.read("reports/betting_benchmarks/after_raw_recovery.json") == b'{"examined":true}'
        assert "reports/betting_benchmarks/predictions.parquet" not in archive.namelist()
    target = tmp_path / "restored" / "betting.db"
    backup_archive.install_archive(snapshot, target, tmp_path / "restored-models", force=False)
    assert (target.parent / bundle.name).read_bytes() == bundle.read_bytes()


def test_restore_rejects_corrupt_evidence_and_keeps_prior_bundle(tmp_path):
    snapshot, _ = _snapshot(tmp_path)
    bundle = snapshot / backup_evidence.EVIDENCE_ZIP
    with zipfile.ZipFile(bundle) as archive:
        members = {name: archive.read(name) for name in archive.namelist()}
    members["raw/observation.json"] = b"changed"
    with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, contents in members.items():
            archive.writestr(name, contents)
    backup_archive.write_manifest(snapshot)
    target = tmp_path / "restored" / "betting.db"
    target.parent.mkdir()
    previous = target.parent / bundle.name
    previous.write_bytes(b"previous evidence")
    with pytest.raises(ValueError, match="evidence ZIP"):
        backup_archive.install_archive(snapshot, target, tmp_path / "restored-models", force=True)
    assert previous.read_bytes() == b"previous evidence"
    assert not target.exists()


def test_failed_database_install_rolls_back_evidence_bundle(tmp_path, monkeypatch):
    snapshot, _ = _snapshot(tmp_path)
    target = tmp_path / "restored" / "betting.db"
    target.parent.mkdir()
    previous = target.parent / backup_evidence.EVIDENCE_ZIP
    previous.write_bytes(b"previous evidence")

    def failed_database_install(*args):
        raise RuntimeError("injected install failure")

    monkeypatch.setattr(backup_archive, "_restore_database", failed_database_install)
    with pytest.raises(RuntimeError, match="injected install failure"):
        backup_archive.install_archive(snapshot, target, tmp_path / "restored-models", force=True)
    assert previous.read_bytes() == b"previous evidence"


@pytest.mark.parametrize("name", ["raw/../escape", "/raw/absolute", "raw/NUL.json", "raw/quote:stream", "raw\\escape"])
def test_evidence_zip_rejects_unsafe_member_names(tmp_path, name):
    path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(name, b"unsafe")
    if "\\" in name:
        path.write_bytes(path.read_bytes().replace(name.replace("\\", "/").encode(), name.encode()))
    with pytest.raises(ValueError, match="unsafe"):
        backup_evidence.validate_evidence_zip(path)


def test_evidence_zip_rejects_duplicate_names_and_size_limit(tmp_path, monkeypatch):
    path = tmp_path / "duplicate.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("raw/quote.json", b"one")
        with pytest.warns(UserWarning, match="Duplicate name"):
            archive.writestr("raw/quote.json", b"two")
    with pytest.raises(ValueError, match="duplicate"):
        backup_evidence.validate_evidence_zip(path)

    _, roots = _source(tmp_path)
    monkeypatch.setattr(backup_evidence, "MAX_MEMBER_BYTES", 4)
    with pytest.raises(ValueError, match="size limits"):
        backup_evidence.create_evidence_zip(tmp_path / "limited.zip", roots)


def test_evidence_capture_rejects_symlinks(tmp_path):
    observation, roots = _source(tmp_path)
    try:
        (observation.parent / "link.json").symlink_to(observation)
    except OSError:
        pytest.skip("Creating symlinks is unavailable")
    with pytest.raises(ValueError, match="symbolic link"):
        backup_evidence.create_evidence_zip(tmp_path / "links.zip", roots)


def test_evidence_change_aborts_snapshot_publication(tmp_path, monkeypatch):
    observation, roots = _source(tmp_path)
    db = tmp_path / "data" / "betting.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE evidence (body TEXT)")
    state = backup_evidence._source_state
    calls = 0

    def changed_state(path):
        nonlocal calls
        if path == observation:
            calls += 1
            if calls == 3:
                observation.write_bytes(b"different capture")
        return state(path)

    monkeypatch.setattr(backup_evidence, "_source_state", changed_state)
    with pytest.raises(ValueError, match="source changed"):
        backup_data.create_backup(db, tmp_path / "snapshots", [], evidence_roots=roots)
    assert list((tmp_path / "snapshots").iterdir()) == []

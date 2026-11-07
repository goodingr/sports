import hashlib
import json
import zipfile

import pytest

from scripts import restore_ncaab_scores
from scripts.backup_evidence import EVIDENCE_MANIFEST

FIRST = "raw/scores/NCAAB/scores_2026-11-08T23-04-05Z.json"
SECOND = "raw/scores/NCAAB/scores_2026-11-09T23-04-05Z.json"
OTHER = "raw/scores/NBA/scores_2026-11-08T23-04-05Z.json"


def _entry(content):
    return {"size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()}


def _archive(path, members, manifest=None):
    if manifest is None:
        manifest = {name: _entry(content) for name, content in members}
    with zipfile.ZipFile(path, "x") as archive:
        for name, content in members:
            archive.writestr(name, content)
        archive.writestr(EVIDENCE_MANIFEST, json.dumps({"version": 1, "files": manifest}))
    return path


def test_dry_run_reads_only_manifest_and_restore_preserves_exact_bytes(tmp_path, monkeypatch):
    archive = _archive(tmp_path / "source_evidence.zip", [
        (FIRST, b"first opaque original\x00"),
        (SECOND, b"second opaque original\r\n"),
        (OTHER, b"wrong league"),
        ("raw/scores/NCAAB/other/scores_2026-11-08T23-04-05Z.json", b"nested"),
        ("raw/scores/NCAAB/scores_2026-02-31T23-04-05Z.json", b"invalid date"),
    ])
    destination = tmp_path / "restored-scores"
    original_open = zipfile.ZipFile.open

    def manifest_only(self, name, *args, **kwargs):
        assert name == EVIDENCE_MANIFEST
        return original_open(self, name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(zipfile.ZipFile, "open", manifest_only)
        assert restore_ncaab_scores.restore_scores(archive) == {
            "status": "manifest_preview",
            "score_receipts": 2,
            "first_receipt_filename": FIRST.rsplit("/", 1)[1],
            "last_receipt_filename": SECOND.rsplit("/", 1)[1],
        }
    assert not destination.exists()
    assert restore_ncaab_scores.restore_scores(archive, destination)["status"] == "restored"
    assert {item.name: item.read_bytes() for item in destination.iterdir()} == {
        FIRST.rsplit("/", 1)[1]: b"first opaque original\x00",
        SECOND.rsplit("/", 1)[1]: b"second opaque original\r\n",
    }


def test_checksum_disagreement_leaves_no_destination(tmp_path):
    members = [(FIRST, b"first"), (SECOND, b"wrong")]
    archive = _archive(
        tmp_path / "source_evidence.zip",
        members,
        {FIRST: _entry(b"first"), SECOND: _entry(b"right")},
    )
    destination = tmp_path / "restored-scores"
    assert restore_ncaab_scores.restore_scores(archive)["score_receipts"] == 2
    with pytest.raises(ValueError, match="checksum failed"):
        restore_ncaab_scores.restore_scores(archive, destination)
    assert not destination.exists()
    assert not list(tmp_path.glob(".ncaab-scores-*"))


def test_corrupt_member_crc_leaves_no_destination(tmp_path):
    payload = b"opaque uncompressed member"
    archive = _archive(tmp_path / "source_evidence.zip", [(FIRST, payload)])
    archive.write_bytes(archive.read_bytes().replace(payload, b"x" + payload[1:], 1))
    destination = tmp_path / "restored-scores"
    with pytest.raises(ValueError, match="invalid evidence ZIP"):
        restore_ncaab_scores.restore_scores(archive, destination)
    assert not destination.exists()


def test_missing_member_and_missing_score_subset_fail_closed(tmp_path):
    missing = _archive(
        tmp_path / "missing.zip", [(FIRST, b"first")], {FIRST: _entry(b"first"), SECOND: _entry(b"missing")},
    )
    unrelated = _archive(tmp_path / "unrelated.zip", [(OTHER, b"wrong league")])
    destination = tmp_path / "restored-scores"
    with pytest.raises(ValueError, match="contents differ from the manifest"):
        restore_ncaab_scores.restore_scores(missing, destination)
    with pytest.raises(ValueError, match="no exact NCAAB score receipt"):
        restore_ncaab_scores.restore_scores(unrelated, destination)
    assert not destination.exists()


def test_duplicate_member_fails_before_writing(tmp_path):
    with pytest.warns(UserWarning, match="Duplicate name"):
        archive = _archive(tmp_path / "duplicate.zip", [(FIRST, b"first"), (FIRST, b"again")])
    destination = tmp_path / "restored-scores"
    with pytest.raises(ValueError, match="duplicate evidence ZIP member"):
        restore_ncaab_scores.restore_scores(archive, destination)
    assert not destination.exists()


def test_existing_directory_is_untouched(tmp_path):
    archive = _archive(tmp_path / "source_evidence.zip", [(FIRST, b"first")])
    destination = tmp_path / "existing"
    destination.mkdir()
    sentinel = destination / "existing-file"
    sentinel.write_bytes(b"keep")
    with pytest.raises(FileExistsError, match="destination already exists"):
        restore_ncaab_scores.restore_scores(archive, destination)
    assert list(destination.iterdir()) == [sentinel]
    assert sentinel.read_bytes() == b"keep"


def test_publication_failure_removes_partial_new_directory(tmp_path, monkeypatch):
    archive = _archive(tmp_path / "source_evidence.zip", [(FIRST, b"first")])
    destination = tmp_path / "restored-scores"

    def fail(source, target):
        target.write(source.read(1))
        raise OSError("synthetic write failure")

    monkeypatch.setattr(restore_ncaab_scores, "copyfileobj", fail)
    with pytest.raises(OSError, match="synthetic write failure"):
        restore_ncaab_scores.restore_scores(archive, destination)
    assert not destination.exists()
    assert not list(tmp_path.glob(".ncaab-scores-*"))

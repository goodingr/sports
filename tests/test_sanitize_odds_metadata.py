from __future__ import annotations

import copy
import json
import os
import stat
from pathlib import Path

import pytest

from scripts import sanitize_odds_metadata as sanitizer


def _seed_snapshot(path: Path) -> dict:
    payload = {
        "fetched_at": "2025-11-06T12:34:56.123456+00:00",
        "request": {
            "url": "https://api.example.test/sports/basketball_nba/odds",
            "params": {"apiKey": "credential-value", "regions": "us", "markets": "h2h,totals"},
        },
        "results": [
            {
                "id": "event",
                "name": "München",
                "line": 221.5,
                "score": None,
                "apiKey": "provider-field",
            }
        ],
        "rate_limit_remaining": "42",
    }
    path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    return payload


def test_default_dry_run_keeps_original_bytes(tmp_path, capsys):
    path = tmp_path / "odds.json"
    _seed_snapshot(path)
    original = path.read_bytes()
    assert sanitizer.main(["--root", str(tmp_path)]) == 0
    assert path.read_bytes() == original
    output = capsys.readouterr().out
    assert "Found 1 credential fields" in output
    assert "Rewrote 0 snapshots" in output
    assert "credential-value" not in output
    assert str(path) not in output


def test_apply_preserves_payload_and_filesystem_metadata(tmp_path):
    path = tmp_path / "odds.json"
    original = _seed_snapshot(path)
    os.chmod(path, 0o640)
    timestamp = 1_600_000_000_123_456_700
    os.utime(path, ns=(timestamp, timestamp))
    before = path.stat()

    result = sanitizer.sanitize_tree(tmp_path, apply=True)
    after = path.stat()
    expected = copy.deepcopy(original)
    del expected["request"]["params"]["apiKey"]

    assert result == sanitizer.Summary(scanned=1, affected=1, rewritten=1)
    assert after.st_atime_ns == before.st_atime_ns
    assert after.st_mtime_ns == before.st_mtime_ns
    assert stat.S_IMODE(after.st_mode) == stat.S_IMODE(before.st_mode)
    assert json.loads(path.read_bytes()) == expected
    assert list(tmp_path.iterdir()) == [path]


def test_clean_files_and_second_run_are_unchanged(tmp_path):
    dirty = tmp_path / "odds.json"
    _seed_snapshot(dirty)
    clean = tmp_path / "clean.json"
    clean.write_bytes(b'{ "request": { "params": { "markets": "totals" } }, "results": [] }\n')
    before_bytes, before_mtime = clean.read_bytes(), clean.stat().st_mtime_ns

    assert sanitizer.sanitize_tree(tmp_path, apply=True).rewritten == 1
    assert clean.read_bytes() == before_bytes
    assert clean.stat().st_mtime_ns == before_mtime
    sanitized_bytes, sanitized_mtime = dirty.read_bytes(), dirty.stat().st_mtime_ns

    assert sanitizer.sanitize_tree(tmp_path, apply=True) == sanitizer.Summary(scanned=2)
    assert dirty.read_bytes() == sanitized_bytes
    assert dirty.stat().st_mtime_ns == sanitized_mtime


def test_concurrent_content_change_aborts_without_overwriting(tmp_path, monkeypatch):
    path = tmp_path / "odds.json"
    _seed_snapshot(path)
    original_replace = sanitizer._replace_if_unchanged
    changed = path.read_bytes().replace(b'"42"', b'"41"')

    def mutate_before_replace(path, temporary, original, metadata):
        path.write_bytes(changed)
        os.utime(path, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
        original_replace(path, temporary, original, metadata)

    monkeypatch.setattr(sanitizer, "_replace_if_unchanged", mutate_before_replace)
    with pytest.raises(sanitizer.SanitizationError, match="Concurrent snapshot modification"):
        sanitizer.sanitize_tree(tmp_path, apply=True)
    assert path.read_bytes() == changed
    assert list(tmp_path.iterdir()) == [path]


def test_failed_replace_keeps_original_and_cleans_temporary(tmp_path, monkeypatch):
    path = tmp_path / "odds.json"
    _seed_snapshot(path)
    original = path.read_bytes()

    def unavailable(*args):
        raise PermissionError("simulated access failure")

    monkeypatch.setattr(sanitizer.os, "replace", unavailable)
    with pytest.raises(PermissionError):
        sanitizer.sanitize_tree(tmp_path, apply=True)
    assert path.read_bytes() == original
    assert list(tmp_path.iterdir()) == [path]


def test_invalid_json_reports_failure_without_content(tmp_path, capsys):
    path = tmp_path / "odds.json"
    path.write_text('{"apiKey": "credential-value",', encoding="utf-8")
    assert sanitizer.main(["--root", str(tmp_path), "--apply"]) == 1
    output = capsys.readouterr().out
    assert "credential-value" not in output
    assert str(path) not in output

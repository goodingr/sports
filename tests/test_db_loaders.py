"""Tests for database loader helpers."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from src.db import loaders


def _sqlite_context(db_path: Path):
    @contextmanager
    def _inner(path: Path | None = None) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    return _inner


def test_has_successful_source_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE data_sources (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_key TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE source_runs (
            run_id TEXT PRIMARY KEY,
            source_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT
        )
        """
    )
    conn.execute("INSERT INTO data_sources (source_key) VALUES ('demo_source')")
    conn.execute(
        "INSERT INTO source_runs (run_id, source_id, status) VALUES ('run1', 1, 'success')"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(loaders, "connect", _sqlite_context(db_path))

    assert loaders.has_successful_source_run("demo_source") is True
    assert loaders.has_successful_source_run("missing") is False

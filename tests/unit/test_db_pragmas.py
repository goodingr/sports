"""Verify the SQLite hardening pragmas applied by `src.db.core.connect`."""

from __future__ import annotations

import sqlite3

from src.db.core import connect, online_backup


def test_connect_applies_hardening_pragmas(tmp_path):
    db_path = tmp_path / "warehouse.db"

    with connect(db_path) as conn:
        # journal_mode survives across connections (it's persisted in the DB
        # header), so this query confirms our PRAGMA stuck.
        journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
        synchronous = conn.execute("PRAGMA synchronous").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        busy_timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]

    assert journal.lower() == "wal"
    # synchronous=NORMAL is `1`. FULL would be `2`.
    assert synchronous == 1
    assert foreign_keys == 1
    assert busy_timeout >= 5000


def test_foreign_keys_are_enforced(tmp_path):
    db_path = tmp_path / "warehouse.db"
    with connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE parent (id INTEGER PRIMARY KEY);
            CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL REFERENCES parent(id)
            );
            """
        )

    # Inserting a child whose parent_id has no match must now raise. If the
    # PRAGMA didn't stick, the insert would silently succeed.
    with connect(db_path) as conn:
        try:
            conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 999)")
            raised = False
        except sqlite3.IntegrityError:
            raised = True
    assert raised, "foreign key constraint should have rejected dangling parent_id"


def test_online_backup_produces_readable_copy(tmp_path):
    src_path = tmp_path / "src.db"
    dst_path = tmp_path / "snap" / "src.db.bak"

    with connect(src_path) as conn:
        conn.executescript(
            """
            CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL);
            INSERT INTO notes (id, body) VALUES (1, 'hello'), (2, 'world');
            """
        )

    online_backup(dst_path, db_path=src_path)

    assert dst_path.exists()
    # The backup must round-trip integrity_check and contain the rows we
    # wrote on the source side.
    with sqlite3.connect(dst_path) as conn:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        rows = conn.execute("SELECT id, body FROM notes ORDER BY id").fetchall()
    assert integrity == "ok"
    assert rows == [(1, "hello"), (2, "world")]

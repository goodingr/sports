"""Snapshot the warehouse + canonical predictions parquet for disaster recovery.

Invoked from cron via scripts/run_job.sh and as a pre-flight step inside
scripts/pipeline.sh. Writes timestamped subdirectories under $BACKUP_DIR
(default ./backups locally, /app/backups in the container) and prunes
anything older than RETENTION_DAYS so the disk doesn't fill up unnoticed.

Two design choices worth knowing about:

1. SQLite backup uses the *online backup API* (`Connection.backup`) instead
   of `shutil.copy`. The warehouse is written to live by the worker; a
   filesystem copy in the middle of a write produces a torn page that fails
   `PRAGMA integrity_check`. The online API copies page-by-page while
   readers/writers continue, which is the only safe way to snapshot a live
   database file. See https://www.sqlite.org/backup.html.

2. Paths are read from the same env vars the rest of the pipeline uses
   (SPORTS_DB_PATH, BACKUP_DIR), so the container's volumes (/app/data,
   /app/backups) and a local dev checkout (./data, ./backups) both work
   without any per-environment branching.

Exit codes:
    0  success
    1  source DB missing or backup failed
"""
from __future__ import annotations

import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make `src` importable when this script is run directly (e.g. cron does
# `python scripts/backup_data.py`, not `python -m`).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.db.core import online_backup, resolve_db_path  # noqa: E402


def _resolve_backup_root() -> Path:
    raw = os.getenv("BACKUP_DIR")
    if raw:
        path = Path(raw).expanduser()
        return path if path.is_absolute() else _PROJECT_ROOT / path
    return _PROJECT_ROOT / "backups"


def _resolve_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _extra_files_to_copy() -> list[Path]:
    """Parquet sidecars that aren't in the SQLite file but ship with snapshots."""
    return [
        _PROJECT_ROOT / "data" / "forward_test" / "ensemble" / "predictions_master.parquet",
    ]


def _format_size_mb(path: Path) -> str:
    try:
        size = path.stat().st_size
    except OSError:
        return "?"
    return f"{size / (1024 * 1024):.2f} MB"


def create_backup(
    db_path: Path,
    backup_root: Path,
    extras: list[Path],
) -> Path:
    """Create a timestamped snapshot directory and return its path."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    snapshot_dir = backup_root / timestamp
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    print(f"Creating backup in {snapshot_dir} ...")

    if not db_path.exists():
        print(f"ERROR: source database missing at {db_path}", file=sys.stderr)
        raise FileNotFoundError(db_path)

    db_target = snapshot_dir / "betting.db"
    print(f"  online-backup {db_path} -> {db_target}")
    online_backup(db_target, db_path=db_path)
    print(f"  database snapshot size: {_format_size_mb(db_target)}")

    for source in extras:
        if not source.exists():
            print(f"  skip {source} (not present)")
            continue
        target = snapshot_dir / source.name
        print(f"  copy  {source} -> {target}")
        shutil.copy2(source, target)

    return snapshot_dir


def cleanup_old_backups(backup_root: Path, retention_days: int) -> None:
    """Delete the oldest snapshots once we exceed `retention_days` directories.

    Retention is measured in count-of-snapshots rather than wall-clock days
    because the cadence is configurable via WORKER_BACKUP_CRON; if the cron
    runs hourly, this naturally keeps the last N backups regardless.
    """
    if not backup_root.exists():
        return

    snapshots = sorted(
        (d for d in backup_root.iterdir() if d.is_dir()),
        key=lambda d: d.stat().st_mtime,
    )

    if len(snapshots) <= retention_days:
        print("  no old backups to delete")
        return

    to_delete = snapshots[: len(snapshots) - retention_days]
    for old in to_delete:
        print(f"  pruning old snapshot: {old}")
        shutil.rmtree(old, ignore_errors=True)


def main() -> int:
    db_path = resolve_db_path()
    backup_root = _resolve_backup_root()
    retention_days = _resolve_int("BACKUP_RETENTION_DAYS", 7)

    backup_root.mkdir(parents=True, exist_ok=True)

    print(f"db_path     = {db_path}")
    print(f"backup_root = {backup_root}")
    print(f"retention   = {retention_days} snapshots")

    try:
        create_backup(db_path, backup_root, _extra_files_to_copy())
    except FileNotFoundError:
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"Backup failed: {exc!r}", file=sys.stderr)
        return 1

    print(f"Cleaning up; keeping last {retention_days} snapshots ...")
    cleanup_old_backups(backup_root, retention_days)
    print("Backup completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

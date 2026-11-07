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

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Make `src` importable when this script is run directly (e.g. cron does
# `python scripts/backup_data.py`, not `python -m`).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.backup_archive import validate_database  # noqa: E402
from scripts.backup_evidence import EVIDENCE_ZIP, create_evidence_zip  # noqa: E402
from src.db.core import online_backup, resolve_db_path  # noqa: E402


def _resolve_backup_root() -> Path:
    raw = os.getenv("BACKUP_DIR")
    if raw:
        path = Path(raw).expanduser()
        return path if path.is_absolute() else _PROJECT_ROOT / path
    return _PROJECT_ROOT / "backups"


def _resolve_models_dir() -> Path:
    raw = os.getenv("MODELS_DIR") or os.getenv("MODEL_DIR")
    path = Path(raw).expanduser() if raw else _PROJECT_ROOT / "models"
    return path if path.is_absolute() else _PROJECT_ROOT / path


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


def _evidence_roots() -> list[tuple[str, Path]]:
    return [
        ("raw", _PROJECT_ROOT / "data" / "raw"),
        ("reports/research", _PROJECT_ROOT / "reports" / "research"),
        ("reports/data_quality", _PROJECT_ROOT / "reports" / "data_quality"),
        ("reports/odds_coverage", _PROJECT_ROOT / "reports" / "odds_coverage"),
        ("reports/betting_benchmarks", _PROJECT_ROOT / "reports" / "betting_benchmarks"),
    ]


def _format_size_mb(path: Path) -> str:
    try:
        size = path.stat().st_size
    except OSError:
        return "?"
    return f"{size / (1024 * 1024):.2f} MB"


def _model_tree_state(models_dir: Path) -> dict[str, tuple[int, int]]:
    state = {}
    for path in [models_dir, *models_dir.rglob("*")]:
        metadata = path.stat()
        state[path.relative_to(models_dir).as_posix()] = (
            metadata.st_size,
            metadata.st_mtime_ns,
        )
    return state


def create_backup(
    db_path: Path,
    backup_root: Path,
    extras: list[Path],
    *,
    models_dir: Path | None = None,
    evidence_roots: list[tuple[str, Path]] | None = None,
) -> Path:
    """Publish a complete, uniquely named snapshot after all copies succeed."""
    if not db_path.is_file():
        print(f"ERROR: source database missing at {db_path}", file=sys.stderr)
        raise FileNotFoundError(db_path)
    if models_dir is not None and not models_dir.is_dir():
        print(f"ERROR: source models directory missing at {models_dir}", file=sys.stderr)
        raise FileNotFoundError(models_dir)

    created_at = datetime.now(timezone.utc)
    timestamp = created_at.strftime("%Y-%m-%d_%H-%M-%S_%f")
    snapshot_name = f"{timestamp}_{uuid.uuid4().hex[:12]}"
    backup_root.mkdir(parents=True, exist_ok=True)
    snapshot_dir = backup_root / snapshot_name
    staging_dir = Path(tempfile.mkdtemp(prefix=f".{snapshot_name}.", dir=backup_root))
    print(f"Creating backup in {staging_dir} ...")

    try:
        db_target = staging_dir / "betting.db"
        print(f"  online-backup {db_path} -> {db_target}")
        online_backup(db_target, db_path=db_path)
        validate_database(db_target)
        print(f"  database snapshot size: {_format_size_mb(db_target)}")

        for source in extras:
            if not source.exists():
                print(f"  skip {source} (not present)")
                continue
            target = staging_dir / source.name
            if target.exists() or source.name in {"manifest.json", "models", EVIDENCE_ZIP}:
                raise ValueError(f"duplicate or reserved backup filename: {source.name}")
            print(f"  copy  {source} -> {target}")
            shutil.copy2(source, target)

        if models_dir is not None:
            if any(models_dir.glob(".restore-*")):
                raise ValueError("cannot back up models during an interrupted restore")
            if staging_dir.resolve().is_relative_to(models_dir.resolve()):
                raise ValueError("backup staging directory cannot be inside the models directory")
            model_state = _model_tree_state(models_dir)
            print(f"  copy models {models_dir} -> {staging_dir / 'models'}")
            shutil.copytree(models_dir, staging_dir / "models")
            if model_state != _model_tree_state(models_dir):
                raise ValueError("models changed during backup; retry after training finishes")

        if evidence_roots is not None:
            evidence = create_evidence_zip(staging_dir / EVIDENCE_ZIP, evidence_roots)
            print(f"  source evidence: {evidence['files']} files, {evidence['zip_bytes']} compressed bytes")

        files = {}
        for path in sorted(staging_dir.rglob("*")):
            if path.is_file():
                with path.open("rb") as handle:
                    digest = hashlib.file_digest(handle, "sha256").hexdigest()
                files[path.relative_to(staging_dir).as_posix()] = {
                    "size_bytes": path.stat().st_size,
                    "sha256": digest,
                }
        manifest = {"version": 1, "created_at": created_at.isoformat(), "files": files}
        with (staging_dir / "manifest.json").open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        staging_dir.rename(snapshot_dir)
    except BaseException:
        if staging_dir.resolve().parent != backup_root.resolve():
            raise RuntimeError("backup staging directory moved outside the backup root") from None
        if staging_dir.exists():
            shutil.rmtree(staging_dir)
        raise

    print(f"Published complete backup in {snapshot_dir}")
    return snapshot_dir


def _is_completed_snapshot(path: Path) -> bool:
    if not path.is_dir() or path.is_symlink() or path.name.startswith("."):
        return False
    if not (path / "betting.db").is_file():
        return False
    manifest_path = path / "manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            return (
                isinstance(manifest, dict)
                and manifest.get("version") == 1
                and isinstance(manifest.get("files"), dict)
                and isinstance(manifest["files"].get("betting.db"), dict)
                and isinstance(manifest.get("created_at"), str)
            )
        except (OSError, ValueError):
            return False
    return re.fullmatch(r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", path.name) is not None


def cleanup_old_backups(backup_root: Path, retention_days: int) -> None:
    """Delete the oldest snapshots once we exceed `retention_days` directories.

    Retention is measured in count-of-snapshots rather than wall-clock days
    because the cadence is configurable via WORKER_BACKUP_CRON; if the cron
    runs hourly, this naturally keeps the last N backups regardless.
    """
    if retention_days <= 0:
        raise ValueError("backup retention must be a positive snapshot count")
    if not backup_root.exists():
        return

    resolved_root = backup_root.resolve()
    snapshots = sorted(
        (
            path
            for path in backup_root.iterdir()
            if path.resolve().parent == resolved_root and _is_completed_snapshot(path)
        ),
        key=lambda path: (path.stat().st_mtime_ns, path.name),
    )

    if len(snapshots) <= retention_days:
        print("  no old backups to delete")
        return

    to_delete = snapshots[: len(snapshots) - retention_days]
    for old in to_delete:
        print(f"  pruning old snapshot: {old}")
        shutil.rmtree(old)


def main() -> int:
    db_path = resolve_db_path()
    backup_root = _resolve_backup_root()
    retention_days = _resolve_int("BACKUP_RETENTION_DAYS", 7)
    if retention_days <= 0:
        print("ERROR: BACKUP_RETENTION_DAYS must be positive", file=sys.stderr)
        return 1

    backup_root.mkdir(parents=True, exist_ok=True)

    print(f"db_path     = {db_path}")
    print(f"backup_root = {backup_root}")
    print(f"retention   = {retention_days} snapshots")

    try:
        create_backup(
            db_path, backup_root, _extra_files_to_copy(), models_dir=_resolve_models_dir(),
            evidence_roots=_evidence_roots(),
        )
        print(f"Cleaning up; keeping last {retention_days} snapshots ...")
        cleanup_old_backups(backup_root, retention_days)
    except FileNotFoundError:
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"Backup failed: {exc!r}", file=sys.stderr)
        return 1

    print("Backup completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Validate backup archives and install an offline warehouse restore."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.backup_evidence import EVIDENCE_ZIP, validate_evidence_zip  # noqa: E402

MANIFEST = "manifest.json"
SNAPSHOT_NAME = re.compile(r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}(?:_[A-Za-z0-9_]+)?")


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _files(root: Path) -> dict[str, Path]:
    files = {}
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"archive contains a symbolic link: {path.relative_to(root)}")
        if path.is_file() and path.relative_to(root).as_posix() != MANIFEST:
            files[path.relative_to(root).as_posix()] = path
    return files


def write_manifest(root: Path) -> None:
    files = _files(root)
    payload = {
        "version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "files": {
            name: {"size_bytes": path.stat().st_size, "sha256": _digest(path)}
            for name, path in sorted(files.items())
        },
    }
    (root / MANIFEST).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def validate_database(path: Path) -> None:
    """Check a closed SQLite snapshot before it is published or restored."""
    db_uri = path.resolve().as_uri() + "?mode=ro&immutable=1"
    with closing(sqlite3.connect(db_uri, uri=True)) as conn:
        result = conn.execute("PRAGMA integrity_check").fetchall()
    if result != [("ok",)]:
        raise ValueError("archive database integrity check failed")


def validate_archive(root: Path, *, allow_legacy: bool = False) -> None:
    if root.is_symlink():
        raise ValueError("archive root cannot be a symbolic link")
    files = _files(root)
    if "betting.db" not in files:
        raise ValueError("archive has no betting.db")
    if not any(name.startswith("models/") and path.stat().st_size for name, path in files.items()):
        raise ValueError("archive has no nonempty model artifacts")
    if any(name.startswith("models/") and path.stat().st_size == 0 for name, path in files.items()):
        raise ValueError("archive contains an empty model artifact")
    if any((root / "models").glob(".restore-*")):
        raise ValueError("archive contains an interrupted model restore")

    manifest_path = root / MANIFEST
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            not isinstance(manifest, dict)
            or manifest.get("version") != 1
            or not isinstance(manifest.get("files"), dict)
        ):
            raise ValueError("unsupported archive manifest")
        expected = manifest["files"]
        if expected.keys() != files.keys():
            raise ValueError("archive file list differs from the manifest")
        for name, entry in expected.items():
            relative = PurePosixPath(name)
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("unsafe archive manifest path")
            path = files[name]
            if (
                not isinstance(entry, dict)
                or path.stat().st_size != entry.get("size_bytes")
                or _digest(path) != entry.get("sha256")
            ):
                raise ValueError(f"archive checksum failed: {name}")
    elif not allow_legacy:
        raise ValueError("archive manifest is missing")

    validate_database(root / "betting.db")
    evidence = root / EVIDENCE_ZIP
    if evidence.is_file():
        validate_evidence_zip(evidence)


def latest_local(root: Path) -> Path:
    snapshots = [
        path
        for path in root.iterdir()
        if path.is_dir()
        and not path.is_symlink()
        and SNAPSHOT_NAME.fullmatch(path.name)
        and (path / "betting.db").is_file()
    ]
    if not snapshots:
        raise ValueError(f"no completed snapshot found under {root}")
    return max(snapshots, key=lambda path: path.name)


def prepare_legacy(source: Path, target: Path, models: Path) -> None:
    shutil.copytree(source, target, dirs_exist_ok=True)
    if not (target / "models").is_dir():
        if not models.is_dir():
            raise ValueError("legacy snapshot requires a models directory")
        shutil.copytree(models, target / "models")
    write_manifest(target)
    validate_archive(target)


def _copy_database(source: Path, target: Path) -> None:
    """Use SQLite transactions so existing WAL files stay consistent."""
    started = time.monotonic()

    def progress(status: int, remaining: int, total: int) -> None:
        if (
            status in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
            and time.monotonic() - started > 10
        ):
            raise TimeoutError("database is busy; stop API and worker before restoring")

    with closing(
        sqlite3.connect(source.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
    ) as src:
        with closing(sqlite3.connect(target, timeout=5)) as dst:
            dst.execute("PRAGMA synchronous = FULL")
            src.backup(dst, pages=1024, progress=progress, sleep=0.1)


def _restore_database(source: Path, target: Path) -> None:
    try:
        _copy_database(source, target)
        return
    except sqlite3.DatabaseError as error:
        if getattr(error, "sqlite_errorcode", None) not in (
            sqlite3.SQLITE_CORRUPT,
            sqlite3.SQLITE_NOTADB,
        ):
            raise
    quarantine = Path(tempfile.mkdtemp(prefix=f".{target.name}.damaged-", dir=target.parent))
    moved = []
    try:
        for suffix in ("", "-wal", "-shm", "-journal"):
            previous = Path(str(target) + suffix)
            if previous.exists():
                saved = quarantine / previous.name
                os.replace(previous, saved)
                moved.append((previous, saved))
        _copy_database(source, target)
    except Exception:
        try:
            for suffix in ("", "-wal", "-shm", "-journal"):
                Path(str(target) + suffix).unlink(missing_ok=True)
            for previous, saved in moved:
                os.replace(saved, previous)
            quarantine.rmdir()
        except Exception as error:
            raise RuntimeError(
                f"damaged database recovery files retained in {quarantine}"
            ) from error
        raise
    print(f"Preserved damaged database in {quarantine}")


def _prune_empty_model_directories(root: Path, stage: Path) -> None:
    directories = [
        path
        for path in root.rglob("*")
        if path.is_dir() and path != stage and stage not in path.parents
    ]
    for path in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        if not any(path.iterdir()):
            path.rmdir()


def install_archive(
    source: Path, target_db: Path, target_models: Path, *, force: bool, allow_legacy: bool = False
) -> None:
    validate_archive(source, allow_legacy=allow_legacy)
    target_db = target_db.resolve()
    target_models = target_models.resolve()
    if (
        target_models in target_db.parents
        or target_db == target_models
        or source.resolve() == target_models
        or source.resolve() in target_models.parents
        or target_models in source.resolve().parents
        or source.resolve() in target_db.parents
    ):
        raise ValueError("restore source and destination paths overlap")
    current_models = (
        [path for path in target_models.rglob("*") if path.is_file()]
        if target_models.exists()
        else []
    )
    if not force and (target_db.exists() or current_models):
        raise ValueError("restore targets exist; use --force for an offline restore")
    if any(path.is_symlink() for path in target_models.rglob("*")):
        raise ValueError("restore target contains symbolic links")
    if any(target_models.glob(".restore-*")):
        raise ValueError("an interrupted restore needs recovery before retrying")

    extras = [path for path in source.glob("*.parquet") if path.is_file()]
    if (source / EVIDENCE_ZIP).is_file():
        extras.append(source / EVIDENCE_ZIP)
    if any(target_db.parent / path.name in {target_db, target_models} for path in extras):
        raise ValueError("restore evidence destination overlaps a database or model target")
    if any((target_db.parent / path.name).is_symlink() for path in extras):
        raise ValueError("restore evidence target contains a symbolic link")
    if not force and any((target_db.parent / path.name).exists() for path in extras):
        raise ValueError("restore sidecars exist; use --force for an offline restore")
    target_db.parent.mkdir(parents=True, exist_ok=True)
    target_models.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".restore-", dir=target_models))
    old = stage / "old"
    new = stage / "new"
    installed = False
    original_db = target_db.exists()
    mutated = False
    rollback_complete = False
    old_extras = stage / "old-extras"
    new_extras: dict[Path, Path] = {}
    try:
        shutil.copytree(source / "models", new)
        for path in current_models:
            backup = old / path.relative_to(target_models)
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, backup)
        for path in extras:
            destination = target_db.parent / path.name
            if destination.exists():
                old_extras.mkdir(exist_ok=True)
                shutil.copy2(destination, old_extras / path.name)
            descriptor, temporary = tempfile.mkstemp(prefix=".restore-", dir=target_db.parent)
            os.close(descriptor)
            pending = Path(temporary)
            new_extras[destination] = pending
            shutil.copy2(path, pending)
        staged_models = [path for path in new.rglob("*") if path.is_file()]
        mutated = True
        for path in current_models:
            path.unlink()
        _prune_empty_model_directories(target_models, stage)
        for path in staged_models:
            destination = target_models / path.relative_to(new)
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(path, destination)
        for destination, pending in new_extras.items():
            os.replace(pending, destination)
        _restore_database(source / "betting.db", target_db)
        installed = True
    except Exception:
        if mutated:
            try:
                for path in (source / "models").rglob("*"):
                    if path.is_file():
                        destination = target_models / path.relative_to(source / "models")
                        if destination.is_file():
                            destination.unlink()
                _prune_empty_model_directories(target_models, stage)
                for path in old.rglob("*"):
                    if path.is_file():
                        destination = target_models / path.relative_to(old)
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(path, destination)
                for destination in new_extras:
                    previous = old_extras / destination.name
                    if previous.exists():
                        shutil.copy2(previous, destination)
                    else:
                        destination.unlink(missing_ok=True)
                if not original_db:
                    target_db.unlink(missing_ok=True)
                rollback_complete = True
            except Exception as rollback_error:
                raise RuntimeError(
                    f"restore rollback failed; recovery files retained in {stage}"
                ) from rollback_error
        raise
    finally:
        for pending in new_extras.values():
            pending.unlink(missing_ok=True)
        if installed or rollback_complete or not mutated:
            shutil.rmtree(stage)


def pointer_prefix(path: Path) -> str:
    values = dict(
        line.split("=", 1) for line in path.read_text(encoding="utf-8").splitlines() if "=" in line
    )
    date = values.get("snapshot_date", "")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        raise ValueError("invalid snapshot date in backup pointer")
    datetime.strptime(date, "%Y-%m-%d")
    prefix = values.get("snapshot_prefix", date)
    if not re.fullmatch(re.escape(date) + r"(?:/[A-Za-z0-9][A-Za-z0-9_.-]*)?", prefix):
        raise ValueError("invalid snapshot prefix in backup pointer")
    return prefix


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    select = sub.add_parser("latest")
    select.add_argument("root", type=Path)
    verify = sub.add_parser("verify")
    verify.add_argument("source", type=Path)
    verify.add_argument("--allow-legacy", action="store_true")
    prepare = sub.add_parser("prepare-legacy")
    prepare.add_argument("source", type=Path)
    prepare.add_argument("target", type=Path)
    prepare.add_argument("models", type=Path)
    pointer = sub.add_parser("pointer")
    pointer.add_argument("file", type=Path)
    restore = sub.add_parser("install")
    restore.add_argument("source", type=Path)
    restore.add_argument("db", type=Path)
    restore.add_argument("models", type=Path)
    restore.add_argument("--force", action="store_true")
    restore.add_argument("--allow-legacy", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "latest":
            print(latest_local(args.root).as_posix())
        elif args.command == "verify":
            validate_archive(args.source, allow_legacy=args.allow_legacy)
        elif args.command == "prepare-legacy":
            prepare_legacy(args.source, args.target, args.models)
        elif args.command == "pointer":
            print(pointer_prefix(args.file))
        elif args.command == "install":
            install_archive(
                args.source, args.db, args.models, force=args.force, allow_legacy=args.allow_legacy
            )
    except (OSError, ValueError, sqlite3.Error, TimeoutError, RuntimeError) as error:
        print(f"Archive operation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()

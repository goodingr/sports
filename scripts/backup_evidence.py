"""Capture original observations and research artifacts in a verified ZIP."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import IO

EVIDENCE_ZIP = "source_evidence.zip"
EVIDENCE_MANIFEST = "evidence_manifest.json"
MAX_MEMBER_BYTES = 4 * 1024**3
MAX_TOTAL_BYTES = 64 * 1024**3
MAX_MANIFEST_BYTES = 64 * 1024**2
MAX_COMPRESSION_RATIO = 10_000
_RESERVED = {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}


def _safe_name(name: str) -> None:
    parts = name.split("/")
    if (
        not name or PurePosixPath(name).is_absolute() or "\\" in name
        or any(part in {"", ".", ".."} or ":" in part or any(ord(char) < 32 for char in part) for part in parts)
        or any(part.rstrip(" .") != part or part.split(".")[0].lower() in _RESERVED for part in parts)
    ):
        raise ValueError(f"unsafe evidence archive path: {name}")
    if name != EVIDENCE_MANIFEST and not (
        len(parts) >= 2 and parts[0] == "raw"
        or len(parts) >= 3 and parts[:2] in [
            ["reports", "research"], ["reports", "data_quality"], ["reports", "odds_coverage"],
            ["reports", "betting_benchmarks"],
        ]
    ):
        raise ValueError(f"unsupported evidence archive path: {name}")


def _stat_state(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns


def _source_state(path: Path) -> tuple[int, int, int, int, int]:
    metadata = path.lstat()
    if path.is_symlink() or getattr(metadata, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0):
        raise ValueError(f"evidence source contains a symbolic link: {path}")
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"evidence source is not a regular file: {path}")
    return _stat_state(metadata)


@contextmanager
def _open_evidence_source(
    path: Path, expected: tuple[int, int, int, int, int]
) -> Iterator[IO[bytes]]:
    if _source_state(path) != expected:
        raise ValueError(f"evidence source changed during capture: {path}")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    with os.fdopen(os.open(path, flags), "rb") as source:
        if _stat_state(os.fstat(source.fileno())) != expected:
            raise ValueError("evidence source changed before it was opened")
        yield source
    if _source_state(path) != expected:
        raise ValueError(f"evidence source changed during capture: {path}")


def _measure_member(source: IO[bytes], output: IO[bytes] | None = None) -> dict[str, int | str]:
    digest = hashlib.sha256()
    size = 0
    while chunk := source.read(1024 * 1024):
        digest.update(chunk)
        size += len(chunk)
        if output is not None:
            output.write(chunk)
    return {"size_bytes": size, "sha256": digest.hexdigest()}


def _append_inherited_members(
    target: zipfile.ZipFile, inherited: Path, files: dict[str, dict[str, int | str]]
) -> None:
    local_names = {name.casefold(): name for name in files}
    try:
        with (
            _open_evidence_source(inherited, _source_state(inherited)) as source,
            zipfile.ZipFile(source) as archive,
        ):
            for name, entry in _zip_manifest(archive)["files"].items():
                local_name = local_names.get(name.casefold())
                if local_name is not None and local_name != name:
                    raise ValueError(f"inherited evidence path case conflicts with local file: {name}")
                with archive.open(name) as member:
                    if local_name is not None:
                        observed = _measure_member(member)
                    else:
                        with target.open(name, "w", force_zip64=True) as output:
                            observed = _measure_member(member, output)
                if (
                    observed["size_bytes"] != entry["size_bytes"]
                    or observed["sha256"] != entry.get("sha256")
                ):
                    raise ValueError(f"inherited evidence ZIP checksum failed: {name}")
                if local_name is not None and observed != files[local_name]:
                    raise ValueError(f"inherited evidence differs from local file: {name}")
                files[name] = observed
    except (zipfile.BadZipFile, KeyError, json.JSONDecodeError) as error:
        raise ValueError("invalid inherited evidence ZIP") from error


def create_evidence_zip(
    target: Path, roots: list[tuple[str, Path]], *, inherited: Path | None = None
) -> dict:
    """Freeze verified inherited members and stable local files in one archive."""
    sources = {}
    names = set()
    for prefix, root in roots:
        if root.is_symlink() or root.exists() and getattr(root.lstat(), "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0):
            raise ValueError(f"evidence source root is a symbolic link: {root}")
        if not root.exists():
            continue
        if not root.is_dir():
            raise ValueError(f"evidence source root is not a directory: {root}")
        if target.resolve().is_relative_to(root.resolve()):
            raise ValueError("evidence ZIP cannot be inside a source directory")
        for path in sorted(root.rglob("*")):
            if path.is_symlink() or getattr(path.lstat(), "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0):
                raise ValueError(f"evidence source contains a symbolic link: {path}")
            if not path.is_file() or "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
                continue
            if prefix in {"reports/data_quality", "reports/odds_coverage", "reports/betting_benchmarks"} and path.suffix != ".json":
                continue
            name = f"{prefix}/{path.relative_to(root).as_posix()}"
            _safe_name(name)
            if name.casefold() in names:
                raise ValueError("duplicate evidence archive path")
            names.add(name.casefold())
            sources[name] = (path, _source_state(path))
    files = {}
    with zipfile.ZipFile(target, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for name, (path, expected) in sources.items():
            with (
                _open_evidence_source(path, expected) as source,
                archive.open(name, "w", force_zip64=True) as output,
            ):
                files[name] = _measure_member(source, output)
            if files[name]["size_bytes"] != expected[2]:
                raise ValueError(f"evidence source changed during capture: {path}")
        if inherited is not None:
            _append_inherited_members(archive, inherited, files)
        for path, expected in sources.values():
            if _source_state(path) != expected:
                raise ValueError(f"evidence source changed during capture: {path}")
        manifest = {"version": 1, "created_at": datetime.now(timezone.utc).isoformat(), "files": files}
        archive.writestr(EVIDENCE_MANIFEST, json.dumps(manifest, sort_keys=True))
    validate_evidence_zip(target)
    return {"files": len(files), "source_bytes": sum(row["size_bytes"] for row in files.values()), "zip_bytes": target.stat().st_size}


def _zip_manifest(archive: zipfile.ZipFile) -> dict:
    members = archive.infolist()
    names = set()
    total = 0
    for member in members:
        _safe_name(member.orig_filename)
        _safe_name(member.filename)
        mode = stat.S_IFMT(member.external_attr >> 16)
        if (
            member.is_dir() or mode not in {0, stat.S_IFREG} or member.flag_bits & 1
            or member.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
        ):
            raise ValueError("unsupported evidence ZIP member type")
        if member.filename.casefold() in names:
            raise ValueError("duplicate evidence ZIP member")
        names.add(member.filename.casefold())
        if member.file_size > MAX_MEMBER_BYTES or member.file_size > max(member.compress_size, 1) * MAX_COMPRESSION_RATIO:
            raise ValueError("evidence ZIP member exceeds size limits")
        total += member.file_size
        if total > MAX_TOTAL_BYTES:
            raise ValueError("evidence ZIP exceeds total size limit")
    info = archive.getinfo(EVIDENCE_MANIFEST)
    if info.file_size > MAX_MANIFEST_BYTES:
        raise ValueError("evidence manifest exceeds size limit")
    manifest = json.loads(archive.read(EVIDENCE_MANIFEST))
    if not isinstance(manifest, dict) or manifest.get("version") != 1 or not isinstance(manifest.get("files"), dict):
        raise ValueError("unsupported evidence manifest")
    files = manifest["files"]
    if set(files) != {member.filename for member in members} - {EVIDENCE_MANIFEST}:
        raise ValueError("evidence ZIP contents differ from the manifest")
    for name, entry in files.items():
        _safe_name(name)
        if not isinstance(entry, dict) or archive.getinfo(name).file_size != entry.get("size_bytes"):
            raise ValueError("evidence ZIP member size differs from the manifest")
    return manifest


def validate_evidence_zip(path: Path) -> None:
    try:
        with zipfile.ZipFile(path) as archive:
            manifest = _zip_manifest(archive)
            for name, entry in manifest["files"].items():
                with archive.open(name) as source:
                    observed = _measure_member(source)
                if observed["sha256"] != entry.get("sha256"):
                    raise ValueError(f"evidence ZIP checksum failed: {name}")
    except (zipfile.BadZipFile, KeyError, json.JSONDecodeError) as error:
        raise ValueError("invalid evidence ZIP") from error


def read_raw_evidence(stored_path: str, data_dir: Path) -> bytes:
    """Resolve captured metadata without rewriting its original stored path."""
    normalized = stored_path.replace("\\", "/")
    if normalized.startswith("raw/"):
        name = normalized
    else:
        original = Path(stored_path)
        if original.is_file():
            return original.read_bytes()
        marker = "/data/raw/"
        if marker not in normalized:
            raise FileNotFoundError(stored_path)
        name = "raw/" + normalized.split(marker, 1)[1]
    _safe_name(name)
    relocated = data_dir / PurePosixPath(name)
    if relocated.is_file():
        return relocated.read_bytes()
    bundle = data_dir / EVIDENCE_ZIP
    if not bundle.is_file():
        raise FileNotFoundError(stored_path)
    try:
        with zipfile.ZipFile(bundle) as archive:
            manifest = _zip_manifest(archive)
            contents = archive.read(name)
            if hashlib.sha256(contents).hexdigest() != manifest["files"][name].get("sha256"):
                raise ValueError("restored raw evidence checksum failed")
            return contents
    except (zipfile.BadZipFile, KeyError, json.JSONDecodeError) as error:
        raise ValueError("invalid restored raw evidence ZIP") from error

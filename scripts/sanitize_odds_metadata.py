"""Remove saved Odds API credentials while preserving the archived odds payloads."""

from __future__ import annotations

import argparse
import json
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_ROOT = Path(__file__).resolve().parent.parent / "data" / "raw" / "odds"


class SanitizationError(RuntimeError):
    """A snapshot could not be safely sanitized."""


@dataclass
class Summary:
    scanned: int = 0
    affected: int = 0
    rewritten: int = 0


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = dict(pairs)
    if len(result) != len(pairs):
        raise SanitizationError("A snapshot has duplicate JSON keys")
    return result


def _parse_snapshot(content: bytes) -> Any:
    try:
        return json.loads(content.decode("utf-8-sig"), object_pairs_hook=_unique_object)
    except (UnicodeError, ValueError):
        raise SanitizationError("A snapshot contains invalid JSON") from None


def _sanitized_content(original: bytes) -> bytes | None:
    payload = _parse_snapshot(original)
    if not isinstance(payload, dict):
        return None
    request = payload.get("request")
    params = request.get("params") if isinstance(request, dict) else None
    if not isinstance(params, dict) or "apiKey" not in params:
        return None
    del params["apiKey"]
    sanitized = json.dumps(payload, indent=2, allow_nan=False).encode("utf-8")
    if _parse_snapshot(sanitized) != payload:
        raise SanitizationError("A sanitized snapshot failed payload verification")
    return sanitized


def _fingerprint(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_mode,
    )


def _replace_if_unchanged(
    path: Path, temporary: Path, original: bytes, metadata: os.stat_result
) -> None:
    current = path.lstat()
    if _fingerprint(current) != _fingerprint(metadata) or path.read_bytes() != original:
        raise SanitizationError("Concurrent snapshot modification detected")
    if _fingerprint(path.lstat()) != _fingerprint(metadata):
        raise SanitizationError("Concurrent snapshot modification detected")
    os.replace(temporary, path)


def _sanitize_file(path: Path, root: Path, *, apply: bool) -> bool:
    if not path.resolve().is_relative_to(root):
        raise SanitizationError("A snapshot resolves outside the selected root")
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode):
        raise SanitizationError("A snapshot is not a regular file")
    original = path.read_bytes()
    if _fingerprint(path.lstat()) != _fingerprint(metadata):
        raise SanitizationError("Concurrent snapshot modification detected")
    sanitized = _sanitized_content(original)
    if sanitized is None:
        return False
    if not apply:
        return True

    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=".sanitized-", delete=False
        ) as out:
            temporary = Path(out.name)
            out.write(sanitized)
            out.flush()
            os.fsync(out.fileno())
        os.chmod(temporary, stat.S_IMODE(metadata.st_mode))
        os.utime(temporary, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
        _replace_if_unchanged(path, temporary, original, metadata)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return True


def sanitize_tree(root: Path = DEFAULT_ROOT, *, apply: bool = False) -> Summary:
    root = root.resolve()
    if not root.is_dir():
        raise SanitizationError("The odds snapshot root must be an existing directory")
    summary = Summary()
    for path in sorted(root.rglob("*.json")):
        summary.scanned += 1
        if _sanitize_file(path, root, apply=apply):
            summary.affected += 1
            summary.rewritten += int(apply)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument(
        "--apply", action="store_true", help="Apply the verified credential removal"
    )
    args = parser.parse_args(argv)
    try:
        result = sanitize_tree(args.root, apply=args.apply)
    except SanitizationError as exc:
        print(f"Sanitization failed. {exc}")
        return 1
    except (OSError, ValueError):
        print("Sanitization failed while reading or replacing a snapshot")
        return 1
    print(
        f"Scanned {result.scanned} snapshots. Found {result.affected} credential fields. "
        f"Rewrote {result.rewritten} snapshots."
    )
    if not args.apply:
        print("Dry run. Use --apply to remove the credential metadata.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

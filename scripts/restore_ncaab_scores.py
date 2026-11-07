"""Materialize verified original NCAA basketball score receipts for frozen review."""

from __future__ import annotations

import argparse
import json
import re
import zipfile
from datetime import datetime
from pathlib import Path
from shutil import copyfileobj
from tempfile import TemporaryDirectory

from scripts.backup_evidence import (
    _measure_member,
    _open_evidence_source,
    _source_state,
    _zip_manifest,
)

SCORE_MEMBER = re.compile(r"raw/scores/NCAAB/(scores_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)\.json)")


def _score_members(manifest: dict) -> list[tuple[str, str, dict]]:
    members = []
    for name, entry in sorted(manifest["files"].items()):
        match = SCORE_MEMBER.fullmatch(name)
        if match is None:
            continue
        try:
            datetime.strptime(match[2], "%Y-%m-%dT%H-%M-%SZ")
        except ValueError:
            continue
        digest = entry.get("sha256")
        if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise ValueError("invalid NCAAB score receipt checksum declaration")
        members.append((name, match[1], entry))
    if not members:
        raise ValueError("no exact NCAAB score receipt members in the evidence ZIP")
    return members


def _stage_scores(archive_path: Path, stage: Path | None) -> dict[str, int | str]:
    try:
        with (
            _open_evidence_source(archive_path, _source_state(archive_path)) as source,
            zipfile.ZipFile(source) as archive,
        ):
            members = _score_members(_zip_manifest(archive))
            if stage is not None:
                for name, filename, expected in members:
                    with archive.open(name) as member, (stage / filename).open("xb") as output:
                        observed = _measure_member(member, output)
                    if any(observed[key] != expected.get(key) for key in ("size_bytes", "sha256")):
                        raise ValueError(f"NCAAB score receipt checksum failed: {filename}")
    except (KeyError, UnicodeError, json.JSONDecodeError, zipfile.BadZipFile) as error:
        raise ValueError("invalid evidence ZIP or manifest") from error
    return {
        "score_receipts": len(members),
        "first_receipt_filename": members[0][1],
        "last_receipt_filename": members[-1][1],
    }


def _install_scores(stage: Path, destination: Path) -> None:
    destination.mkdir()
    created = []
    try:
        for staged in sorted(stage.iterdir()):
            target = destination / staged.name
            with target.open("xb") as output:
                created.append(target)
                with staged.open("rb") as source:
                    copyfileobj(source, output)
    except BaseException:
        for target in created:
            target.unlink()
        destination.rmdir()
        raise


def restore_scores(archive_path: Path, destination: Path | None = None) -> dict[str, int | str]:
    """Preview ZIP manifest names, or verify and write the exact receipt bytes."""
    if destination is None:
        return {"status": "manifest_preview", **_stage_scores(archive_path, None)}
    destination = destination.parent.resolve(strict=True) / destination.name
    if destination.exists() or destination.is_symlink():
        raise FileExistsError("NCAAB score destination already exists")
    with TemporaryDirectory(prefix=".ncaab-scores-", dir=destination.parent) as folder:
        stage = Path(folder)
        summary = _stage_scores(archive_path, stage)
        _install_scores(stage, destination)
    return {"status": "restored", **summary}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True, help="Verified source_evidence.zip")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--dry-run", action="store_true", help="Inspect manifest names only")
    action.add_argument("--destination", type=Path, help="New score directory under an existing parent")
    args = parser.parse_args()
    try:
        summary = restore_scores(args.archive, args.destination)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"{exc}\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

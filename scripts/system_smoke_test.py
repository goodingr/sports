"""Lightweight smoke test covering ingestion, dataset build, training, and dashboard imports."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run_step(name: str, command: list[str]) -> bool:
    print(f"[CHECK] {name}")
    try:
        result = subprocess.run(command, cwd=PROJECT_ROOT, capture_output=True, text=True, check=False)
    except Exception as exc:  # pragma: no cover - subprocess failure
        print(f"  [X] {exc}")
        return False

    if result.returncode != 0:
        print("  [X] Command failed:")
        print(f"      {' '.join(command)}")
        print(f"      stdout: {result.stdout.strip()}")
        print(f"      stderr: {result.stderr.strip()}")
        return False

    print("  [OK]")
    return True


def main() -> None:
    python = sys.executable
    steps = [
        (
            "Dry-run multi-source ingest (NFL)",
            [python, "-m", "src.data.ingest_sources", "--league", "nfl", "--dry-run"],
        ),
        (
            "Moneyline dataset CLI reachable",
            [python, "-m", "src.features.moneyline_dataset", "--help"],
        ),
        (
            "Training CLI reachable",
            [python, "-m", "src.models.train", "--help"],
        ),
        (
            "Forward-test CLI reachable",
            [python, "-m", "src.models.forward_test", "--help"],
        ),
        (
            "Dashboard module import",
            [python, "-c", "from src.dashboard import app, data; print('dashboard import ok')"],
        ),
    ]

    passed = all(_run_step(name, cmd) for name, cmd in steps)
    print()
    if passed:
        print("[SUCCESS] Smoke test passed. You can proceed with full ingestion/training.")
        sys.exit(0)
    print("[FAIL] Smoke test failed. Review errors above.")
    sys.exit(1)


if __name__ == "__main__":
    main()

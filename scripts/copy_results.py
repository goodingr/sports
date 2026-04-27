"""Copy completed forward-test results from ensemble output to model outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


RESULT_COLUMNS = ("home_score", "away_score", "result", "result_updated_at")


def _completed_results(ensemble: pd.DataFrame) -> pd.DataFrame:
    missing = {"game_id", *RESULT_COLUMNS}.difference(ensemble.columns)
    if missing:
        raise ValueError(f"ensemble predictions are missing columns: {sorted(missing)}")

    completed = ensemble[
        ensemble["result"].notna()
        | (ensemble["home_score"].notna() & ensemble["away_score"].notna())
    ]
    return completed[["game_id", *RESULT_COLUMNS]].drop_duplicates("game_id", keep="last")


def copy_results(base_dir: str | Path = Path("data") / "forward_test") -> dict[str, Any]:
    """Copy known game results into each non-ensemble forward-test parquet file.

    The ensemble `predictions_master.parquet` is treated as the source of truth for
    completed game results. Target rows are matched by `game_id`; games without a
    completed source result are left unchanged.
    """

    base_path = Path(base_dir)
    source_path = base_path / "ensemble" / "predictions_master.parquet"
    if not source_path.exists():
        raise FileNotFoundError(f"ensemble predictions not found: {source_path}")

    results = _completed_results(pd.read_parquet(source_path))
    result_lookup = results.set_index("game_id")
    stats: dict[str, Any] = {
        "source": str(source_path),
        "completed_results": int(len(results)),
        "updated_files": 0,
        "updated_rows": 0,
    }

    for target_path in sorted(base_path.glob("*/predictions_master.parquet")):
        if target_path == source_path:
            continue

        target = pd.read_parquet(target_path)
        if "game_id" not in target.columns:
            continue

        matched = target["game_id"].isin(result_lookup.index)
        if not matched.any():
            continue

        updated = target.copy()
        for column in RESULT_COLUMNS:
            if column not in updated.columns:
                updated[column] = pd.NA
            updated.loc[matched, column] = updated.loc[matched, "game_id"].map(
                result_lookup[column]
            )

        updated.to_parquet(target_path, index=False)
        stats["updated_files"] += 1
        stats["updated_rows"] += int(matched.sum())

    return stats


if __name__ == "__main__":
    print(copy_results())

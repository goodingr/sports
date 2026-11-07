"""Review the untouched NCAA totals offset cohort after the fixed final cutoff."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from . import experiment

FIT = Path(__file__).with_name("frozen_fit.json")


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_text(encoding="utf-8").encode()).hexdigest()


def _validate_fit(fit: dict, protocol: dict, start: datetime, end: datetime) -> float:
    beta = fit.get("beta")
    try:
        frozen = datetime.fromisoformat(fit["fit_frozen_at_utc"])
        registered = datetime.fromisoformat(protocol["frozen_at_utc"])
        lower, _ = protocol["model"]["beta_bounds_inclusive"]
        if (
            not registered <= frozen < start or not isinstance(beta, (int, float)) or not lower <= beta < 0
            or fit.get("status") != "development_fit_nonzero"
            or fit.get("protocol_id") != protocol["protocol_id"]
            or fit.get("development_end_exclusive_utc") != protocol["development"]["end_exclusive_utc"]
            or fit.get("protocol_sha256") != _digest(experiment.PROTOCOL)
            or fit.get("experiment_sha256") != _digest(Path(experiment.__file__))
            or fit.get("review_sha256") != _digest(Path(__file__))
            or fit.get("holdout_first_capture_utc") != start.isoformat()
            or fit.get("holdout_end_exclusive_utc") != end.isoformat()
            or fit.get("holdout_review_not_before_utc") != protocol["prospective_holdout"]["review_not_before_utc"]
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise ValueError("The fit and review inputs do not match the preregistered frozen artifact") from None
    return float(beta)


def _paired_metrics(nonpush, beta: float, *, bootstrap_resamples: int, bootstrap_seed: int):
    dates = [quote.kickoff.astimezone(experiment.EASTERN).date() for quote, _ in nonpush]
    unique_dates = sorted(set(dates))
    if not dates:
        return {"eastern_dates": 0}
    date_codes = {day: index for index, day in enumerate(unique_dates)}
    clusters = np.array([date_codes[day] for day in dates])
    offset = np.array([math.log(q.over / (1 - q.over)) for q, _ in nonpush])
    imbalance = np.array([abs(math.log(q.home / (1 - q.home))) for q, _ in nonpush])
    labels = np.array([float(y) for _, y in nonpush])
    forecast = offset + beta * imbalance
    loss_delta = np.logaddexp(0, forecast) - np.logaddexp(0, offset) - labels * (forecast - offset)
    model_prob = np.exp(-np.logaddexp(0, -forecast))
    brier_delta = np.square(model_prob - labels) - np.square(np.array([q.over for q, _ in nonpush]) - labels)
    date_sums = np.bincount(clusters, weights=loss_delta)
    date_sizes = np.bincount(clusters)
    draws = np.random.default_rng(bootstrap_seed).integers(
        0, len(unique_dates), size=(bootstrap_resamples, len(unique_dates))
    )
    resampled = date_sums[draws].sum(axis=1) / date_sizes[draws].sum(axis=1)
    return {
        "eastern_dates": len(unique_dates),
        "paired_log_loss_delta": float(np.mean(loss_delta)),
        "paired_log_loss_delta_ci95": [float(x) for x in np.quantile(resampled, [0.025, 0.975])],
        "paired_brier_delta": float(np.mean(brier_delta)),
    }


def review(db: Path, score_dir: Path, *, now: datetime) -> dict:
    protocol = json.loads(experiment.PROTOCOL.read_text(encoding="utf-8"))
    holdout = protocol["prospective_holdout"]
    review_at = datetime.fromisoformat(holdout["review_not_before_utc"])
    if now < review_at:
        return {"status": "review_not_yet_open", "review_not_before_utc": review_at.isoformat()}

    start = datetime.fromisoformat(holdout["first_quote_and_kickoff_not_before_utc"])
    end = datetime.fromisoformat(holdout["quote_and_kickoff_end_exclusive_utc"])
    fit = json.loads(FIT.read_text(encoding="utf-8"))
    beta = _validate_fit(fit, protocol, start, end)
    with closing(sqlite3.connect(db.as_uri() + "?mode=ro", uri=True)) as conn:
        quotes, quote_counts = experiment._quotes(conn, db.parent, protocol, start, end, first_capture=start)
    scores, score_counts = experiment._score_evidence(score_dir, quotes, protocol, start, review_at)
    nonpush = [
        (quote, scores[key] > quote.line)
        for key, quote in quotes.items() if key in scores and scores[key] != quote.line
    ]
    metrics = _paired_metrics(
        nonpush,
        bootstrap_resamples=holdout["bootstrap_resamples"],
        bootstrap_seed=holdout["bootstrap_seed"],
        beta=beta,
    )
    complete = (
        len(scores) == len(quotes)
        and not quote_counts["ambiguous_quote_fixtures"]
        and not quote_counts["unverified_odds_registrations"]
        and not score_counts.get("unverified_score_receipts", 0)
    )
    enough = (
        len(nonpush) >= holdout["minimum_non_push_fixtures"]
        and metrics["eastern_dates"] >= holdout["minimum_eastern_date_clusters"]
    )
    passed = complete and enough and metrics["paired_log_loss_delta_ci95"][1] < 0
    return {
        "status": "passed" if passed else "did_not_pass",
        "scope": "frozen_archive_quote_replay",
        "protocol_id": protocol["protocol_id"],
        "beta": beta,
        "reviewed_evidence_through_exclusive_utc": review_at.isoformat(),
        "complete": complete,
        "minimum_sample_met": enough,
        "paired_quote_fixtures": len(quotes),
        "matched_final_fixtures": len(scores),
        "nonpush_fixtures": len(nonpush),
        "push_fixtures": len(scores) - len(nonpush),
        **quote_counts,
        **{key: score_counts[key] for key in ("missing_score_fixtures", "ambiguous_score_fixtures")},
        **metrics,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--score-dir", type=Path)
    args = parser.parse_args()
    db = args.db.resolve()
    try:
        result = review(db, args.score_dir or db.parent / "raw" / "scores" / "NCAAB", now=datetime.now(timezone.utc))
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(1, f"{exc}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

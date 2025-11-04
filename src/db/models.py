"""Persist model artefacts to SQLite."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd

from .core import connect


LOGGER = logging.getLogger(__name__)


def register_model(entry: Dict[str, Any], metrics: Dict[str, Any]) -> None:
    seasons = entry.get("seasons", [])
    seasons_start = min(seasons) if seasons else None
    seasons_end = max(seasons) if seasons else None

    with connect() as conn:
        conn.execute(
            """
            INSERT INTO models (
                model_id, trained_at, model_type, calibration,
                seasons_start, seasons_end, features, dataset_hash,
                metrics_json, artifact_path, predictions_path, league
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_id) DO UPDATE SET
                trained_at = excluded.trained_at,
                model_type = excluded.model_type,
                calibration = excluded.calibration,
                seasons_start = excluded.seasons_start,
                seasons_end = excluded.seasons_end,
                features = excluded.features,
                dataset_hash = excluded.dataset_hash,
                metrics_json = excluded.metrics_json,
                artifact_path = excluded.artifact_path,
                predictions_path = excluded.predictions_path,
                league = excluded.league
            """,
            (
                entry["model_id"],
                entry.get("trained_at"),
                entry.get("model_type"),
                entry.get("calibration"),
                seasons_start,
                seasons_end,
                json.dumps(entry.get("features", [])),
                entry.get("dataset_hash"),
                json.dumps(metrics),
                entry.get("model_path"),
                entry.get("predictions_path"),
                entry.get("league"),
            ),
        )


def _get_team_id(conn, team_code: str) -> int | None:
    row = conn.execute(
        "SELECT team_id FROM teams WHERE code = ?",
        (team_code,),
    ).fetchone()
    if row:
        return row[0]
    LOGGER.warning("Team code %s not found in database; skipping prediction", team_code)
    return None


def persist_model_predictions(model_id: str, predictions: pd.DataFrame) -> None:
    if predictions.empty:
        LOGGER.info("No predictions to persist for model %s", model_id)
        return

    with connect() as conn:
        for row in predictions.to_dict("records"):
            team_code = str(row.get("team")) if row.get("team") is not None else ""
            game_id = row.get("game_id")
            if not team_code or not game_id:
                continue

            team_id = _get_team_id(conn, team_code.upper())
            if team_id is None:
                continue

            market_prob = row.get("implied_prob")
            probability = row.get("predicted_prob")
            edge = None
            if probability is not None and market_prob is not None:
                edge = float(probability) - float(market_prob)

            conn.execute(
                """
                INSERT OR REPLACE INTO model_predictions (
                    model_id, game_id, team_id, probability, market_prob, edge
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    model_id,
                    game_id,
                    team_id,
                    probability,
                    market_prob,
                    edge,
                ),
            )

        LOGGER.info("Persisted %d predictions for model %s", len(predictions), model_id)


def find_model_id_by_predictions_path(predictions_path: str) -> Optional[str]:
    with connect() as conn:
        row = conn.execute(
            "SELECT model_id FROM models WHERE predictions_path = ? ORDER BY trained_at DESC LIMIT 1",
            (predictions_path,),
        ).fetchone()
        if row:
            return row[0]
    return None


def persist_recommendations(
    model_id: str,
    recommendations: pd.DataFrame,
    recommended_at: datetime,
    snapshot_id: Optional[str] = None,
) -> None:
    if recommendations.empty:
        LOGGER.info("No recommendations to persist for model %s", model_id)
        return

    iso_time = recommended_at.astimezone().isoformat()

    with connect() as conn:
        for row in recommendations.to_dict("records"):
            team_code = row.get("team")
            game_id = row.get("game_id")
            if not team_code or not game_id:
                continue

            team_id = _get_team_id(conn, team_code)
            if team_id is None:
                continue

            edge = row.get("edge")
            kelly_fraction = row.get("kelly_fraction")
            stake = row.get("stake")

            conn.execute(
                """
                INSERT INTO recommendations (
                    model_id, snapshot_id, game_id, team_id, recommended_at,
                    edge, kelly_fraction, stake
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    model_id,
                    snapshot_id,
                    game_id,
                    team_id,
                    iso_time,
                    edge,
                    kelly_fraction,
                    stake,
                ),
            )

        LOGGER.info("Persisted %d recommendations for model %s", len(recommendations), model_id)


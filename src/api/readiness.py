"""Readiness checks for the FastAPI service."""

from __future__ import annotations

import math
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from src.api.settings import APISettings, get_settings

router = APIRouter(tags=["health"])

REQUIRED_TABLES = {
    "sports",
    "teams",
    "games",
    "books",
    "odds_snapshots",
    "odds",
    "predictions",
    "models",
}

REQUIRED_PREDICTION_COLUMNS = {
    "game_id",
    "model_type",
    "predicted_at",
    "total_line",
    "over_prob",
    "under_prob",
    "over_edge",
    "under_edge",
    "predicted_total_points",
}


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if len(raw) <= 10:  # A date alone supplies no observation time.
        return None
    try:
        parsed = datetime.fromisoformat(raw)
        if parsed.utcoffset() is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def _is_priced_odds(value: object) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value) and abs(value) >= 100


def _freshness_status(
    rows: list[sqlite3.Row], threshold_minutes: int, now: datetime
) -> dict[str, Any]:
    leagues: dict[str, Any] = {}
    ok = True
    for row in rows:
        league = str(row["league"])
        latest = _parse_timestamp(row["latest"])
        age = (now - latest).total_seconds() / 60.0 if latest is not None else None
        league_ok = (
            age is not None and age >= 0 and (threshold_minutes == 0 or age <= threshold_minutes)
        )
        ok = ok and league_ok
        leagues[league] = {
            "ok": league_ok,
            "latest": latest.isoformat() if latest else None,
            "age_minutes": round(age, 2) if age is not None else None,
        }
    return {"ok": ok, "leagues": leagues}


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2)
    conn.row_factory = sqlite3.Row
    return conn


def run_readiness_checks(settings: APISettings | None = None) -> dict[str, Any]:
    settings = settings or get_settings()
    checks: dict[str, Any] = {}
    try:
        interrupted_restore = any(settings.models_dir.glob(".restore-*"))
        checks["restore"] = {"ok": not interrupted_restore}
        if interrupted_restore:
            checks["restore"]["error"] = "an interrupted restore requires recovery"
    except OSError as error:
        checks["restore"] = {"ok": False, "error": str(error)}

    db_path = settings.db_path
    conn: sqlite3.Connection | None = None
    try:
        conn = _connect_readonly(db_path)
        conn.execute("SELECT 1").fetchone()
        checks["db"] = {"ok": True, "path": str(db_path)}
    except Exception as exc:
        checks["db"] = {"ok": False, "path": str(db_path), "error": str(exc)}

    if conn is None:
        checks["schema"] = {"ok": False, "error": "database unavailable"}
        checks["odds_freshness"] = {"ok": False, "error": "database unavailable"}
        checks["predictions_freshness"] = {"ok": False, "error": "database unavailable"}
        checks["models"] = {"ok": False, "error": "database unavailable"}
    else:
        try:
            table_rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
            tables = {str(row[0]) for row in table_rows}
            missing_tables = sorted(REQUIRED_TABLES - tables)
            prediction_cols = {
                str(row["name"]) for row in conn.execute("PRAGMA table_info(predictions)").fetchall()
            }
            missing_prediction_cols = sorted(REQUIRED_PREDICTION_COLUMNS - prediction_cols)
            checks["schema"] = {
                "ok": not missing_tables and not missing_prediction_cols,
                "missing_tables": missing_tables,
                "missing_prediction_columns": missing_prediction_cols,
            }
        except Exception as exc:
            checks["schema"] = {"ok": False, "error": str(exc)}

        release_leagues = tuple(settings.release_leagues)
        league_placeholders = ",".join("?" for _ in release_leagues)
        now = datetime.now(timezone.utc)

        def nonfuture_timestamp(value: object) -> str | None:
            parsed = _parse_timestamp(value)
            if parsed is None or parsed > now:
                return None
            # Normalize offsets and retain microseconds so SQL MAX orders exact UTC times.
            return parsed.isoformat(timespec="microseconds")

        conn.create_function("readiness_timestamp", 1, nonfuture_timestamp, deterministic=True)
        conn.create_function("readiness_priced_odds", 1, _is_priced_odds, deterministic=True)

        try:
            odds_query = f"""
                SELECT s.league, MAX(readiness_timestamp(os.fetched_at_utc)) AS latest
                FROM sports s
                LEFT JOIN odds_snapshots os ON os.sport_id = s.sport_id
                    AND EXISTS (
                        SELECT 1 FROM odds o
                        JOIN games g ON g.game_id = o.game_id AND g.sport_id = os.sport_id
                        WHERE o.snapshot_id = os.snapshot_id
                          AND readiness_priced_odds(o.price_american)
                    )
                WHERE s.league IN ({league_placeholders})
                GROUP BY s.league
            """
            odds_rows = conn.execute(odds_query, release_leagues).fetchall()
            found = {str(row["league"]) for row in odds_rows}
            missing = sorted(set(release_leagues) - found)
            status = _freshness_status(odds_rows, settings.odds_freshness_minutes, now)
            status["missing_leagues"] = missing
            status["ok"] = bool(status["ok"] and not missing)
            checks["odds_freshness"] = status
        except Exception as exc:
            checks["odds_freshness"] = {"ok": False, "error": str(exc)}

        try:
            predictions_query = f"""
                SELECT s.league, MAX(readiness_timestamp(p.predicted_at)) AS latest
                FROM sports s
                LEFT JOIN games g ON g.sport_id = s.sport_id
                LEFT JOIN predictions p ON p.game_id = g.game_id
                WHERE s.league IN ({league_placeholders})
                GROUP BY s.league
            """
            prediction_rows = conn.execute(predictions_query, release_leagues).fetchall()
            found = {str(row["league"]) for row in prediction_rows}
            missing = sorted(set(release_leagues) - found)
            status = _freshness_status(prediction_rows, settings.predictions_freshness_minutes, now)
            status["missing_leagues"] = missing
            status["ok"] = bool(status["ok"] and not missing)
            checks["predictions_freshness"] = status
        except Exception as exc:
            checks["predictions_freshness"] = {"ok": False, "error": str(exc)}

        try:
            artifact_rows = conn.execute(
                """
                SELECT league, artifact_path
                FROM models
                WHERE league IS NOT NULL
                  AND artifact_path IS NOT NULL
                  AND artifact_path != ''
                """
            ).fetchall()
            artifacts_by_league: dict[str, list[Path]] = {}
            for row in artifact_rows:
                league = str(row["league"]).upper()
                artifact = Path(str(row["artifact_path"]))
                if not artifact.is_absolute():
                    artifact = db_path.parents[1] / artifact if len(db_path.parents) > 1 else artifact
                artifacts_by_league.setdefault(league, []).append(artifact)

            league_status = {}
            ok = True
            for league in release_leagues:
                league_lower = league.lower()
                file_matches = list(settings.models_dir.glob(f"{league_lower}_*.pkl"))
                db_matches = [path for path in artifacts_by_league.get(league, []) if path.exists()]
                present = bool(file_matches or db_matches)
                ok = ok and present
                league_status[league] = {
                    "ok": present,
                    "file_count": len(file_matches) + len(db_matches),
                }
            checks["models"] = {"ok": ok, "leagues": league_status}
        except Exception as exc:
            checks["models"] = {"ok": False, "error": str(exc)}
        finally:
            conn.close()

    try:
        disk_path = db_path.parent if db_path.parent.exists() else Path.cwd()
        usage = shutil.disk_usage(disk_path)
        free_mb = usage.free / (1024 * 1024)
        checks["disk"] = {
            "ok": free_mb >= settings.min_disk_free_mb,
            "path": str(disk_path),
            "free_mb": round(free_mb, 2),
            "min_free_mb": settings.min_disk_free_mb,
        }
    except Exception as exc:
        checks["disk"] = {"ok": False, "error": str(exc)}

    ok = all(bool(check.get("ok")) for check in checks.values())
    return {"status": "ready" if ok else "not_ready", "checks": checks}


@router.get("/ready")
def ready() -> JSONResponse:
    payload = run_readiness_checks()
    status_code = 200 if payload["status"] == "ready" else 503
    return JSONResponse(status_code=status_code, content=payload)

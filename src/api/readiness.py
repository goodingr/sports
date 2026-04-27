"""Readiness checks for the FastAPI service."""

from __future__ import annotations

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


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_minutes(timestamp: datetime | None) -> float | None:
    if timestamp is None:
        return None
    return (datetime.now(timezone.utc) - timestamp).total_seconds() / 60.0


def _freshness_status(rows: list[sqlite3.Row], threshold_minutes: int) -> dict[str, Any]:
    leagues: dict[str, Any] = {}
    ok = True
    for row in rows:
        league = str(row["league"])
        latest = _parse_timestamp(row["latest"])
        age = _age_minutes(latest)
        league_ok = latest is not None and (
            threshold_minutes == 0 or (age is not None and age <= threshold_minutes)
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

        try:
            odds_query = f"""
                SELECT s.league, MAX(os.fetched_at_utc) AS latest
                FROM sports s
                LEFT JOIN odds_snapshots os ON os.sport_id = s.sport_id
                WHERE s.league IN ({league_placeholders})
                GROUP BY s.league
            """
            odds_rows = conn.execute(odds_query, release_leagues).fetchall()
            found = {str(row["league"]) for row in odds_rows}
            missing = sorted(set(release_leagues) - found)
            status = _freshness_status(list(odds_rows), settings.odds_freshness_minutes)
            status["missing_leagues"] = missing
            status["ok"] = bool(status["ok"] and not missing)
            checks["odds_freshness"] = status
        except Exception as exc:
            checks["odds_freshness"] = {"ok": False, "error": str(exc)}

        try:
            predictions_query = f"""
                SELECT s.league, MAX(p.predicted_at) AS latest
                FROM sports s
                LEFT JOIN games g ON g.sport_id = s.sport_id
                LEFT JOIN predictions p ON p.game_id = g.game_id
                WHERE s.league IN ({league_placeholders})
                GROUP BY s.league
            """
            prediction_rows = conn.execute(predictions_query, release_leagues).fetchall()
            found = {str(row["league"]) for row in prediction_rows}
            missing = sorted(set(release_leagues) - found)
            status = _freshness_status(
                list(prediction_rows), settings.predictions_freshness_minutes
            )
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

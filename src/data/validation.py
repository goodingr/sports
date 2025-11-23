"""Data validation utilities."""
import logging
from dataclasses import dataclass
from typing import List, Dict, Any

import pandas as pd

from src.db.core import connect

LOGGER = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    check_name: str
    status: str  # "PASS", "WARN", "FAIL"
    message: str
    details: Dict[str, Any]

def check_odds_coverage(threshold: float = 0.9) -> ValidationResult:
    """Check if recent games have odds data."""
    with connect() as conn:
        # Check games in the last 30 days
        query = """
        SELECT 
            COUNT(*) as total_games,
            SUM(CASE WHEN r.home_moneyline_close IS NOT NULL THEN 1 ELSE 0 END) as games_with_odds
        FROM games g
        LEFT JOIN game_results r ON g.game_id = r.game_id
        WHERE g.start_time_utc > date('now', '-30 days')
          AND g.status = 'final'
        """
        row = conn.execute(query).fetchone()
        total = row["total_games"]
        with_odds = row["games_with_odds"] or 0
        
        if total == 0:
            return ValidationResult("Odds Coverage", "WARN", "No recent games found", {"total": 0})
            
        coverage = with_odds / total
        status = "PASS" if coverage >= threshold else "WARN"
        
        return ValidationResult(
            "Odds Coverage", 
            status, 
            f"Odds coverage: {coverage:.1%} ({with_odds}/{total})",
            {"coverage": coverage, "total": total, "with_odds": with_odds}
        )

def check_rolling_metrics_freshness(league: str = "NBA") -> ValidationResult:
    """Check if rolling metrics are up to date."""
    # This requires loading the parquet file
    from src.features.dataset.shared import load_latest_parquet
    
    try:
        df = load_latest_parquet(league.lower(), "rolling_metrics", "rolling_metrics.parquet")
        if df.empty:
            return ValidationResult(f"{league} Rolling Metrics", "FAIL", "No rolling metrics found", {})
            
        last_date = pd.to_datetime(df["game_date"]).max()
        days_since = (pd.Timestamp.now(tz="UTC") - last_date.tz_localize("UTC")).days
        
        status = "PASS" if days_since < 3 else "WARN"
        return ValidationResult(
            f"{league} Rolling Metrics",
            status,
            f"Last rolling metric date: {last_date.date()} ({days_since} days ago)",
            {"last_date": str(last_date), "days_since": days_since}
        )
    except Exception as e:
        return ValidationResult(f"{league} Rolling Metrics", "FAIL", f"Error checking metrics: {e}", {})

def run_validation_suite() -> List[ValidationResult]:
    results = []
    results.append(check_odds_coverage())
    results.append(check_rolling_metrics_freshness("NBA"))

    return results

def print_report(results: List[ValidationResult]):
    print("\nData Validation Report")
    print("======================")
    for r in results:
        icon = "✅" if r.status == "PASS" else "⚠️" if r.status == "WARN" else "❌"
        print(f"{icon} [{r.status}] {r.check_name}: {r.message}")

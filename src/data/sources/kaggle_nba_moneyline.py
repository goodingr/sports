"""Load NBA betting moneyline data from Kaggle CSV format."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from src.db.loaders import load_odds_snapshot
from src.data.team_mappings import normalize_team_code

LOGGER = logging.getLogger(__name__)

# NBA Stats API team ID to team code mapping
NBA_TEAM_ID_MAP = {
    1610612737: "ATL",  # Atlanta Hawks
    1610612738: "BOS",  # Boston Celtics
    1610612751: "BKN",  # Brooklyn Nets
    1610612766: "CHA",  # Charlotte Hornets
    1610612741: "CHI",  # Chicago Bulls
    1610612739: "CLE",  # Cleveland Cavaliers
    1610612742: "DAL",  # Dallas Mavericks
    1610612743: "DEN",  # Denver Nuggets
    1610612765: "DET",  # Detroit Pistons
    1610612744: "GSW",  # Golden State Warriors
    1610612745: "HOU",  # Houston Rockets
    1610612754: "IND",  # Indiana Pacers
    1610612746: "LAC",  # LA Clippers
    1610612747: "LAL",  # Los Angeles Lakers
    1610612763: "MEM",  # Memphis Grizzlies
    1610612748: "MIA",  # Miami Heat
    1610612749: "MIL",  # Milwaukee Bucks
    1610612750: "MIN",  # Minnesota Timberwolves
    1610612740: "NOP",  # New Orleans Pelicans
    1610612752: "NYK",  # New York Knicks
    1610612760: "OKC",  # Oklahoma City Thunder
    1610612753: "ORL",  # Orlando Magic
    1610612755: "PHI",  # Philadelphia 76ers
    1610612756: "PHX",  # Phoenix Suns
    1610612757: "POR",  # Portland Trail Blazers
    1610612758: "SAC",  # Sacramento Kings
    1610612759: "SAS",  # San Antonio Spurs
    1610612761: "TOR",  # Toronto Raptors
    1610612762: "UTA",  # Utah Jazz
    1610612764: "WAS",  # Washington Wizards
}


def _team_id_to_code(team_id: int) -> Optional[str]:
    """Convert NBA team ID to team code."""
    return NBA_TEAM_ID_MAP.get(team_id)


def _transform_kaggle_moneyline(csv_path: Path) -> pd.DataFrame:
    """Transform Kaggle moneyline CSV into our format."""
    df = pd.read_csv(csv_path)
    
    LOGGER.info("Loaded Kaggle CSV with %d rows, %d unique games", len(df), df['game_id'].nunique())
    
    # Convert game_id to string and format
    df['game_id'] = df['game_id'].astype(str).str.zfill(10)
    df['game_id'] = 'NBA_' + df['game_id']
    
    # Map team IDs to codes
    df['team_code'] = df['team_id'].map(_team_id_to_code)
    df['opponent_code'] = df['a_team_id'].map(_team_id_to_code)
    
    # Remove rows where we can't map team IDs
    missing = df[df['team_code'].isna() | df['opponent_code'].isna()]
    if len(missing) > 0:
        LOGGER.warning("Could not map %d rows due to unknown team IDs", len(missing))
        df = df.dropna(subset=['team_code', 'opponent_code'])
    
    # Group by game and bookmaker to get average/consensus moneylines
    # For each game, we'll take the most common bookmaker or average
    game_odds = []
    
    for game_id, game_group in df.groupby('game_id'):
        # Get unique team pair
        team_code = game_group['team_code'].iloc[0]
        opponent_code = game_group['opponent_code'].iloc[0]
        
        # Aggregate moneylines across bookmakers (take median to avoid outliers)
        team_ml = game_group['price1'].median()
        opponent_ml = game_group['price2'].median()
        
        # Determine home/away (we'll need to check existing games or assume team_id is home)
        # For now, assume team_id is home team and a_team_id is away
        game_odds.append({
            'game_id': game_id,
            'home_team': team_code,
            'away_team': opponent_code,
            'home_moneyline': team_ml,
            'away_moneyline': opponent_ml,
            'source': 'kaggle',
            'bookmakers_count': len(game_group),
        })
    
    result_df = pd.DataFrame(game_odds)
    LOGGER.info("Transformed to %d unique games", len(result_df))
    
    return result_df


def ingest(csv_path: str, *, timeout: int = 300) -> str:  # noqa: ARG001
    """Ingest NBA betting moneyline data from Kaggle CSV.
    
    Args:
        csv_path: Path to nba_betting_money_line.csv file
        timeout: Not used (kept for compatibility)
    """
    from .utils import SourceDefinition, source_run, write_dataframe
    
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    definition = SourceDefinition(
        key="kaggle_nba_moneyline",
        name="Kaggle NBA betting moneyline",
        league="NBA",
        category="odds",
        url="https://www.kaggle.com/datasets/ehallmar/nba-historical-stats-and-betting-data",
        default_frequency="manual",
        storage_subdir="nba/kaggle_moneyline",
    )
    
    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        
        LOGGER.info("Processing Kaggle NBA moneyline CSV: %s", csv_path)
        df = _transform_kaggle_moneyline(csv_file)
        
        if df.empty:
            run.set_message("No betting data after transformation")
            run.set_raw_path(run.storage_dir)
            return output_dir
        
        # Save processed data
        parquet_path = run.make_path("moneyline_data.parquet")
        write_dataframe(df, parquet_path)
        run.record_file(
            parquet_path,
            metadata={"rows": len(df), "columns": list(df.columns)},
            records=len(df),
        )
        
        csv_output = run.make_path("moneyline_data.csv")
        df.to_csv(csv_output, index=False)
        run.record_file(csv_output, metadata={"rows": len(df)})
        
        # Convert to odds snapshot format and load into database
        from datetime import datetime, timezone
        
        results = []
        for _, row in df.iterrows():
            results.append({
                "id": row['game_id'].replace('NBA_', ''),  # Use as odds_api_id
                "sport_key": "basketball_nba",
                "commence_time": None,  # Will match by game_id
                "home_team": row['home_team'],
                "away_team": row['away_team'],
                "bookmakers": [{
                    "key": "kaggle_consensus",
                    "title": f"Kaggle Consensus ({int(row['bookmakers_count'])} books)",
                    "markets": [{
                        "key": "h2h",
                        "outcomes": [
                            {"name": row['home_team'], "price": row['home_moneyline']},
                            {"name": row['away_team'], "price": row['away_moneyline']},
                        ]
                    }]
                }]
            })
        
        payload = {
            "results": results,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source": "kaggle",
        }
        
        try:
            LOGGER.info("Loading %d games into database via odds snapshot...", len(results))
            load_odds_snapshot(payload, raw_path=str(csv_path), sport_key="basketball_nba")
            LOGGER.info("Successfully loaded %d games into database", len(results))
            run.set_message(f"Loaded {len(results)} games from Kaggle moneyline data into database")
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed to load data into database: %s", exc)
            run.set_message(f"Processed {len(results)} games but database load failed: {exc}")
        
        run.set_records(len(df))
        run.set_raw_path(run.storage_dir)
    
    return output_dir


__all__ = ["ingest"]


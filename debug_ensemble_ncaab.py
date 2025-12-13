"""
Test ensemble backfill for NCAAB to see what fails.
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.predict.engine import PredictionEngine
from src.models.train import CalibratedModel, EnsembleModel, ProbabilityCalibrator
import src.models.train
from src.db.core import connect
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG)

engine = PredictionEngine(model_type="ensemble")

print("Testing ensemble model for NCAAB...")

# Try to load the model
if engine.load_model("NCAAB"):
    print("✓ Model loaded successfully")
    print(f"  Totals model loaded: {engine.totals_model is not None}")
    print(f"  Feature columns: {len(engine.feature_columns) if engine.feature_columns else 0}")
else:
    print("✗ Failed to load model")
    sys.exit(1)

# Try to get some games
with connect() as conn:
    sport_row = conn.execute("SELECT sport_id FROM sports WHERE league = 'NCAAB'").fetchone()
    sport_id = sport_row[0]
    
    # Get Dec 5 games
    query = """
        SELECT g.game_id, g.start_time_utc as commence_time,
               ht.name as home_team, at.name as away_team
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        WHERE g.sport_id = ?
          AND date(g.start_time_utc) = '2025-12-05'
        LIMIT 3
    """
    games_df = pd.read_sql_query(query, conn, params=(sport_id,))
    
print(f"\nFound {len(games_df)} games for Dec 5")

if not games_df.empty:
    print("\nTrying to prepare features...")
    try:
        X_df = engine.prepare_features(games_df, "NCAAB")
        print(f"✓ Prepared features: {len(X_df)} rows, {len(X_df.columns)} columns")
        print(f"  Sample columns: {X_df.columns[:10].tolist()}")
    except Exception as e:
        print(f"✗ Failed to prepare features: {e}")
        import traceback
        traceback.print_exc()

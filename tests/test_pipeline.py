import pandas as pd
import pytest
from pathlib import Path
import sys
import os

# Add project root to path to import scripts
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from scripts.copy_results import copy_results

@pytest.fixture
def temp_data_dir(tmp_path):
    """Create a temporary data directory structure."""
    forward_test = tmp_path / "forward_test"
    forward_test.mkdir()
    
    # Create model directories
    (forward_test / "ensemble").mkdir()
    (forward_test / "random_forest").mkdir()
    (forward_test / "gradient_boosting").mkdir()
    
    return forward_test

def test_copy_results(temp_data_dir):
    """Test that results are correctly copied from ensemble to other models."""
    
    # 1. Create ensemble data with results
    ensemble_data = pd.DataFrame({
        "game_id": ["game1", "game2", "game3"],
        "home_team": ["Team A", "Team C", "Team E"],
        "away_team": ["Team B", "Team D", "Team F"],
        "home_score": [24.0, 10.0, None],  # game3 not completed
        "away_score": [20.0, 14.0, None],
        "result": ["home", "away", None],
        "result_updated_at": ["2023-01-01", "2023-01-01", None]
    })
    ensemble_path = temp_data_dir / "ensemble" / "predictions_master.parquet"
    ensemble_data.to_parquet(ensemble_path)
    
    # 2. Create random_forest data (missing results)
    rf_data = pd.DataFrame({
        "game_id": ["game1", "game2", "game3"],
        "home_team": ["Team A", "Team C", "Team E"],
        "away_team": ["Team B", "Team D", "Team F"],
        "home_score": [None, None, None],
        "away_score": [None, None, None],
        "result": [None, None, None],
        "result_updated_at": [None, None, None]
    })
    rf_path = temp_data_dir / "random_forest" / "predictions_master.parquet"
    rf_data.to_parquet(rf_path)
    
    # 3. Create gradient_boosting data (partial results, maybe from old run)
    gb_data = pd.DataFrame({
        "game_id": ["game1", "game2", "game3"],
        "home_team": ["Team A", "Team C", "Team E"],
        "away_team": ["Team B", "Team D", "Team F"],
        "home_score": [None, None, None],
        "away_score": [None, None, None],
        "result": [None, None, None],
        "result_updated_at": [None, None, None]
    })
    gb_path = temp_data_dir / "gradient_boosting" / "predictions_master.parquet"
    gb_data.to_parquet(gb_path)
    
    # 4. Run copy_results
    copy_results(base_dir=temp_data_dir)
    
    # 5. Verify random_forest updated
    rf_updated = pd.read_parquet(rf_path)
    
    # Game 1 should be updated
    game1 = rf_updated[rf_updated["game_id"] == "game1"].iloc[0]
    assert game1["result"] == "home"
    assert game1["home_score"] == 24.0
    assert game1["away_score"] == 20.0
    
    # Game 2 should be updated
    game2 = rf_updated[rf_updated["game_id"] == "game2"].iloc[0]
    assert game2["result"] == "away"
    assert game2["home_score"] == 10.0
    assert game2["away_score"] == 14.0
    
    # Game 3 should NOT be updated (no result in ensemble)
    game3 = rf_updated[rf_updated["game_id"] == "game3"].iloc[0]
    assert pd.isna(game3["result"])
    
    # 6. Verify gradient_boosting updated
    gb_updated = pd.read_parquet(gb_path)
    game1_gb = gb_updated[gb_updated["game_id"] == "game1"].iloc[0]
    assert game1_gb["result"] == "home"

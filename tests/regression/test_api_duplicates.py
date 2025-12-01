"""
Regression test for duplicate game bug.

This test demonstrates the issue where games with both game_id and odds_api_id
in the database can create duplicate entries in the API response.

Bug: Cremonese vs Bologna appears twice with different IDs:
  - SERIEA_736909 (game_id)
  - b9aa270332a686385631824d477fd3b5 (odds_api_id)
"""
import pytest
import pandas as pd
import requests
from datetime import datetime, timezone


def test_no_duplicate_games_in_api_response():
    """
    Test that games don't appear as duplicates in /api/bets/upcoming response.
    
    This test will FAIL before the fix and PASS after the fix.
    """
    # Fetch upcoming bets from API
    response = requests.get("http://localhost:8000/api/bets/upcoming")
    assert response.status_code == 200, f"API returned {response.status_code}"
    
    data = response.json()
    bets = data.get('data', [])
    
    # Create a list of (home_team, away_team, commence_time) tuples
    # These represent unique physical games
    physical_games = []
    for bet in bets:
        game_tuple = (
            bet.get('home_team'),
            bet.get('away_team'),
            bet.get('commence_time')  # Same game should have same time
        )
        physical_games.append(game_tuple)
    
    # Check for duplicates
    unique_games = set(physical_games)
    
    # If there are duplicates, identify them
    if len(physical_games) != len(unique_games):
        # Find which games are duplicated
        from collections import Counter
        game_counts = Counter(physical_games)
        duplicates = [game for game, count in game_counts.items() if count > 1]
        
        duplicate_details = []
        for dup in duplicates:
            home, away, time = dup
            matching_bets = [b for b in bets 
                           if b.get('home_team') == home 
                           and b.get('away_team') == away
                           and b.get('commence_time') == time]
            
            duplicate_details.append({
                'matchup': f"{away} @ {home}",
                'time': time,
                'count': len(matching_bets),
                'game_ids': [b.get('game_id') for b in matching_bets]
            })
        
        error_msg = f"Found {len(physical_games) - len(unique_games)} duplicate games:\n"
        for detail in duplicate_details:
            error_msg += f"  - {detail['matchup']} at {detail['time']}\n"
            error_msg += f"    Appears {detail['count']} times with game_ids: {detail['game_ids']}\n"
        
        pytest.fail(error_msg)
    
    # Test passes if no duplicates found
    assert len(physical_games) == len(unique_games), \
        f"Expected {len(unique_games)} unique games, but found {len(physical_games)} total entries"


def test_cremonese_bologna_not_duplicated():
    """
    Specific test for the Cremonese vs Bologna game.
    
    This game is known to have both game_id and odds_api_id, which triggers the bug.
    """
    response = requests.get("http://localhost:8000/api/bets/upcoming")
    assert response.status_code == 200
    
    data = response.json()
    bets = data.get('data', [])
    
    # Find Cremonese games
    cremonese_games = [
        bet for bet in bets
        if 'Cremonese' in bet.get('home_team', '') or 'Cremonese' in bet.get('away_team', '')
    ]
    
    # Count unique Cremonese vs Bologna games
    bologna_games = [
        game for game in cremonese_games
        if 'Bologna' in game.get('home_team', '') or 'Bologna' in game.get('away_team', '')
    ]
    
    if len(bologna_games) > 1:
        # Show the duplicate IDs
        game_ids = [g.get('game_id') for g in bologna_games]
        pytest.fail(
            f"Cremonese vs Bologna appears {len(bologna_games)} times "
            f"with game_ids: {game_ids}"
        )
    
    # Should be 0 or 1, never more than 1
    assert len(bologna_games) <= 1, \
        f"Cremonese vs Bologna should appear at most once, found {len(bologna_games)} times"


def test_prediction_file_has_no_duplicates():
    """
    Verify that the source predictions file doesn't have duplicates.
    
    This test should PASS both before and after the fix, confirming that
    duplicates are NOT in the prediction file itself.
    """
    df = pd.read_parquet('data/forward_test/predictions_master.parquet')
    
    if df.empty:
        pytest.skip("No predictions in file")
    
    # Check for physical game duplicates
    duplicates = df[df.duplicated(subset=['home_team', 'away_team', 'commence_time'], keep=False)]
    
    if not duplicates.empty:
        # Show details of duplicates
        dup_summary = duplicates.groupby(['home_team', 'away_team', 'commence_time']).agg({
            'game_id': list
        }).reset_index()
        
        error_msg = f"Found {len(duplicates)} duplicate predictions in parquet file:\n"
        for _, row in dup_summary.iterrows():
            error_msg += f"  - {row['away_team']} @ {row['home_team']}\n"
            error_msg += f"    game_ids: {row['game_id']}\n"
        
        pytest.fail(error_msg)
    
    assert duplicates.empty, "Predictions file should not contain duplicates"


if __name__ == "__main__":
    """
    Run tests manually to see the issue.
    
    Usage:
        poetry run python test_duplicate_bug.py
    """
    print("Running duplicate game bug tests...\n")
    
    print("=" * 80)
    print("TEST 1: Checking predictions file for duplicates")
    print("=" * 80)
    try:
        test_prediction_file_has_no_duplicates()
        print("✓ PASS: No duplicates in predictions file\n")
    except AssertionError as e:
        print(f"✗ FAIL: {e}\n")
    
    print("=" * 80)
    print("TEST 2: Checking API for duplicate games")
    print("=" * 80)
    try:
        test_no_duplicate_games_in_api_response()
        print("✓ PASS: No duplicates in API response\n")
    except (AssertionError, Exception) as e:
        print(f"✗ FAIL: {e}\n")
    
    print("=" * 80)
    print("TEST 3: Checking specific Cremonese vs Bologna game")
    print("=" * 80)
    try:
        test_cremonese_bologna_not_duplicated()
        print("✓ PASS: Cremonese vs Bologna not duplicated\n")
    except (AssertionError, Exception) as e:
        print(f"✗ FAIL: {e}\n")

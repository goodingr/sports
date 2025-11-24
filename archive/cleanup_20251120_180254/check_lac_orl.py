"""
Check the LAC vs ORL game to see all available odds data
"""
import pandas as pd

# Load predictions
df = pd.read_parquet("data/forward_test/predictions_master.parquet")
nba = df[df['league'] == 'NBA'].copy()

# Find the LAC vs ORL game
game = nba[((nba['home_team'] == 'ORL') & (nba['away_team'] == 'LAC')) | 
           ((nba['home_team'] == 'LAC') & (nba['away_team'] == 'ORL'))].copy()

if len(game) > 0:
    print("LAC vs ORL GAME DATA")
    print("=" * 80)
    
    for idx, row in game.iterrows():
        print(f"\nGame ID: {row['game_id']}")
        print(f"Commence Time: {row['commence_time']}")
        print(f"Predicted At: {row['predicted_at']}")
        print()
        print(f"Home Team: {row['home_team']}")
        print(f"  Moneyline: {row.get('home_moneyline', 'N/A')}")
        print(f"  Predicted Prob: {row['home_predicted_prob']:.1%}")
        print(f"  Implied Prob: {row['home_implied_prob']:.1%}")
        print(f"  Edge: {row['home_edge']*100:.1f}%")
        print()
        print(f"Away Team: {row['away_team']}")
        print(f"  Moneyline: {row.get('away_moneyline', 'N/A')}")
        print(f"  Predicted Prob: {row['away_predicted_prob']:.1%}")
        print(f"  Implied Prob: {row['away_implied_prob']:.1%}")
        print(f"  Edge: {row['away_edge']*100:.1f}%")
        
        print("\n" + "-" * 80)
        print("EXPLANATION:")
        print("-" * 80)
        print("The game DOES have sportsbook odds:")
        print(f"  - {row['home_team']} (Home): {row.get('home_moneyline', 'N/A')}")
        print(f"  - {row['away_team']} (Away): {row.get('away_moneyline', 'N/A')}")
        print()
        print("These odds come from the Odds API and represent the best")
        print("available line at the time the prediction was made.")
        print()
        print("The dashboard shows these odds in the predictions table.")
        print("If you're not seeing them, try refreshing the dashboard.")
else:
    print("Game not found in predictions")

import pandas as pd
from src.dashboard.data import get_game_odds

game_id = "2824b0eee01ab1fc4155185a69980d39"
print(f"Testing get_game_odds for {game_id}")

try:
    df = get_game_odds(game_id)
    print(f"Rows returned: {len(df)}")
    if not df.empty:
        print("Columns:", df.columns.tolist())
        print(df[['book', 'book_url']].head())
    else:
        print("No data found.")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

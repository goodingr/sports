import sys
import os
import asyncio

# Add project root to path
sys.path.append(os.getcwd())

from src.api.routes.bets import get_totals_data, get_stats, get_history

async def main():
    try:
        print("Testing get_totals_data...")
        df = get_totals_data()
        print(f"Data loaded. Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        
        print("Testing get_stats...")
        stats = await get_stats()
        print(f"Stats: {stats}")
        
        print("Testing get_history...")
        history = await get_history(limit=5)
        print(f"History: {history.keys()}")
        print(f"History Data Length: {len(history['data'])}")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())

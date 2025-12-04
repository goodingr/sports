import sqlite3
import pandas as pd

def check_duplicate_odds():
    conn = sqlite3.connect('data/betting.db')
    
    source_id = 'EPL_id1000001761300711'
    target_id = 'EPL_740699'
    
    print(f"Checking odds for source {source_id}:")
    query_source = f"SELECT count(*) FROM odds WHERE game_id = '{source_id}'"
    print(pd.read_sql_query(query_source, conn))
    
    print(f"Checking odds for target {target_id}:")
    query_target = f"SELECT count(*) FROM odds WHERE game_id = '{target_id}'"
    print(pd.read_sql_query(query_target, conn))
    
    conn.close()

if __name__ == "__main__":
    check_duplicate_odds()

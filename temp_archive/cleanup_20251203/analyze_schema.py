import sqlite3
import json

db_path = "data/betting.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get all table names
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [row[0] for row in cursor.fetchall()]

print("DATABASE SCHEMA ANALYSIS")
print("=" * 80)
print(f"\nTotal Tables: {len(tables)}\n")

for table in tables:
    print(f"\n{'='*80}")
    print(f"TABLE: {table}")
    print('='*80)
    
    # Get table schema
    cursor.execute(f"PRAGMA table_info({table})")
    columns = cursor.fetchall()
    
    print("\nColumns:")
    for col in columns:
        col_id, name, type_, notnull, default, pk = col
        nullable = "NOT NULL" if notnull else "NULL"
        pk_marker = " [PRIMARY KEY]" if pk else ""
        default_val = f" DEFAULT {default}" if default else ""
        print(f"  - {name:<30} {type_:<15} {nullable:<10}{default_val}{pk_marker}")
    
    # Get foreign keys
    cursor.execute(f"PRAGMA foreign_key_list({table})")
    fks = cursor.fetchall()
    if fks:
        print("\nForeign Keys:")
        for fk in fks:
            id_, seq, ref_table, from_col, to_col, on_update, on_delete, match = fk
            print(f"  - {from_col} -> {ref_table}({to_col})")
    
    # Get indexes
    cursor.execute(f"PRAGMA index_list({table})")
    indexes = cursor.fetchall()
    if indexes:
        print("\nIndexes:")
        for idx in indexes:
            seq, name, unique, origin, partial = idx
            unique_marker = " [UNIQUE]" if unique else ""
            print(f"  - {name}{unique_marker}")
    
    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    row_count = cursor.fetchone()[0]
    print(f"\nRow Count: {row_count:,}")

conn.close()
print("\n" + "="*80)
print("Analysis complete!")

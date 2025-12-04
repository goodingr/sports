import pandas as pd

# Check the predictions file directly
df = pd.read_parquet('data/forward_test/predictions_master.parquet')

print("="*80)
print("CHECKING PREDICTIONS FILE FOR DUPLICATES")
print("="*80)

# Find Cremonese games
cremonese = df[(df['home_team'].str.contains('Crem', case=False, na=False)) | 
                (df['away_team'].str.contains('Crem', case=False, na=False))]

print(f"\nFound {len(cremonese)} Cremonese predictions:\n")

if len(cremonese) > 0:
    cols_to_show = ['game_id', 'home_team', 'away_team', 'league', 'commence_time', 'over_edge', 'under_edge']
    available_cols = [col for col in cols_to_show if col in cremonese.columns]
    print(cremonese[available_cols].to_string())
    
    # Check if they're for the same physical game
    print("\n" + "="*80)
    print("CHECKING FOR PHYSICAL DUPLICATES")
    print("="*80)
    
    duplicates = cremonese[cremonese.duplicated(subset=['home_team', 'away_team', 'commence_time'], keep=False)]
    if len(duplicates) > 0:
        print(f"\n⚠️ Found {len(duplicates)} records for the same physical game(s)!")
        print("\nDuplicate details:")
        print(duplicates[available_cols].to_string())
    else:
        print("\n✓ No duplicates found in predictions file")
        print("\n→ The duplicate must be happening in the API layer or frontend!")
else:
    print("No Cremonese games found in predictions file")

# Check for any duplicates across all leagues
print("\n" + "="*80)
print("CHECKING ALL LEAGUES FOR DUPLICATES")
print("="*80)

all_dups = df[df.duplicated(subset=['home_team', 'away_team', 'commence_time'], keep=False)]
if len(all_dups) > 0:
    print(f"\n⚠️  Found {len(all_dups)} total duplicate predictions across all leagues!")
    # Group by matchup
    dup_groups = all_dups.groupby(['home_team', 'away_team', 'commence_time']).size().reset_index(name='count')
    dup_groups = dup_groups[dup_groups['count'] > 1].sort_values('count', ascending=False)
    print(f"\n{len(dup_groups)} unique games with duplicates:\n")
    print(dup_groups.to_string(index=False))
else:
    print("\n✓ No duplicates found in predictions file!")

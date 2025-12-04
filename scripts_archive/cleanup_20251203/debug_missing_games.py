
import pandas as pd
from src.dashboard.data import load_forward_test_data, get_full_team_name

def check_missing_games():
    print("Loading data...")
    df = load_forward_test_data(force_refresh=False)
    
    if df.empty:
        print("No data found.")
        return

    # Filter for CFB
    if "league" in df.columns:
        cfb_df = df[df["league"] == "CFB"]
    else:
        cfb_df = df[df["game_id"].str.startswith("CFB_")]
        
    print(f"Total CFB rows: {len(cfb_df)}")

    # List of games to check (sample)
    games_to_check = [
        ("UMass Minutemen", "Bowling Green Falcons"),
        ("Memphis Tigers", "Navy Midshipmen"),
        ("Arizona State Sun Devils", "Arizona Wildcats"),
        ("Kansas Jayhawks", "Utah Utes"),
        ("Mississippi State Bulldogs", "Oregon Ducks"),
        ("Nebraska Cornhuskers", "Iowa Hawkeyes"),
        ("New Mexico Lobos", "San Diego State Aztecs"),
        ("Georgia Tech Yellow Jackets", "Georgia Bulldogs"),
        ("Utah State Aggies", "Boise State Broncos"),
        ("Texas Longhorns", "Texas A&M Aggies"),
        ("Purdue Boilermakers", "Indiana Hoosiers"),
        ("Miami (OH) RedHawks", "Ball State Cardinals"),
        ("South Florida Bulls", "Rice Owls"),
        ("Tulsa Golden Hurricane", "UAB Blazers"),
        ("BYU Cougars", "UCF Knights"),
        ("Oklahoma State Cowboys", "Iowa State Cyclones"),
        ("Florida Atlantic Owls", "East Carolina Pirates"),
        ("Auburn Tigers", "Alabama Crimson Tide"),
        ("Louisville Cardinals", "Kentucky Wildcats"),
        ("USC Trojans", "UCLA Bruins"),
        ("TCU Horned Frogs", "Cincinnati Bearcats"),
        ("Old Dominion Monarchs", "Georgia State Panthers"),
        ("Syracuse Orange", "Boston College Eagles"),
        ("California Golden Bears", "SMU Mustangs"),
        ("Arkansas Razorbacks", "Missouri Tigers"),
        ("West Virginia Mountaineers", "Texas Tech Red Raiders"),
        ("Tulane Green Wave", "Charlotte 49ers"),
        ("Illinois Fighting Illini", "Northwestern Wildcats"),
        ("Coastal Carolina Chanticleers", "James Madison Dukes"),
        ("New Mexico State Aggies", "Middle Tennessee Blue Raiders"),
        ("Virginia Cavaliers", "Virginia Tech Hokies"),
        ("Washington Huskies", "Oregon Ducks"),
        ("Appalachian State Mountaineers", "Arkansas State Red Wolves"),
        ("UTSA Roadrunners", "Army Black Knights"),
        ("Duke Blue Devils", "Wake Forest Demon Deacons"),
        ("Oklahoma Sooners", "LSU Tigers"),
        ("Baylor Bears", "Houston Cougars"),
        ("Florida Gators", "Florida State Seminoles"),
        ("Marshall Thundering Herd", "Georgia Southern Eagles"),
        ("Texas State Bobcats", "South Alabama Jaguars"),
        ("Stanford Cardinal", "Notre Dame Fighting Irish"),
        ("Pittsburgh Panthers", "Miami Hurricanes"),
        ("Kansas State Wildcats", "Colorado Buffaloes"),
        ("Minnesota Golden Gophers", "Wisconsin Badgers"),
        ("Sam Houston State Bearkats", "Florida International Panthers"),
        ("Delaware Blue Hens", "UTEP Miners"),
        ("Jacksonville State Gamecocks", "Western Kentucky Hilltoppers"),
        ("Missouri State Bears", "Louisiana Tech Bulldogs"),
        ("Louisiana Ragin Cajuns", "UL Monroe Warhawks"),
        ("Southern Mississippi Golden Eagles", "Troy Trojans"),
        ("Liberty Flames", "Kennesaw State Owls"),
        ("Washington State Cougars", "Oregon State Beavers"),
        ("Michigan State Spartans", "Maryland Terrapins"),
        ("NC State Wolfpack", "North Carolina Tar Heels"),
        ("Nevada Wolf Pack", "UNLV Rebels"),
        ("Indiana Hoosiers", "Ohio State Buckeyes")
    ]

    found_count = 0
    missing_count = 0
    
    print("\nChecking for specific games:")
    for home, away in games_to_check:
        # Try to find match using full names or partial matches
        # Note: The dataframe might have abbreviated names or slightly different names
        # We'll try to match loosely
        
        match = None
        
        # Helper to check if row matches
        def is_match(row):
            h = str(row.get("home_team", "")).lower()
            a = str(row.get("away_team", "")).lower()
            
            # Try full name resolution
            h_full = get_full_team_name("CFB", row.get("home_team")) or h
            a_full = get_full_team_name("CFB", row.get("away_team")) or a
            
            h_full = h_full.lower()
            a_full = a_full.lower()
            
            target_h = home.lower()
            target_a = away.lower()
            
            # Check if target names are contained in data names or vice versa
            # This handles "UMass" vs "Massachusetts" etc if lucky, but mostly handles "Tigers" vs "Memphis Tigers"
            
            # Simple containment check
            h_match = (target_h in h_full) or (h_full in target_h)
            a_match = (target_a in a_full) or (a_full in target_a)
            
            return h_match and a_match

        matches = cfb_df[cfb_df.apply(is_match, axis=1)]
        
        if not matches.empty:
            found_count += 1
            print(f"[FOUND] {home} vs {away} ({len(matches)} records)")
            for idx, row in matches.iterrows():
                has_home_ml = pd.notna(row.get("home_moneyline"))
                has_away_ml = pd.notna(row.get("away_moneyline"))
                has_ml = has_home_ml and has_away_ml
                
                has_home_pred = pd.notna(row.get("home_predicted_prob"))
                has_away_pred = pd.notna(row.get("away_predicted_prob"))
                has_pred = has_home_pred and has_away_pred
                
                edge = row.get("home_edge") if pd.notna(row.get("home_edge")) else row.get("away_edge")
                predicted_at = row.get("predicted_at")
                
                print(f"    Record {idx}:")
                print(f"      - Predicted At: {predicted_at}")
                print(f"      - Moneyline: {'YES' if has_ml else 'NO'} (Home: {row.get('home_moneyline')}, Away: {row.get('away_moneyline')})")
                print(f"      - Prediction: {'YES' if has_pred else 'NO'}")
                print(f"      - Edge: {edge}")
        else:
            missing_count += 1
            print(f"[MISSING] {home} vs {away}")

    print(f"\nSummary: Found {found_count}, Missing {missing_count}")

    print("\nAll Unique CFB Team Names in Predictions:")
    unique_teams = sorted(list(set(cfb_df["home_team"].unique()) | set(cfb_df["away_team"].unique())))
    for team in unique_teams:
        print(team)

if __name__ == "__main__":
    check_missing_games()

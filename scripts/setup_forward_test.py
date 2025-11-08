"""Setup script for forward testing - checks prerequisites and creates necessary files."""
import os
import sys
from pathlib import Path

def check_model_file():
    """Check if trained model exists."""
    model_path = Path("models/nba_gradient_boosting_calibrated_moneyline.pkl")
    if model_path.exists():
        print(f"[OK] Model file found: {model_path}")
        return True
    else:
        print(f"[X] Model file NOT found: {model_path}")
        print("   Run: poetry run python -m src.models.train --league NBA --model-type gradient_boosting --calibration sigmoid --seasons 2009 2017")
        return False

def check_env_file():
    """Check if .env file exists and has ODDS_API_KEY."""
    env_path = Path(".env")
    if not env_path.exists():
        print("[X] .env file NOT found")
        print("   Creating template .env file...")
        template = """# The Odds API Configuration
ODDS_API_KEY=your_api_key_here

# Get your API key from: https://the-odds-api.com/
# Free tier: 500 requests/month
"""
        env_path.write_text(template)
        print(f"   [OK] Created {env_path}")
        print("   [!]  Please edit .env and add your ODDS_API_KEY")
        return False
    
    print(f"[OK] .env file found: {env_path}")
    
    # Check for API key
    content = env_path.read_text()
    if "ODDS_API_KEY" in content:
        # Check if it's not the placeholder
        lines = content.split("\n")
        for line in lines:
            if line.startswith("ODDS_API_KEY") and "your_api_key_here" not in line:
                key = line.split("=", 1)[1].strip()
                if key:
                    print(f"[OK] ODDS_API_KEY is set ({key[:8]}...)")
                    return True
        
        print("[!]  ODDS_API_KEY found but appears to be placeholder")
        print("   Please edit .env and add your actual API key")
        return False
    else:
        print("[X] ODDS_API_KEY not found in .env")
        print("   Add this line to .env: ODDS_API_KEY=your_api_key_here")
        return False

def check_directories():
    """Ensure necessary directories exist."""
    dirs = [
        Path("data/forward_test"),
        Path("models"),
    ]
    
    all_exist = True
    for dir_path in dirs:
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"[OK] Created directory: {dir_path}")
        else:
            print(f"[OK] Directory exists: {dir_path}")
    
    return True

def check_imports():
    """Verify required modules can be imported."""
    try:
        import joblib
        import pandas as pd
        from src.data.config import OddsAPISettings
        from src.models.train import FEATURE_COLUMNS
        print("[OK] All required modules can be imported")
    except ImportError as e:
        print(f"[X] Import error: {e}")
        print("   Run: poetry install")
        return False
    return True

def main():
    print("=" * 60)
    print("Forward Testing Setup Checklist")
    print("=" * 60)
    print()
    
    checks = [
        ("Required modules", check_imports),
        ("Directories", check_directories),
        ("Model file", check_model_file),
        ("Environment file", check_env_file),
    ]
    
    results = []
    for name, check_func in checks:
        print(f"\n[{name}]")
        result = check_func()
        results.append((name, result))
    
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    
    all_passed = all(result for _, result in results)
    
    for name, result in results:
        status = "[OK] PASS" if result else "[X] FAIL"
        print(f"{status}: {name}")
    
    print()
    if all_passed:
        print("[SUCCESS] All checks passed! You're ready for forward testing.")
        print()
        print("Next steps:")
        print("1. Make predictions: poetry run python -m src.models.forward_test predict --dotenv .env")
        print("2. Update results: poetry run python -m src.models.forward_test update")
        print("3. View report: poetry run python -m src.models.forward_test report")
    else:
        print("[!]  Some checks failed. Please fix the issues above before proceeding.")
        sys.exit(1)

if __name__ == "__main__":
    main()

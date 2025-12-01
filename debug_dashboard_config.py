from pathlib import Path
import yaml
import pandas as pd

def check_config():
    config_path = Path("config/versions.yml")
    print(f"Checking config at: {config_path.resolve()}")
    
    if not config_path.exists():
        print("Config file NOT found!")
        return

    print("Config file found.")
    try:
        data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        print("Config content:")
        print(data)
        
        versions = data.get("versions", [])
        print(f"Found {len(versions)} versions.")
        
        # Test logic from data.py
        if not versions:
            print("Logic would default to v0.1")
        else:
            print("Logic would use versions from config")
            
    except Exception as e:
        print(f"Error reading config: {e}")

if __name__ == "__main__":
    check_config()

import joblib
from pathlib import Path
import sys

model_path = Path("models/epl_totals_gradient_boosting.pkl")
if not model_path.exists():
    print(f"Model file not found: {model_path}")
    sys.exit(1)

try:
    data = joblib.load(model_path)
    print(f"Keys: {list(data.keys())}")
    if "residual_std" in data:
        print(f"residual_std: {data['residual_std']}")
    else:
        print("residual_std NOT FOUND")
except Exception as e:
    print(f"Error loading model: {e}")

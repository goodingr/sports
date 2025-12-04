import pickle
import pandas as pd
import matplotlib.pyplot as plt

# Load the trained model
with open("models/nba_lightgbm_calibrated_moneyline.pkl", "rb") as f:
    model_data = pickle.load(f)

model = model_data["model"]
feature_names = model_data.get("feature_names", [])

# Get feature importance
if hasattr(model, "feature_importances_"):
    importance = model.feature_importances_
    feature_importance_df = pd.DataFrame({
        "feature": feature_names,
        "importance": importance
    }).sort_values("importance", ascending=False)
    
    print("Top 20 Most Important Features:")
    print(feature_importance_df.head(20).to_string(index=False))
    
    # Save to CSV
    feature_importance_df.to_csv("reports/nba_feature_importance.csv", index=False)
    print("\nFull feature importance saved to reports/nba_feature_importance.csv")
    
    # Create visualization
    plt.figure(figsize=(10, 8))
    top_features = feature_importance_df.head(20)
    plt.barh(range(len(top_features)), top_features["importance"])
    plt.yticks(range(len(top_features)), top_features["feature"])
    plt.xlabel("Importance")
    plt.title("Top 20 NBA Model Features")
    plt.tight_layout()
    plt.savefig("reports/nba_feature_importance.png", dpi=150)
    print("Visualization saved to reports/nba_feature_importance.png")
else:
    print("Model does not have feature_importances_ attribute")

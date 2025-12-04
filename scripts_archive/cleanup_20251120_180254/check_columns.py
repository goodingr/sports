import pandas as pd

# Check what columns are in the predictions
df = pd.read_parquet("data/forward_test/predictions_master.parquet")
print("Columns in predictions_master.parquet:")
print(df.columns.tolist())

# Check if any rolling metrics are stored
rolling_cols = [col for col in df.columns if 'rolling' in col.lower()]
if rolling_cols:
    print(f"\nRolling metric columns found: {rolling_cols}")
else:
    print("\nNo rolling metric columns in predictions file")
    
print("\nNote: Rolling metrics are used by the MODEL to generate predictions,")
print("but are not stored in the forward test predictions file.")
print("The model uses them internally to calculate the predicted probabilities.")

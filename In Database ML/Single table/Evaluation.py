import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error

# --- 1) Load CSV ---
csv_path = "C:/Desktop/res1.csv"
df = pd.read_csv(csv_path)

# --- 2) Keep only the needed columns ---
cols_needed = ["tree_name", "predicted_value", "true_value"]
missing = set(cols_needed) - set(df.columns)
if missing:
    raise ValueError(f"Missing columns in CSV: {missing}")

df = df.loc[df["tree_name"] == "tree_7", cols_needed].copy()

# --- 3)  Drop any rows with NaNs in the metrics columns to avoid errors
df = df.dropna(subset=["y_true", "y_pred"])

# --- 4) Compute metrics ---
y_true = df["y_true"].to_numpy()
y_pred = df["y_pred"].to_numpy()

mse = mean_squared_error(y_true, y_pred)
rmse = np.sqrt(mse)
# MAR = Mean Absolute Residual (same as MAE)
mar = np.mean(np.abs(y_true - y_pred))

print("Metrics on tree_100 (original scale):")
print(f"  MSE : {mse:.6f}")
print(f"  MAR : {mar:.6f}")   # Mean Absolute Residual
print(f"  RMSE: {rmse:.6f}")
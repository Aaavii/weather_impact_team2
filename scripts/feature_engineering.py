
"""
Step 2 — Feature Engineering for Weather Impact Score (WIS)

Reads cleaned airport-year CSVs from data_clean/, adds weather
"stress" features , and writes outputs
to data_features/.

Run from repo root, e.g.:

  py -3.14 scripts/feature_engineering.py --clean .\data_clean --out .\data_features
"""

import argparse
import glob
import os

import numpy as np
import pandas as pd


# -------------------- stress functions -------------------- #

def wind_stress(wind_speed_ms: float) -> float:
    """
    Convert wind speed (m/s) into a 0–100 'wind stress' score.
    0   = calm/weak winds (<= 10 kt)
    100 = very strong winds (>= 35 kt)
    Linear ramp between 10 and 35 kt.
    """
    if pd.isna(wind_speed_ms):
        return np.nan
    
    # m/s -> knots
    knots = wind_speed_ms * 1.94384

    if knots <= 10:
        return 0.0
    elif knots >= 35:
        return 100.0
    else:
        return (knots - 10) / (35 - 10) * 100.0
    
def visibility_stress(vis_m: float) -> float:
    if pd.isna(vis_m):
        return np.nan

    if vis_m >= 16000:
        return 0.0
    elif vis_m <= 400:
        return 100.0
    else:
        return 100.0 * (16000 - vis_m) / (16000 - 400)
    
def temperature_stress(temp_c: float) -> float:
    """
    Temperature stress for airline ops (0–100).

    - Comfortable band: 5°C to 30°C  → 0 stress
    - Cold side: from 5°C down to -15°C, stress rises 0 → 100
    - Hot side: from 30°C up to 45°C, stress rises 0 → 100
    - ≤ -15°C or ≥ 45°C → capped at 100
    """
    if pd.isna(temp_c):
        return np.nan

    # ---- Cold side ----
    if temp_c <= -15:
        return 100.0
    elif temp_c < 5:  # -15 < temp_c < 5
        # 5°C -> 0, -15°C -> 100
        return 100.0 * (5 - temp_c) / 20.0

    # ---- Hot side ----
    elif temp_c >= 45:
        return 100.0
    elif temp_c > 30:  # 30 < temp_c < 45
        # 30°C -> 0, 45°C -> 100
        return 100.0 * (temp_c - 30) / 15.0

    # ---- Comfortable band ----
    else:  # 5°C <= temp_c <= 30°C
        return 0.0
def ceiling_stress(row):
    """
    Compute ceiling stress (0–100) from ceiling_m and ceiling_unlimited.

    0   = ceiling >= 3000 ft (~914 m) or unlimited
    100 = ceiling <= 200 ft (~60 m)
    Linear ramp between 60 m and 914 m.
    """
    h = row["ceiling_m"]
    unlimited = row["ceiling_unlimited"]

    # Unlimited ceiling → no stress
    if bool(unlimited):
        return 0.0

    # Missing ceiling (and not flagged unlimited) → unknown
    if pd.isna(h):
        return np.nan

    # Apply thresholds
    if h >= 914:         # >= 3000 ft
        return 0.0
    elif h <= 60:        # <= 200 ft
        return 100.0
    else:
        # As ceiling decreases, stress increases
        return 100.0 * (914 - h) / (914 - 60)

def precipitation_stress(p_mm: float) -> float:
    if pd.isna(p_mm):
        return np.nan
    if p_mm >= 4:
        return 100.0
    elif p_mm <= 0:
        return 0.0
    else:
        return (p_mm / 4.0) * 100.0
    
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    if "wind_speed_ms" in df.columns:
        df["wind_stress"] = df["wind_speed_ms"].apply(wind_stress)
    else:
        df["wind_stress"] = np.nan

    if "vis_m" in df.columns:
        df["visibility_stress"] = df["vis_m"].apply(visibility_stress)
    else:
        df["visibility_stress"] = np.nan

    if "ceiling_m" in df.columns and "ceiling_unlimited" in df.columns:
        df["ceiling_stress"] = df.apply(ceiling_stress, axis=1)
    else:
        df["ceiling_stress"] = np.nan
    
    if "temp_c" in df.columns:
        df["temperature_stress"] = df["temp_c"].apply(temperature_stress)
    else:
        df["temperature_stress"] = np.nan
    
    if "precip_mm_1h" in df.columns:
        df["precipitation_stress"] = df["precip_mm_1h"].apply(precipitation_stress)
    else:
        df["precipitation_stress"] = np.nan

    # After all individual stress columns exist
    df["WIS"] = df.apply(compute_wis_row, axis=1)
    return df

def compute_wis_row(row: pd.Series) -> float:
    return (
        0.2 * row.get("wind_stress", 0.0) +
        0.2 * row.get("visibility_stress", 0.0) +
        0.2 * row.get("ceiling_stress", 0.0) +
        0.2 * row.get("temperature_stress", 0.0) +
        0.2 * row.get("precipitation_stress", 0.0)
    )



# ------------------------ I/O helpers ---------------------- #

def process_file(path_in: str, out_dir: str) -> str:
    fname = os.path.basename(path_in)
    print(f"\n▶ Processing {fname}")

    df = pd.read_csv(path_in, parse_dates=["datetime"])
    df = add_features(df)

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, fname.replace("_clean", "_features"))

    df.to_csv(out_path, index=False)
    print(f"  ✓ Wrote {out_path}")
    return out_path


def process_all(clean_dir: str, out_dir: str, pattern: str = "*_clean.csv"):
    files = sorted(glob.glob(os.path.join(clean_dir, pattern)))
    if not files:
        print(f"No files matched {clean_dir}/{pattern}")
        return

    for fp in files:
        try:
            process_file(fp, out_dir)
        except Exception as e:
            print(f"  ✗ Failed on {os.path.basename(fp)}: {e}")


# --------------------------- main -------------------------- #

def main():
    ap = argparse.ArgumentParser(description="Add WIS feature-engineering columns")
    ap.add_argument("--clean", required=True, help="Directory with cleaned CSVs")
    ap.add_argument("--out", required=True, help="Output directory for feature CSVs")
    ap.add_argument("--glob", default="*_clean.csv", help="Glob pattern (default *_clean.csv)")
    args = ap.parse_args()

    process_all(args.clean, args.out, args.glob)


if __name__ == "__main__":
    main()
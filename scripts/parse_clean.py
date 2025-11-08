#!/usr/bin/env python3

"""
Step 1 — Parse & Clean NOAA ISD (global-hourly) for Weather Impact Score (WIS)
-----------------------------------------------------------------------------
Reads raw airport-year CSVs exported from NCEI/ISD (columns like WND, VIS, TMP,
DEW, SLP, AA1..AA3, etc.), parses compound fields, standardizes units, applies
basic QC, enforces one observation per hour, and writes cleaned CSVs.

Output columns (superset; some may be missing depending on source rows):
  datetime (UTC, hourly), airport, station, lat, lon, elev_m,
  wind_dir_deg, wind_speed_ms, vis_m, ceiling_m, ceiling_unlimited,
  temp_c, dewpoint_c, slp_hpa,
  precip_mm_1h, precip_mm_6h, precip_mm_24h


Run:
  python scripts/step1_parse_clean.py --raw ../data_raw --out ../data_clean

Notes:
- Timestamps are preserved in UTC (ISD is UTC by construction).
- We DO NOT forward-fill critical weather (wind/vis/ceiling/precip). Missing
  remains NaN; later steps may choose conservative imputation if needed.
- Units: wind (m/s), vis (m), ceiling (m), temp/dew (°C), SLP (hPa), precip (mm).
"""
from __future__ import annotations


# import sys, os
# print("RUNNING WITH:", sys.executable)
# print("CWD:", os.getcwd())
# print("sys.path (first 5):", sys.path[:5])
# try:
#     import numpy as np
#     print("NUMPY OK:", np.__version__, "from", np.__file__)
# except Exception as e:
#     print("NUMPY IMPORT FAILED:", repr(e))
#     print("FULL sys.path =", sys.path)
#     raise

# This is what I put into the terminal to run the script.
#Aaron Jathanna's script to run: py -3.14 scripts/parse_clean.py --raw .\data_csv_files_2019_2025_raw --out .\data_clean

import argparse
import os
import glob
from typing import List

import numpy as np
import pandas as pd

CORE_COMPOUND = ["WND", "VIS", "CIG", "TMP", "DEW", "SLP"]
PRECIP_FIELDS = ["AA1", "AA2", "AA3"]



# ----------------------------- helpers ------------------------------------ #

def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _split_tokens(df: pd.DataFrame, col: str, expected: int) -> pd.DataFrame:
    """Safely split a compound ISD column into `expected` comma-separated tokens.
    Returns a DataFrame with integer-labeled columns [0..expected-1].
    If `col` missing or all-null, returns empty frame with right shape.
    """
    if col not in df.columns:
        return pd.DataFrame({i: pd.Series(dtype=object) for i in range(expected)})
    # Ensure string, handle NaN gracefully
    raw = df[col].astype("string")
    parts = raw.str.split(',', n=expected - 1, expand=True)
    # Ensure expected width
    for i in range(expected):
        if i not in parts.columns:
            parts[i] = pd.NA
    return parts[[i for i in range(expected)]]


# --------------------------- parsing logic -------------------------------- #

def parse_isd(df: pd.DataFrame) -> pd.DataFrame:
    """Parse core ISD compound fields into numeric columns with standard units."""
    # Wind: WND = dir (deg), speed (0.1 m/s), code, quality
    w = _split_tokens(df, "WND", 4)
    df["wind_dir_deg"] = _to_num(w[0])
    df["wind_speed_ms"] = _to_num(w[1]) / 10.0
    # 999 usually indicates missing in dir; also treat speeds >= 9999 as missing
    df.loc[df["wind_dir_deg"] == 999, "wind_dir_deg"] = pd.NA
    df.loc[df["wind_speed_ms"] >= 999, "wind_speed_ms"] = pd.NA

    # Visibility: VIS = distance (m), var flag, dir, quality (token count varies).
    v = _split_tokens(df, "VIS", 4)
    df["vis_m"] = _to_num(v[0])
    # Cap missings coded as 99999/999999
    df.loc[df["vis_m"].isin([99999, 999999]), "vis_m"] = pd.NA

    # Ceiling: CIG = height (m), quality, method, code (variants exist)
    c = _split_tokens(df, "CIG", 4)
    df["ceiling_m"] = _to_num(c[0])
    # Unlimited often coded as 99999 (some feeds 22000); flag and set to NaN numeric
    df["ceiling_unlimited"] = df["ceiling_m"].ge(99999)
    df.loc[df["ceiling_unlimited"] == True, "ceiling_m"] = pd.NA

    # Temperature / Dewpoint / Sea-level pressure (tenths)
    t = _split_tokens(df, "TMP", 2)
    d = _split_tokens(df, "DEW", 2)
    s = _split_tokens(df, "SLP", 2)
    df["temp_c"] = _to_num(t[0]) / 10.0
    df["dewpoint_c"] = _to_num(d[0]) / 10.0
    df["slp_hpa"] = _to_num(s[0]) / 10.0
    # Map sentinel +/-999.9 after divide
    for col in ["temp_c", "dewpoint_c"]:
        df.loc[df[col].abs() >= 900, col] = pd.NA
    df.loc[df["slp_hpa"].abs() >= 9000, "slp_hpa"] = pd.NA

    # Precip: AA1..AA3 = period_hr, depth_tenths_mm, condition, quality
    for k in PRECIP_FIELDS:
        aa = _split_tokens(df, k, 4)
        df[f"{k}_period_hr"] = _to_num(aa[0])  # e.g., 01,06,24
        df[f"{k}_depth_tmm"] = _to_num(aa[1])  # tenths of mm
        df[f"{k}_mm"] = df[f"{k}_depth_tmm"] / 10.0

    # Basic identifiers
    df["datetime"] = pd.to_datetime(df["DATE"], utc=True, errors="coerce")
    df["station"] = df.get("STATION")
    df["airport"] = df.get("CALL_SIGN")  # often ICAO like KDAL; may be NaN
    df["lat"] = _to_num(df.get("LATITUDE"))
    df["lon"] = _to_num(df.get("LONGITUDE"))
    df["elev_m"] = _to_num(df.get("ELEVATION"))

    return df


# --------------------------- cleaning logic ------------------------------- #

def _physically_clip(df: pd.DataFrame) -> None:
    # Wind speed physically plausible range
    df.loc[(df["wind_speed_ms"] < 0) | (df["wind_speed_ms"] > 75), "wind_speed_ms"] = pd.NA
    # Visibility in meters (0 .. 100 km)
    df.loc[(df["vis_m"] < 0) | (df["vis_m"] > 100000), "vis_m"] = pd.NA
    # Ceiling in meters (0 .. 20 km); unlimited is already NaN + flag
    df.loc[(df["ceiling_m"] < 0) | (df["ceiling_m"] > 20000), "ceiling_m"] = pd.NA
    # Temperatures
    df.loc[(df["temp_c"] < -90) | (df["temp_c"] > 60), "temp_c"] = pd.NA
    df.loc[(df["dewpoint_c"] < -100) | (df["dewpoint_c"] > 60), "dewpoint_c"] = pd.NA
    # Sea-level pressure
    df.loc[(df["slp_hpa"] < 870) | (df["slp_hpa"] > 1085), "slp_hpa"] = pd.NA


def _select_one_row_per_hour(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse to one record per hour by choosing the row with the most
    non-null core weather fields; break ties by latest timestamp within the hour."""
    # Floor to hour for grouping
    df["datetime_hour"] = df["datetime"].dt.floor("h")

    core = [
        "wind_dir_deg", "wind_speed_ms", "vis_m", "ceiling_m",
        "temp_c", "dewpoint_c", "slp_hpa",
        "precip_mm_1h", "precip_mm_6h", "precip_mm_24h",
    ]
    present_cols = [c for c in core if c in df.columns]

    df["_nn"] = df[present_cols].notna().sum(axis=1)

    # Rank within each hour: higher _nn first, then later timestamp
    df.sort_values(["datetime_hour", "_nn", "datetime"], ascending=[True, False, True], inplace=True)
    best = df.groupby("datetime_hour", as_index=False).tail(1)

    # Clean helper cols
    best = best.drop(columns=["_nn"])

    # Drop the original high-resolution datetime to avoid two columns
    if "datetime" in best.columns:
        best = best.drop(columns=["datetime"])

    # Rename floored hour → final 'datetime'
    best = best.rename(columns={"datetime_hour": "datetime"})
    return best.sort_values("datetime").reset_index(drop=True)


def _assemble_precip_columns(df: pd.DataFrame) -> None:
    # Initialize precip columns
    for name in ["precip_mm_1h", "precip_mm_6h", "precip_mm_24h"]:
        if name not in df.columns:
            df[name] = pd.NA
    # Map AA* by their reported period
    for k in PRECIP_FIELDS:
        per = df.get(f"{k}_period_hr")
        val = df.get(f"{k}_mm")
        if per is None or val is None:
            continue
        df.loc[per == 1, "precip_mm_1h"] = val
        df.loc[per == 6, "precip_mm_6h"] = val
        df.loc[per == 24, "precip_mm_24h"] = val


def clean_isd(parsed: pd.DataFrame) -> pd.DataFrame:
    # Physical plausibility clips
    _physically_clip(parsed)

    # Build precip columns before hourly collapse
    _assemble_precip_columns(parsed)

    # Collapse to one record per hour (choose most complete)
    hourly = _select_one_row_per_hour(parsed)
    
    # Final column ordering
    if "datetime_hourly" in hourly.columns:
        hourly = hourly.rename(columns={"datetime_hourly": "datetime"})
    keep = [
        "datetime", "airport", "station", "lat", "lon", "elev_m",
        "wind_dir_deg", "wind_speed_ms", "vis_m", "ceiling_m",
        "ceiling_unlimited", "temp_c", "dewpoint_c", "slp_hpa",
        "precip_mm_1h", "precip_mm_6h", "precip_mm_24h",
    ]
    existing = [k for k in keep if k in hourly.columns]
    cleaned = hourly[existing].copy()

    # Ensure dtypes reasonable
    cleaned["airport"] = cleaned["airport"].astype("string")
    cleaned["station"] = cleaned["station"].astype("string")

    return cleaned


# ------------------------------- I/O -------------------------------------- #

def process_file(path_in: str, out_dir: str) -> str:
    fname = os.path.basename(path_in)
    print(f"\n▶ Processing {fname}")
    df = pd.read_csv(path_in)

    # Guard: ensure expected core columns exist
    missing_core = [c for c in CORE_COMPOUND if c not in df.columns]
    if missing_core:
        print(f"  ! Missing core columns: {missing_core} (continuing with what exists)")

    parsed = parse_isd(df)
    cleaned = clean_isd(parsed)

    # Basic summary
    n_raw = len(df)
    n_clean = len(cleaned)
    null_rates = cleaned.isna().mean().round(3)
    print(f"  rows(raw)={n_raw:,} → rows(clean)={n_clean:,}")
    print("  null rates (sample):",
          ", ".join(f"{k}={v:.2f}" for k, v in null_rates.items() if k in [
              'wind_speed_ms','vis_m','ceiling_m','temp_c','slp_hpa','precip_mm_1h'
          ]))

    # Output file name: keep base and append _clean
    base, ext = os.path.splitext(fname)
    out_path = os.path.join(out_dir, f"{base}_clean.csv")
    os.makedirs(out_dir, exist_ok=True)
    cleaned.to_csv(out_path, index=False)
    print(f"  ✓ Wrote {out_path}")
    return out_path


def process_all(raw_dir: str, out_dir: str, pattern: str = "*.csv") -> List[str]:
    files = sorted(glob.glob(os.path.join(raw_dir, pattern)))
    if not files:
        print(f"No files matched {raw_dir}/{pattern}")
        return []
    outputs = []
    for fp in files:
        try:
            outputs.append(process_file(fp, out_dir))
        except Exception as e:
            print(f"  ✗ Failed on {os.path.basename(fp)}: {e}")
    return outputs


# -------------------------------- main ------------------------------------ #

def main():
    ap = argparse.ArgumentParser(description="Parse & clean ISD global-hourly CSVs")
    ap.add_argument("--raw", required=True, help="Directory containing raw {airport}_{year}.csv files")
    ap.add_argument("--out", required=True, help="Directory to write cleaned CSVs")
    ap.add_argument("--glob", default="*.csv", help="Glob pattern (default: *.csv)")
    args = ap.parse_args()

    process_all(args.raw, args.out, args.glob)


if __name__ == "__main__":
    pd.set_option('display.width', 140)
    main()

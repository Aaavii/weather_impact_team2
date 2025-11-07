import csv
import os
import time
import requests
import sys
from datetime import datetime

# data processing
import pandas as pd
import numpy as np
from glob import glob
from pathlib import Path

# Ensure src is on sys.path so this module can be run directly from project root
repo_root = Path(__file__).resolve().parents[2]
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from weather_impact.settings import (
    ICAOS,
    YEARS,
    OUTDIR,
    CLEANED_DIR,
    ISD_HISTORY_URL,
    BASE,
    TIMEOUT,
    RETRIES,
    BACKOFF,
)

def fetch(url):
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, timeout=TIMEOUT)
            if r.status_code == 200 and r.content.strip():
                return r.content
            print(f"    attempt {attempt}: HTTP {r.status_code} (len={len(r.content)})")
        except Exception as e:
            print(f"    attempt {attempt}: error {e}")
        time.sleep(BACKOFF * attempt)
    return None

def load_isd_history():
    print(f"Downloading station catalog: {ISD_HISTORY_URL}")
    raw = fetch(ISD_HISTORY_URL)
    if not raw:
        raise RuntimeError("Failed to download isd-history.csv")
    text = raw.decode("utf-8", errors="replace").splitlines()
    rdr = csv.DictReader(text)
    rows = [row for row in rdr]
    return rows

def pick_station_for_icao(rows, icao):
    candidates = []
    y_min, y_max = min(YEARS), max(YEARS)
    for row in rows:
        if row.get("ICAO", "").strip().upper() != icao:
            continue
        def parse(x):
            if not x:
                return 0, 0, 0
            try:
                return int(x[0:4]), int(x[4:6]), int(x[6:8])
            except:
                return 0, 0, 0
        by, _, _ = parse(row.get("BEGIN", ""))
        ey, _, _ = parse(row.get("END", ""))
        if ey == 0:
            ey = 9999
        if not (ey < y_min or by > y_max):
            candidates.append(row)

    if not candidates:
        return None

    def keyfun(r):
        end = r.get("END", "")
        endy = int(end[:4]) if end and end[:4].isdigit() else 9999
        begin = r.get("BEGIN", "")
        beginy = int(begin[:4]) if begin and begin[:4].isdigit() else 0
        return (endy, beginy)

    best = sorted(candidates, key=keyfun, reverse=True)[0]
    usaf = (best.get("USAF", "") or "").strip().zfill(6)
    wban = (best.get("WBAN", "") or "").strip().zfill(5)
    if not (usaf.isdigit() and wban.isdigit()):
        return None
    station_id = f"{usaf}{wban}"
    return {
        "icao": icao,
        "usaf": usaf,
        "wban": wban,
        "station_id": station_id,
        "name": best.get("STATION_NAME", "").strip(),
        "begin": best.get("BEGIN", ""),
        "end": best.get("END", ""),
    }

# ------------------------- Cleaning helpers -------------------------

KEEP_COLS = [
    "STATION",
    "DATE",
    "NAME",
    "LATITUDE",
    "LONGITUDE",
    "ELEVATION",
    "REPORT_TYPE",
    "SOURCE",
    "CALL_SIGN",
    "WND",
    "CIG",
    "VIS",
    "TMP",
    "DEW",
    "SLP",
]

def _first(val):
    if pd.isna(val):
        return np.nan
    s = str(val)
    return s.split(',')[0].strip() if ',' in s else (s.strip() or np.nan)

def _f(x):
    try:
        return float(x)
    except:
        return np.nan

def parse_tmp(v):
    t = _first(v)
    if t in (None, "", "9999", "9999.9"):
        return np.nan
    return _f(t) if '.' in str(t) else _f(t) / 10.0

def parse_slp(v):
    s = _first(v)
    if s in (None, "", "99999", "9999.9"):
        return np.nan
    val = _f(s)
    return val / 10.0 if val and val > 2000 else val

def parse_vis(v):
    s = _first(v)
    if s in (None, "", "999999", "99999"):
        return np.nan
    return _f(s)

def parse_cig(v):
    s = _first(v)
    if s in (None, "", "99999"):
        return np.nan
    return _f(s)

def parse_wnd_dir(v):
    s = _first(v)
    if s in (None, "", "999"):
        return np.nan
    return _f(s)

def parse_wnd_spd(v):
    if pd.isna(v):
        return np.nan
    parts = str(v).split(',')
    if len(parts) >= 2:
        spd = parts[1].strip()
        if spd in ("", "9999", "999"):
            return np.nan
        val = _f(spd)
        return val / 10.0 if val and val > 200 else val
    return np.nan

def clean_downloads():
    """Read all CSVs in OUTDIR, parse core numerics, write a single cleaned table."""
    Path(CLEANED_DIR).mkdir(parents=True, exist_ok=True)
    files = sorted(glob(os.path.join(OUTDIR, "*.csv")))
    if not files:
        print(f"No raw files in {OUTDIR}; skipping cleaning.")
        return

    frames = []
    for fp in files:
        try:
            df = pd.read_csv(fp, low_memory=False)
        except Exception as e:
            print(f"Skip {fp}: {e}")
            continue

        cols = [c for c in KEEP_COLS if c in df.columns]
        df = df[cols].copy()

        base = os.path.basename(fp)
        icao_from_name = base.split('_')[0].upper()
        df["ICAO"] = icao_from_name

        if "CALL_SIGN" in df.columns:
            icao_from_call = (
                df["CALL_SIGN"].astype("string").str.extract(r'([A-Z]{4})', expand=False)
            )
            df["ICAO"] = icao_from_call.fillna(df["ICAO"])

        if "STATION" in df.columns:
            icao_from_station = (
                df["STATION"].astype("string").str.extract(r'([A-Z]{4})', expand=False)
            )
            df["ICAO"] = icao_from_station.fillna(df["ICAO"])

        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce", utc=True)
        df = df[df["DATE"].notna()]
        df["year"] = df["DATE"].dt.year
        df["month"] = df["DATE"].dt.month
        df["day"] = df["DATE"].dt.day
        df["hour"] = df["DATE"].dt.hour

        df["tmp_c"] = df["TMP"].apply(parse_tmp) if "TMP" in df else np.nan
        df["dew_c"] = df["DEW"].apply(parse_tmp) if "DEW" in df else np.nan
        df["slp_hpa"] = df["SLP"].apply(parse_slp) if "SLP" in df else np.nan
        df["vis_m"] = df["VIS"].apply(parse_vis) if "VIS" in df else np.nan
        df["cig_m"] = df["CIG"].apply(parse_cig) if "CIG" in df else np.nan
        df["wnd_dir"] = df["WND"].apply(parse_wnd_dir) if "WND" in df else np.nan
        df["wnd_spd"] = df["WND"].apply(parse_wnd_spd) if "WND" in df else np.nan

        df["dew_spread_c"] = df["tmp_c"] - df["dew_c"]
        df["is_low_vis"] = (df["vis_m"] < 1600).astype("Int64")
        df["is_low_cig"] = (df["cig_m"] < 300).astype("Int64")

        df = df[(df["tmp_c"].between(-60, 55)) | df["tmp_c"].isna()]
        df = df[(df["slp_hpa"].between(870, 1085)) | df["slp_hpa"].isna()]
        df = df[(df["vis_m"].between(0, 80000)) | df["vis_m"].isna()]
        df = df[(df["wnd_spd"].between(0, 120)) | df["wnd_spd"].isna()]

        keep = [
            "ICAO",
            "DATE",
            "year",
            "month",
            "day",
            "hour",
            "tmp_c",
            "dew_c",
            "dew_spread_c",
            "slp_hpa",
            "vis_m",
            "cig_m",
            "wnd_dir",
            "wnd_spd",
            "is_low_vis",
            "is_low_cig",
            "LATITUDE",
            "LONGITUDE",
            "ELEVATION",
        ]
        keep = [c for c in keep if c in df.columns]
        frames.append(df[keep])

    if not frames:
        print("No parsed frames; nothing to save.")
        return

    all_df = pd.concat(frames, ignore_index=True).sort_values(["ICAO", "DATE"])
    out_parquet = os.path.join(CLEANED_DIR, "hourly_features.parquet")
    out_csv = os.path.join(CLEANED_DIR, "hourly_features.csv")
    all_df.to_parquet(out_parquet, index=False)
    all_df.to_csv(out_csv, index=False)
    print(f"✅ Cleaned table saved: {out_parquet} (rows={len(all_df):,})")
    print(f"✅ Also wrote CSV:       {out_csv}")

def main():
    os.makedirs(OUTDIR, exist_ok=True)
    rows = load_isd_history()

    mapping = {}
    print("\nResolved stations:")
    for icao in ICAOS:
        info = pick_station_for_icao(rows, icao)
        if not info:
            print(f"  {icao}: Error. Not found in station catalog")
            continue
        mapping[icao] = info
        print(f"  {icao} -> {info['station_id']}  ({info['name']})  [{info['begin']}..{info['end'] or 'active'}]")

    for icao, info in mapping.items():
        sid = info["station_id"]
        for year in YEARS:
            url = f"{BASE}/{year}/{sid}.csv"
            out_fn = os.path.join(OUTDIR, f"{icao}_{year}.csv")
            if os.path.exists(out_fn) and os.path.getsize(out_fn) > 0:
                print(f"[{icao} {year}] ⏭️  exists, skipping")
                continue
            print(f"[{icao} {year}] GET {url}")
            content = fetch(url)
            if content:
                with open(out_fn, "wb") as f:
                    f.write(content)
                print(f"[{icao} {year}] . Success. Saved {out_fn} ({len(content):,} bytes)")
            else:
                print(f"[{icao} {year}] Error. not available")

    clean_downloads()

    print("\nDone:", datetime.utcnow().isoformat(), "UTC")

if __name__ == "__main__":
    main()

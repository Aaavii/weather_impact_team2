import csv, os, time, requests
from datetime import datetime

ICAOS = ["KDAL","KMDW","KDEN","KLAS","KPHX","KBWI","KHOU","KMCO","KOAK","KBNA"]
YEARS = range(2019, 2025)
OUTDIR = "downloads_ncei"

ISD_HISTORY_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
BASE = "https://www.ncei.noaa.gov/data/global-hourly/access"

TIMEOUT = 30
RETRIES = 3
BACKOFF = 2

def fetch(url):
    for attempt in range(1, RETRIES+1):
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
    # Filter rows that match ICAO and have any overlap with our requested years
    candidates = []
    y_min, y_max = min(YEARS), max(YEARS)
    for row in rows:
        if row.get("ICAO", "").strip().upper() != icao:
            continue
        # Parse begin/end as YYYYMMDD (may be blank)
        def parse(x):
            if not x: return 0, 0, 0
            try:
                return int(x[0:4]), int(x[4:6]), int(x[6:8])
            except:
                return 0, 0, 0
        by, _, _ = parse(row.get("BEGIN", ""))
        ey, _, _ = parse(row.get("END", ""))
        if ey == 0:  # still active
            ey = 9999
        # Overlaps our target years?
        if not (ey < y_min or by > y_max):
            candidates.append(row)

    if not candidates:
        return None

    # Prefer the one with latest END year (most recent/active), then USAF present, WBAN present
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
    station_id = f"{usaf}{wban}"  # IMPORTANT: no dash in filename
    return {
        "icao": icao,
        "usaf": usaf,
        "wban": wban,
        "station_id": station_id,
        "name": best.get("STATION_NAME", "").strip(),
        "begin": best.get("BEGIN", ""),
        "end": best.get("END", ""),
    }

def main():
    os.makedirs(OUTDIR, exist_ok=True)
    rows = load_isd_history()

    # Resolve all ICAOs first, print mapping
    mapping = {}
    print("\nResolved stations:")
    for icao in ICAOS:
        info = pick_station_for_icao(rows, icao)
        if not info:
            print(f"  {icao}: Error. Not found in station catalog")
            continue
        mapping[icao] = info
        print(f"  {icao} -> {info['station_id']}  ({info['name']})  [{info['begin']}..{info['end'] or 'active'}]")

    # Download each year
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
                print(f"[{icao} {year}] . Sucess. Saved {out_fn} ({len(content):,} bytes)")
            else:
                print(f"[{icao} {year}] Error. not available")

    print("\nDone:", datetime.utcnow().isoformat(), "UTC")

if __name__ == "__main__":
    main()
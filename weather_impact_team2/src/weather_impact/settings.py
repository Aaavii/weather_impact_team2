from pathlib import Path

# Project layout helpers
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data layout (use Path for robust joining in code)
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
INTERIM_DIR = DATA_DIR / "interim"

# Lists / constants
ICAOS = ["KDAL","KMDW","KDEN","KLAS","KPHX","KBWI","KHOU","KMCO","KOAK","KBNA"]
YEARS = list(range(2019, 2025))

# Where to store downloads and cleaned outputs (strings for backward compatibility)
OUTDIR = str(RAW_DIR)
CLEANED_DIR = str(PROCESSED_DIR)

# External URLs and network settings
ISD_HISTORY_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
BASE = "https://www.ncei.noaa.gov/data/global-hourly/access"

TIMEOUT = 30
RETRIES = 3
BACKOFF = 2

# Small convenience for other modules
def ensure_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    INTERIM_DIR.mkdir(parents=True, exist_ok=True)

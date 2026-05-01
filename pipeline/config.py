from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Base Paths
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = Path(os.getenv("DATA_DIR", r"C:\Users\LENOVO\Documents\novatrade")).resolve()

LOCAL_DATA_PATH = DATA_DIR
PARQUET_DIR = BASE_DIR / "data" / "parquet"

PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Database Config
# -----------------------------
DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "name": os.getenv("DB_NAME"),
}

connection_string: str = (
    "postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
).format(**DB_CONFIG)

# -----------------------------
# Debug / Validation
# -----------------------------
if __name__ == "__main__":
    print("=== CONFIG DEBUG ===")
    print(f"BASE_DIR        : {BASE_DIR}")
    print(f"DATA_DIR        : {DATA_DIR} (exists={DATA_DIR.exists()})")
    print(f"LOCAL_DATA_PATH : {LOCAL_DATA_PATH}")
    print(f"PARQUET_DIR     : {PARQUET_DIR}")
    print()
    print("=== DB CONFIG ===")
    print(f"HOST: {DB_CONFIG['host']}")
    print(f"PORT: {DB_CONFIG['port']}")
    print(f"DB  : {DB_CONFIG['name']}")
    print(f"USER: {DB_CONFIG['user']}")
    print(f"PASS: {'*' * len(DB_CONFIG['password'] or '')}")
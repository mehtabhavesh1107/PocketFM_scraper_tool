"""Configuration for the KU Horror & CT Goodreads enrichment pipeline."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

# Input/output paths
INPUT_CSV = BASE_DIR / "KU Horror & CT _ Analysis - CT Keyword Analysis (2).csv"
OUTPUT_CSV = BASE_DIR / "KU Horror & CT _ Analysis - CT Keyword Analysis (2) - enriched.csv"
CHECKPOINT_FILE = BASE_DIR / "goodreads_checkpoint.json"

# Optional Google Sheets integration
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL", "").strip()
GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "").strip()
GOOGLE_CREDENTIALS_FILE = os.getenv(
    "GOOGLE_CREDENTIALS_FILE",
    str(BASE_DIR / "credentials.json"),
).strip()

# Goodreads scraping settings
REQUEST_TIMEOUT = int(os.getenv("GOODREADS_REQUEST_TIMEOUT", "25"))
REQUEST_DELAY_SECONDS = float(os.getenv("GOODREADS_REQUEST_DELAY_SECONDS", "1.0"))
MAX_CANDIDATE_BOOKS = int(os.getenv("GOODREADS_MAX_CANDIDATE_BOOKS", "5"))
MAX_SEARCH_RESULTS = int(os.getenv("GOODREADS_MAX_SEARCH_RESULTS", "8"))
SAVE_EVERY_N_ROWS = int(os.getenv("GOODREADS_SAVE_EVERY_N_ROWS", "25"))

GOODREADS_COLUMNS = [
    "Goodread Link",
    "Series Book 1",
    "Series Link",
    "# of primary book",
    "# of total pages in series",
    "GR Book 1 Rating",
    "Goodreads rating",
    "Goodreads no of rating",
]

MISSING_SENTINELS = {
    "",
    "nan",
    "none",
    "n/a",
    "-",
    "#value!",
}

GENRE_HINTS = {
    "Historical Crime & Mystery": ["historical mystery", "historical crime"],
    "True Crime & Narrative Non-Fiction Crime": ["true crime", "nonfiction crime"],
    "Cozy Mystery & Amateur Sleuth": ["cozy mystery"],
    "Action Crime & Dark Thriller": ["crime thriller", "dark thriller"],
    "Crime Thriller Universe": ["crime thriller", "thriller"],
    "Psychological & Domestic Thriller": ["psychological thriller", "domestic thriller"],
    "Police, PI & Investigative Thriller": ["detective thriller", "investigative thriller"],
    "Legal, Political & Conspiracy Thriller": ["legal thriller", "conspiracy thriller"],
    "Heist & Caper Fiction": ["heist fiction", "crime fiction"],
}


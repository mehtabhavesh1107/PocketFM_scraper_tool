from __future__ import annotations

import os
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
REPO_DIR = BACKEND_DIR.parent
WORKSPACE_ROOT = REPO_DIR.parent
IS_VERCEL = bool(os.getenv("VERCEL"))
VERCEL_TMP_DIR = Path(os.getenv("TMPDIR", "/tmp")) / "pocketfm"


def _path_from_env(name: str, default: Path) -> Path:
    return Path(os.getenv(name, str(default))).expanduser()


DEFAULT_DATA_DIR = VERCEL_TMP_DIR / "backend_data" if IS_VERCEL else BACKEND_DIR / "backend_data"
DEFAULT_GENERATED_DIR = VERCEL_TMP_DIR / "generated" if IS_VERCEL else BACKEND_DIR / "generated"

DATA_DIR = _path_from_env("COMMISSIONING_DATA_DIR", DEFAULT_DATA_DIR)
GENERATED_DIR = _path_from_env("COMMISSIONING_GENERATED_DIR", DEFAULT_GENERATED_DIR)
DATABASE_PATH = DATA_DIR / "commissioning.db"


def _database_url() -> str:
    url = (
        os.getenv("COMMISSIONING_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("POSTGRES_URL")
        or os.getenv("POSTGRES_PRISMA_URL")
        or os.getenv("POSTGRES_URL_NON_POOLING")
        or f"sqlite:///{DATABASE_PATH.as_posix()}"
    )
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


DATABASE_URL = _database_url()
DEFAULT_SHEET_URL = os.getenv(
    "COMMISSIONING_GOOGLE_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1pk-uBaZvd133lqa6ofTbVfmz9jSty4BB8yg0m1ONP68/edit?gid=941904943#gid=941904943",
).strip()
DEFAULT_WORKSHEET_NAME = os.getenv("COMMISSIONING_GOOGLE_WORKSHEET", "Copy of All Data").strip()
ALLOWED_ORIGINS = [origin.strip() for origin in os.getenv("COMMISSIONING_ALLOWED_ORIGINS", "*").split(",") if origin.strip()]

BOOK_SHEET_COLUMN_MAP = {
    "title": "Title",
    "url": "URL",
    "rating": "Rating",
    "rating_count": "no. of rating",
    "publisher": "Publisher",
    "publication_date": "Publication date",
    "part_of_series": "Part of series",
    "language": "Language",
    "author": "Author",
    "best_sellers_rank": "Best Sellers Rank",
    "goodreads_rating": "Goodreads rating",
    "goodreads_rating_count": "Goodreads no of rating",
    "print_length": "Print Length",
    "book_number": "Book number",
    "format": "Format",
    "synopsis": "Synopsis/Summary",
    "genre": "Genre",
    "cleaned_series_name": "Cleaned Series Name",
    "series_flag": "Series?",
    "duplicates_basis_series": "Duplicates basis series?",
    "author_check": "Author Check",
    "clean_author_names": "Clean Author Names",
    "total_pages_in_series": "# of total pages in series",
    "total_word_count": "# Total word count",
    "total_hours": "# of Hrs",
    "goodread_link": "Goodread Link",
    "series_book_1": "Series Book 1",
    "series_link": "Series Link",
    "remarks": "Remarks",
    "primary_book_count": "# of primary book",
    "gr_book_1_rating": "GR Book 1 Rating",
    "gr_book_2_rating": "GR Book 2 Rating",
    "gr_book_3_rating": "GR Book 3 Rating",
    "gr_book_4_rating": "GR Book 4 Rating",
    "gr_book_5_rating": "GR Book 5 Rating",
    "gr_book_6_rating": "GR Book 6 Rating",
    "gr_book_7_rating": "GR Book 7 Rating",
    "gr_book_8_rating": "GR Book 8 Rating",
    "gr_book_9_rating": "GR Book 9 Rating",
    "gr_book_10_rating": "GR Book 1O Rating",
    "final_list": "Final List?",
    "rationale": "Rationale",
}

CONTACT_SHEET_COLUMN_MAP = {
    "email_id": "Email ID",
    "email_source_note": "Email ID source",
    "email_type": "Email type",
    "contact_forms": "Contact Forms",
    "facebook_link": "Facebook link",
    "publisher_details": "Publisher's details",
}

SYNC_FAMILIES = {
    "goodreads": {
        "goodread_link",
        "series_book_1",
        "series_link",
        "primary_book_count",
        "total_pages_in_series",
        "gr_book_1_rating",
        "gr_book_2_rating",
        "gr_book_3_rating",
        "gr_book_4_rating",
        "gr_book_5_rating",
        "gr_book_6_rating",
        "gr_book_7_rating",
        "gr_book_8_rating",
        "gr_book_9_rating",
        "gr_book_10_rating",
        "goodreads_rating",
        "goodreads_rating_count",
    },
    "contact": {"email_id", "email_source_note", "email_type", "contact_forms", "facebook_link", "publisher_details"},
    "curation": {"genre", "sub_genre", "remarks", "final_list", "rationale", "series_flag", "cleaned_series_name"},
}


def ensure_directories() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)

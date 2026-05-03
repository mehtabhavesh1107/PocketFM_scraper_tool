from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path

from ..settings import WORKSPACE_ROOT


FALLBACK_REFERENCE_COLUMNS = [
    "Title",
    "URL",
    "Rating",
    "no. of rating",
    "Publisher",
    "Publication date",
    "Part of series",
    "Language",
    "Author",
    "Best Sellers Rank",
    "Customer Reviews",
    "Goodreads rating",
    "Goodreads no of rating",
    "Print Length",
    "Book number",
    "Format",
    "Synopsis/Summary",
    "Genre",
    "Cleaned Series Name",
    "Series?",
    "Duplicates basis series?",
    "Author Check",
    "Clean Author Names",
    "# of total pages in series",
    "# Total word count",
    "# of Hrs",
    "Goodread Link",
    "Series Book 1",
    "Series Link",
    "Remarks",
    "# of primary book",
    "GR Book 1 Rating",
    "GR Book 2 Rating",
    "GR Book 3 Rating",
    "GR Book 4 Rating",
    "GR Book 5 Rating",
    "GR Book 6 Rating",
    "GR Book 7 Rating",
    "GR Book 8 Rating",
    "GR Book 9 Rating",
    "GR Book 1O Rating",
    "Final List?",
    "Rationale",
    "Email ID",
    "Email ID source",
    "Email type",
    "Contact Forms",
    "Facebook link",
    "Publisher's details",
]


REFERENCE_FILE_CANDIDATES = (
    "contact_live_sheet_snapshot.csv",
    "KU Horror & CT _ Analysis - CT Keyword Analysis (2) - enriched.csv",
    "KU Horror & CT _ Analysis - CT Keyword Analysis (2).csv",
)


def _clean_header(header: str) -> str:
    text = (header or "").strip()
    if not text or text.lower().startswith("unnamed:"):
        return ""
    return text


def _read_headers(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            headers = [_clean_header(value) for value in row]
            return [value for value in headers if value]
    return []


@lru_cache(maxsize=1)
def get_reference_columns() -> list[str]:
    for file_name in REFERENCE_FILE_CANDIDATES:
        path = WORKSPACE_ROOT / file_name
        if not path.exists():
            continue
        columns = _read_headers(path)
        if columns:
            return columns
    return list(FALLBACK_REFERENCE_COLUMNS)


def reference_column_fields() -> list[dict]:
    return [
        {
            "name": column.lower().replace("#", "number").replace("?", "").replace("/", "_").replace(" ", "_"),
            "label": column,
            "type": "string",
            "required": column in {"Title", "URL", "Author"},
            "on": True,
        }
        for column in get_reference_columns()
    ]

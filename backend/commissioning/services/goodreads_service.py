from __future__ import annotations

import re
import sys

from ..settings import WORKSPACE_ROOT

if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from goodreads_scraper import GoodreadsScraper  # type: ignore


def create_scraper() -> GoodreadsScraper:
    return GoodreadsScraper()


def enrich_row(row: dict, scraper: GoodreadsScraper | None = None) -> dict:
    scraper = scraper or GoodreadsScraper()
    return scraper.resolve_row(row)


def _clean_title_for_lookup(value: str | None) -> str:
    title = " ".join((value or "").split()).strip()
    if not title:
        return ""
    title = re.sub(r"\s+Audible Audiobook\s*[-\u2013\u2014]\s*Unabridged$", "", title, flags=re.IGNORECASE)
    title = re.sub(
        r"\s+(Kindle Edition|Audible Audiobook|Paperback|Hardcover|Mass Market Paperback|Audio CD)\b.*$",
        "",
        title,
        flags=re.IGNORECASE,
    )
    return title.strip() or (value or "").strip()


def enrich_book(book, scraper: GoodreadsScraper | None = None) -> dict:
    row = {
        "Title": _clean_title_for_lookup(book.title),
        "Author": book.author,
        "Genre": book.genre,
        "Book number": book.book_number,
        "Part of series": book.part_of_series,
        "Cleaned Series Name": book.cleaned_series_name,
        "Goodread Link": book.goodread_link,
        "Series Book 1": book.series_book_1,
        "Series Link": book.series_link,
    }
    return enrich_row(row, scraper)

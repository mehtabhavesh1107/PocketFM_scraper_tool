from __future__ import annotations

import re

from .goodreads_scraper import BookCandidate, GoodreadsScraper, normalize_isbn


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


def candidate_updates_for_book(book, payload: dict, scraper: GoodreadsScraper | None = None) -> dict:
    scraper = scraper or GoodreadsScraper()
    url = str(payload.get("url") or payload.get("book_url") or "").strip()
    candidate = None
    if url:
        try:
            candidate = scraper.fetch_book(url)
        except Exception:
            candidate = None
    if candidate is None:
        candidate = BookCandidate(url=url)
    candidate.title = candidate.title or str(payload.get("title") or book.title or "")
    candidate.author = candidate.author or str(payload.get("author") or book.author or "")
    candidate.series_name = candidate.series_name or str(payload.get("series_name") or "")
    candidate.series_url = candidate.series_url or str(payload.get("series_url") or "")
    candidate.rating = candidate.rating or str(payload.get("rating") or "")
    candidate.rating_count = candidate.rating_count or str(payload.get("rating_count") or "")
    candidate.pages = candidate.pages or str(payload.get("pages") or "")
    candidate.published_year = candidate.published_year or str(payload.get("published_year") or "")
    candidate.publication = candidate.publication or str(payload.get("publication") or candidate.published_year or "")
    candidate.publisher = candidate.publisher or str(payload.get("publisher") or "")
    candidate.isbn_10 = candidate.isbn_10 or normalize_isbn(payload.get("isbn_10", ""))
    candidate.isbn_13 = candidate.isbn_13 or normalize_isbn(payload.get("isbn_13", ""))
    try:
        candidate.score = float(payload.get("score") or 1)
    except (TypeError, ValueError):
        candidate.score = 1
    candidate.match_method = "manual_accept"
    candidate.evidence = ["Accepted from Goodreads review queue"]

    row = {
        "Title": _clean_title_for_lookup(book.title),
        "Author": book.clean_author_names or book.author,
        "Genre": book.genre,
        "Publisher": book.publisher,
        "Publication date": book.publication_date,
        "Print Length": book.print_length,
        "Book number": book.book_number,
        "Part of series": book.part_of_series,
        "Cleaned Series Name": book.cleaned_series_name,
    }
    amazon = (book.provenance_json or {}).get("amazon", {})
    if isinstance(amazon, dict):
        row["ISBN-10"] = amazon.get("isbn_10", "")
        row["ISBN-13"] = amazon.get("isbn_13", "")
    return scraper._updates_from_match(
        row,
        best=candidate,
        search_url=str(payload.get("search_url") or book.goodread_link or ""),
        resolved_series_url=candidate.series_url or str(payload.get("series_url") or book.series_link or ""),
        candidate_reviews=[payload],
        status="accepted",
    )

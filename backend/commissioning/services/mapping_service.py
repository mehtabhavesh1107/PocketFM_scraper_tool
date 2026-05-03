from __future__ import annotations

import re
from urllib.parse import parse_qs, unquote_plus, urlparse

from ..models import Book


PRIMARY_GENRES = [
    "Thriller",
    "Mystery",
    "Romance",
    "Fantasy",
    "Sci-Fi",
    "Historical",
    "Contemporary",
    "Satire",
]

DEFAULT_PAGES_PER_BOOK = 320
WORDS_PER_PAGE = 250
WORDS_PER_HOUR = 10000

GENERIC_CATEGORIES = {
    "",
    "books",
    "kindle store",
    "audible books originals",
    "genre literature fiction",
    "literature fiction",
}


def _clean(value: str | None) -> str:
    return " ".join((value or "").split()).strip()


def _norm(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _clean(value).lower()).strip()


def _is_generic_category(value: str | None) -> bool:
    return _norm(value) in GENERIC_CATEGORIES


def _source_category(book: Book) -> str:
    provenance = book.provenance_json or {}
    for payload in provenance.values():
        if not isinstance(payload, dict):
            continue
        url = payload.get("source_url") or ""
        parsed = urlparse(url)
        category = parse_qs(parsed.query).get("category", [""])[0]
        if category:
            return _clean(unquote_plus(category))
    return ""


def _rank_category(book: Book) -> str:
    rank = _clean(book.best_sellers_rank)
    if not rank:
        return ""
    matches = re.findall(r"#\d+(?:,\d+)*\s+in\s+([^#()]+)", rank)
    cleaned = [_clean(match) for match in matches if not _is_generic_category(match)]
    if cleaned:
        return cleaned[-1]
    matches = re.findall(r"\bin\s+([A-Z][A-Za-z &/'-]+?)(?=\s+#|\s*\(|$)", rank)
    cleaned = [_clean(match) for match in matches if not _is_generic_category(match)]
    return cleaned[-1] if cleaned else ""


def detailed_category(book: Book) -> str:
    for candidate in (book.sub_genre, book.genre, _rank_category(book), _source_category(book)):
        cleaned = _clean(candidate)
        if cleaned and not _is_generic_category(cleaned) and cleaned not in PRIMARY_GENRES:
            return cleaned
    return _clean(_source_category(book) or book.genre or "General Fiction")


def primary_genre_for(category: str) -> str:
    text = _norm(category)
    if not text:
        return "Contemporary"
    if "romance" in text or "alpha male" in text:
        return "Romance"
    if "fantasy" in text:
        return "Fantasy"
    if "sci fi" in text or "science fiction" in text or "dystopian" in text:
        return "Sci-Fi"
    if "satire" in text:
        return "Satire"
    if "mysteries" in text or "sleuth" in text or "procedural" in text:
        return "Mystery"
    thriller_terms = (
        "thriller",
        "suspense",
        "kidnapping",
        "conspiracy",
        "legal",
        "serial killer",
        "psychological",
        "domestic",
        "vigilante",
        "action",
        "murder",
        "crime",
    )
    if any(term in text for term in thriller_terms):
        return "Thriller"
    if "mystery" in text:
        return "Mystery"
    if "historical" in text:
        return "Historical"
    return "Contemporary"


def _series_count(book: Book) -> int:
    for value in (book.primary_book_count, book.book_number):
        parsed = _parse_int(value)
        if parsed:
            return parsed
    return 1


def _parse_int(value) -> int | None:
    if value in (None, "", "N/A"):
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except ValueError:
        return None


def _book_type(book: Book) -> str:
    existing = _clean(book.book_type)
    if existing == "Anthology":
        return existing
    title = _norm(book.title)
    if "anthology" in title or "collection" in title:
        return "Anthology"
    is_series = (
        book.series_flag == "Y"
        or bool(_clean(book.cleaned_series_name))
        or bool(_clean(book.part_of_series))
        or _series_count(book) > 1
    )
    if is_series:
        return "Series"
    if existing in {"Series", "Standalone"}:
        return existing
    return "Standalone"


def _audio_score(book: Book) -> int:
    score = 45
    rating = book.rating or _parse_float(book.goodreads_rating) or 0
    reviews = book.rating_count or _parse_int(book.goodreads_rating_count) or 0
    words = book.word_count or _parse_int(book.total_word_count) or 0

    if rating >= 4.5:
        score += 15
    elif rating >= 4.2:
        score += 12
    elif rating >= 4.0:
        score += 8
    elif rating >= 3.8:
        score += 5

    if reviews >= 50_000:
        score += 15
    elif reviews >= 10_000:
        score += 10
    elif reviews >= 1_000:
        score += 5

    if book.book_type == "Series":
        score += 10
    if 50_000 <= words <= 1_200_000:
        score += 10
    elif words:
        score += 5
    if book.genre in {"Thriller", "Mystery", "Romance", "Fantasy"}:
        score += 8
    if _clean(book.synopsis):
        score += 5

    return max(0, min(score, 100))


def _parse_float(value) -> float | None:
    if value in (None, "", "N/A"):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


def apply_metric_mapping(book: Book) -> None:
    series_count = max(_series_count(book), 1)
    series_pages = _parse_int(book.total_pages_in_series)
    print_pages = _parse_int(book.print_length)

    if series_pages:
        pages = series_pages
    elif print_pages:
        pages = print_pages * series_count
    else:
        pages = DEFAULT_PAGES_PER_BOOK * series_count

    if pages and not _clean(book.total_pages_in_series):
        book.total_pages_in_series = str(pages)

    derived_words = pages * WORDS_PER_PAGE if pages else None
    words = derived_words if series_pages and derived_words else (_parse_int(book.total_word_count) or book.word_count or derived_words)
    if words:
        book.word_count = words
        book.total_word_count = str(words)
        book.total_hours = str(max(1, round(words / WORDS_PER_HOUR)))


def apply_benchmark_mapping(book: Book) -> None:
    apply_metric_mapping(book)
    detail = detailed_category(book)
    primary = primary_genre_for(detail)

    book.genre = primary
    book.sub_genre = detail if detail else primary
    book.book_type = _book_type(book)
    book.series_flag = "Y" if book.book_type == "Series" else "N"
    if not _clean(book.primary_book_count):
        book.primary_book_count = str(max(_series_count(book), 1))
    if not _clean(book.clean_author_names) and _clean(book.author):
        book.clean_author_names = _clean(book.author)
    book.author_check = "Matched" if _clean(book.author) else "Missing"
    book.duplicates_basis_series = _clean(book.cleaned_series_name) or "Unique"
    if not _clean(book.remarks):
        book.remarks = f"Auto-mapped from {book.sub_genre} via mapping configuration."
    apply_metric_mapping(book)
    book.audio_score = _audio_score(book)

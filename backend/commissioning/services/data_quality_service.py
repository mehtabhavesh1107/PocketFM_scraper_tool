from __future__ import annotations

import re
from collections import Counter, defaultdict
from urllib.parse import parse_qs, unquote_plus, urlparse

from sqlalchemy.orm import Session

from ..models import Book

MISSING_TOKENS = {"", "N/A", "NA", "None", "nan", "null"}
ASIN_RE = re.compile(r"^[A-Z0-9]{10}$", re.IGNORECASE)
PRIMARY_GENRES = {"Thriller", "Mystery", "Romance", "Fantasy", "Sci-Fi", "Historical", "Contemporary", "Satire"}
GENERIC_CATEGORIES = {"", "books", "kindle store", "audible books originals", "genre literature fiction", "literature fiction"}


def value_present(value) -> bool:
    if value is None:
        return False
    return str(value).strip() not in MISSING_TOKENS


def _clean(value: str | None) -> str:
    return " ".join((value or "").split()).strip()


def _norm(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _clean(value).lower()).strip()


def _amazon_payload(book: Book) -> dict:
    payload = (book.provenance_json or {}).get("amazon", {})
    return payload if isinstance(payload, dict) else {}


def _goodreads_payload(book: Book) -> dict:
    payload = (book.provenance_json or {}).get("goodreads", {})
    return payload if isinstance(payload, dict) else {}


def _source_category(book: Book) -> str:
    for payload in (book.provenance_json or {}).values():
        if not isinstance(payload, dict):
            continue
        parsed = urlparse(payload.get("source_url") or "")
        category = parse_qs(parsed.query).get("category", [""])[0]
        if category:
            return _clean(unquote_plus(category))
    return ""


def _rank_text(book: Book) -> str:
    amazon = _amazon_payload(book)
    return _clean(amazon.get("best_sellers_rank_text") or book.best_sellers_rank)


def _rank_category(book: Book) -> str:
    rank = _rank_text(book)
    matches = re.findall(r"#\d+(?:,\d+)*\s+in\s+([^#()]+)", rank)
    cleaned = [_clean(match) for match in matches if _norm(match) not in GENERIC_CATEGORIES]
    if cleaned:
        return cleaned[-1]
    matches = re.findall(r"\bin\s+([A-Z][A-Za-z &/'-]+?)(?=\s+#|\s*\(|$)", rank)
    cleaned = [_clean(match) for match in matches if _norm(match) not in GENERIC_CATEGORIES]
    return cleaned[-1] if cleaned else ""


def _genre_source(book: Book) -> tuple[str, str]:
    sub_genre = _clean(book.sub_genre)
    genre = _clean(book.genre)
    rank_category = _rank_category(book)
    source_category = _source_category(book)
    amazon = _amazon_payload(book)
    normalized = amazon.get("normalized") if isinstance(amazon.get("normalized"), dict) else {}
    amazon_detail_genre = _clean(normalized.get("genre") or "")

    if sub_genre and rank_category and _norm(sub_genre) == _norm(rank_category):
        return "amazon_rank", f"Sub-genre matched Amazon rank category: {rank_category}"
    if sub_genre and amazon_detail_genre and _norm(sub_genre) == _norm(amazon_detail_genre):
        return "amazon_detail", f"Sub-genre matched Amazon detail genre: {amazon_detail_genre}"
    if sub_genre and source_category and _norm(sub_genre) == _norm(source_category):
        return "source_category", f"Sub-genre came from source URL category: {source_category}"
    if sub_genre and sub_genre not in PRIMARY_GENRES:
        return "mapped_detail", f"Sub-genre is specific but source was inferred: {sub_genre}"
    if genre in PRIMARY_GENRES and not sub_genre:
        return "primary_only", f"Only primary genre is populated: {genre}"
    return "fallback", "Genre appears to be a fallback mapping."


def _contact_present(book: Book) -> bool:
    contact = book.contact
    if not contact:
        return False
    return any(value_present(value) for value in (contact.email_id, contact.contact_forms, contact.facebook_link))


def _book_key(book: Book) -> str:
    return f"{_norm(book.title)}|{_norm(book.author)}"


def _issue(code: str, severity: str, message: str, field: str = "") -> dict:
    return {"code": code, "severity": severity, "message": message, "field": field}


def batch_data_quality(db: Session, batch_id: int) -> dict:
    books = db.query(Book).filter(Book.batch_id == batch_id).order_by(Book.id.asc()).all()
    key_counts = Counter(_book_key(book) for book in books)
    url_counts = Counter(_clean(book.amazon_url or book.url) for book in books if value_present(book.amazon_url or book.url))
    rows = []
    coverage_fields = {
        "title": lambda book: book.title and not ASIN_RE.fullmatch(book.title.strip()),
        "author": lambda book: book.author,
        "publisher": lambda book: book.publisher,
        "publication_date": lambda book: book.publication_date,
        "best_sellers_rank": lambda book: book.best_sellers_rank,
        "print_length": lambda book: book.print_length,
        "synopsis": lambda book: book.synopsis,
        "goodreads_rating": lambda book: book.goodreads_rating,
        "goodreads_rating_count": lambda book: book.goodreads_rating_count,
        "contact": _contact_present,
        "genre": lambda book: book.genre,
        "sub_genre": lambda book: book.sub_genre,
    }
    coverage = {name: 0 for name in coverage_fields}
    genre_sources = Counter()

    for book in books:
        issues: list[dict] = []
        missing_fields: list[str] = []
        amazon = _amazon_payload(book)
        goodreads = _goodreads_payload(book)

        for field, getter in coverage_fields.items():
            try:
                present = bool(getter(book))
            except Exception:
                present = False
            if present:
                coverage[field] += 1

        if not value_present(book.title) or ASIN_RE.fullmatch((book.title or "").strip()):
            issues.append(_issue("placeholder_title", "critical", "Title is blank or still an ASIN placeholder.", "Title"))
            missing_fields.append("Title")
        if not value_present(book.author):
            issues.append(_issue("missing_author", "critical", "Author is missing.", "Author"))
            missing_fields.append("Author")
        for attr, label in (
            ("publisher", "Publisher"),
            ("publication_date", "Publication date"),
            ("best_sellers_rank", "Best Sellers Rank"),
            ("print_length", "Print Length"),
            ("synopsis", "Synopsis/Summary"),
        ):
            if not value_present(getattr(book, attr)):
                issues.append(_issue(f"missing_{attr}", "critical", f"{label} is missing.", label))
                missing_fields.append(label)
        if not value_present(book.goodreads_rating):
            issues.append(_issue("missing_goodreads_rating", "critical", "Goodreads rating is missing.", "Goodreads rating"))
            missing_fields.append("Goodreads rating")
        if not value_present(book.goodreads_rating_count):
            issues.append(_issue("missing_goodreads_count", "warning", "Goodreads rating count is missing.", "Goodreads no of rating"))
            missing_fields.append("Goodreads no of rating")
        if not _contact_present(book):
            issues.append(_issue("missing_contact", "warning", "No email, contact form, or Facebook link found yet.", "Contact"))
        if key_counts[_book_key(book)] > 1:
            issues.append(_issue("duplicate_title_author", "critical", "Duplicate title/author appears in this batch.", "Title"))
        url = _clean(book.amazon_url or book.url)
        if url and url_counts[url] > 1:
            issues.append(_issue("duplicate_url", "critical", "Duplicate product URL appears in this batch.", "URL"))
        for flag in amazon.get("amazon_quality_flags") or []:
            if flag:
                issues.append(_issue(f"amazon_{flag}", "warning", f"Amazon parser flag: {flag}", "Amazon"))

        genre_source, genre_reason = _genre_source(book)
        genre_sources[genre_source] += 1
        if genre_source in {"fallback", "primary_only"}:
            issues.append(_issue("weak_genre_source", "warning", genre_reason, "Genre"))

        critical_count = sum(1 for issue in issues if issue["severity"] == "critical")
        warning_count = sum(1 for issue in issues if issue["severity"] == "warning")
        score = max(0, 100 - critical_count * 12 - warning_count * 4)
        rows.append(
            {
                "book_id": book.id,
                "title": book.title,
                "author": book.author,
                "quality_score": score,
                "critical_count": critical_count,
                "warning_count": warning_count,
                "issues": issues,
                "missing_fields": missing_fields,
                "genre": book.genre,
                "sub_genre": book.sub_genre,
                "genre_source": genre_source,
                "genre_reason": genre_reason,
                "source_asin": amazon.get("source_asin", ""),
                "detail_asin": amazon.get("detail_asin", ""),
                "detail_url": amazon.get("detail_url", ""),
                "source_format": amazon.get("source_format", ""),
                "detail_format": amazon.get("detail_format", ""),
                "amazon_quality_flags": amazon.get("amazon_quality_flags") or [],
                "goodreads_link": book.goodread_link or goodreads.get("Goodread Link", ""),
                "contact_ready": _contact_present(book),
            }
        )

    issue_counts: Counter[str] = Counter()
    severity_counts: Counter[str] = Counter()
    for row in rows:
        for issue in row["issues"]:
            issue_counts[issue["code"]] += 1
            severity_counts[issue["severity"]] += 1

    total = len(books)
    return {
        "total": total,
        "ready": total > 0 and severity_counts["critical"] == 0,
        "critical_count": severity_counts["critical"],
        "warning_count": severity_counts["warning"],
        "coverage": coverage,
        "missing": {field: total - count for field, count in coverage.items()},
        "issue_counts": dict(issue_counts),
        "genre_sources": dict(genre_sources),
        "rows": rows,
    }

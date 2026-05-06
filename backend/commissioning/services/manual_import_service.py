from __future__ import annotations

import csv
import io
import re

from sqlalchemy.orm import Session

from ..models import Batch, SourceLink


ALIASES: dict[str, tuple[str, ...]] = {
    "title": ("title", "book title", "name"),
    "author": ("author", "authors", "clean author names"),
    "url": ("url", "book url", "source url"),
    "amazon_url": ("amazon url", "amazon_url", "book url", "url"),
    "rating": ("rating", "amazon rating", "customer rating"),
    "rating_count": ("no. of rating", "rating count", "ratings", "reviews", "customer reviews"),
    "publisher": ("publisher", "publisher name"),
    "publication_date": ("publication date", "published date", "publication", "published year"),
    "part_of_series": ("part of series", "book series"),
    "language": ("language",),
    "best_sellers_rank": ("best sellers rank", "best seller rank", "bsr"),
    "print_length": ("print length", "num pages", "pages"),
    "book_number": ("book number", "# of primary book", "num primary books"),
    "format": ("format", "source format"),
    "synopsis": ("synopsis/summary", "synopsis", "summary", "description"),
    "genre": ("genre", "genre tag"),
    "sub_genre": ("sub genre", "sub-genre", "subgenre"),
    "cleaned_series_name": ("cleaned series name", "series name"),
    "series_flag": ("series?", "series flag"),
    "total_pages_in_series": ("# of total pages in series", "total pages in series"),
    "total_word_count": ("# total word count", "total word count"),
    "total_hours": ("# of hrs", "# of hours", "hours"),
    "goodread_link": ("goodread link", "goodreads link", "book 1 goodreads link"),
    "series_book_1": ("series book 1", "resolved goodreads book"),
    "series_link": ("series link",),
    "primary_book_count": ("# of primary book", "num primary books", "series books"),
    "goodreads_rating": ("goodreads rating", "gr book 1 rating", "book 1 ratings"),
    "goodreads_rating_count": ("goodreads no of rating", "book1 no of rating", "book 1 no of rating"),
    "source_asin": ("asin", "source asin"),
    "detail_asin": ("detail asin",),
    "detail_url": ("detail url",),
    "isbn_10": ("isbn-10", "isbn10", "isbn 10"),
    "isbn_13": ("isbn-13", "isbn13", "isbn 13"),
}


def _key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _clean(value) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _row_lookup(row: dict[str, str]) -> dict[str, str]:
    return {_key(header): _clean(value) for header, value in row.items()}


def _pick(lookup: dict[str, str], aliases: tuple[str, ...]) -> str:
    for alias in aliases:
        value = lookup.get(_key(alias), "")
        if value:
            return value
    return ""


def _normalize_row(row: dict[str, str], row_number: int) -> dict:
    lookup = _row_lookup(row)
    record = {field: _pick(lookup, aliases) for field, aliases in ALIASES.items()}
    if not record.get("amazon_url") and record.get("url", "").lower().find("amazon.") >= 0:
        record["amazon_url"] = record["url"]
    record["source_payload"] = {
        "manual_import": True,
        "row_number": row_number,
        "raw": row,
        "source_asin": record.get("source_asin", ""),
        "detail_asin": record.get("detail_asin", ""),
        "detail_url": record.get("detail_url", ""),
    }
    return record


def import_manual_csv(db: Session, batch: Batch, *, filename: str, content: bytes) -> dict:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("latin-1")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("CSV must include a header row.")

    from ..jobs.tasks import _upsert_book

    imported = 0
    skipped = 0
    source_url = f"manual://{filename or 'upload.csv'}"
    for row_number, row in enumerate(reader, start=2):
        record = _normalize_row(row, row_number)
        if not record.get("title"):
            skipped += 1
            continue
        _upsert_book(db, batch.id, record, "manual_csv", source_url)
        imported += 1

    source = SourceLink(
        batch_id=batch.id,
        source_type="manual_csv",
        url=source_url,
        max_results=imported,
        output_format="CSV",
        status="processed",
        metadata_json={"filename": filename, "imported": imported, "skipped": skipped},
    )
    db.add(source)
    db.commit()
    return {"imported": imported, "skipped": skipped, "filename": filename, "source_id": source.id}

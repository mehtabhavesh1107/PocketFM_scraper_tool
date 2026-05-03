from __future__ import annotations

import logging
import sys

from sqlalchemy.orm import Session

from ..models import Batch, Book, Contact
from ..settings import (
    BOOK_SHEET_COLUMN_MAP,
    CONTACT_SHEET_COLUMN_MAP,
    DEFAULT_SHEET_URL,
    DEFAULT_WORKSHEET_NAME,
    SYNC_FAMILIES,
    WORKSPACE_ROOT,
)

if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

logger = logging.getLogger(__name__)
_SHEET_HANDLER_IMPORT_ERROR: Exception | None = None

try:
    from sheets_handler import batch_update_cells, read_sheet_as_df  # type: ignore
except Exception as exc:  # pragma: no cover - optional local integration
    batch_update_cells = None
    read_sheet_as_df = None
    _SHEET_HANDLER_IMPORT_ERROR = exc


def _require_sheet_handler() -> None:
    if read_sheet_as_df is None or batch_update_cells is None:
        logger.warning("Google Sheet sync unavailable: %s", _SHEET_HANDLER_IMPORT_ERROR)
        raise RuntimeError("Google Sheet sync is unavailable in this deployment because sheets_handler is not packaged.")


def pull_from_sheet(db: Session, batch: Batch, sheet_url: str, worksheet_name: str) -> dict:
    _require_sheet_handler()
    df = read_sheet_as_df(sheet_url=sheet_url, worksheet_name=worksheet_name)
    imported = 0
    for _, row in df.iterrows():
        title = str(row.get("Title", "")).strip()
        if not title:
            continue
        book = Book(
            batch_id=batch.id,
            title=title,
            author=str(row.get("Author", "")).strip(),
            url=str(row.get("URL", "")).strip(),
            rating=_to_float(row.get("Rating")),
            rating_count=_to_int(row.get("no. of rating")),
            publisher=str(row.get("Publisher", "")).strip(),
            publication_date=str(row.get("Publication date", "")).strip(),
            part_of_series=str(row.get("Part of series", "")).strip(),
            language=str(row.get("Language", "")).strip(),
            best_sellers_rank=str(row.get("Best Sellers Rank", "")).strip(),
            print_length=str(row.get("Print Length", "")).strip(),
            format=str(row.get("Format", "")).strip(),
            synopsis=str(row.get("Synopsis/Summary", "")).strip(),
            genre=str(row.get("Genre", "")).strip(),
            cleaned_series_name=str(row.get("Cleaned Series Name", "")).strip(),
            series_flag=str(row.get("Series?", "")).strip(),
            duplicates_basis_series=str(row.get("Duplicates basis series?", "")).strip(),
            author_check=str(row.get("Author Check", "")).strip(),
            clean_author_names=str(row.get("Clean Author Names", "")).strip(),
            total_pages_in_series=str(row.get("# of total pages in series", "")).strip(),
            total_word_count=str(row.get("# Total word count", "")).strip(),
            total_hours=str(row.get("# of Hrs", "")).strip(),
            goodread_link=str(row.get("Goodread Link", "")).strip(),
            series_book_1=str(row.get("Series Book 1", "")).strip(),
            series_link=str(row.get("Series Link", "")).strip(),
            remarks=str(row.get("Remarks", "")).strip(),
            primary_book_count=str(row.get("# of primary book", "")).strip(),
            gr_book_1_rating=str(row.get("GR Book 1 Rating", "")).strip(),
            gr_book_2_rating=str(row.get("GR Book 2 Rating", "")).strip(),
            gr_book_3_rating=str(row.get("GR Book 3 Rating", "")).strip(),
            gr_book_4_rating=str(row.get("GR Book 4 Rating", "")).strip(),
            gr_book_5_rating=str(row.get("GR Book 5 Rating", "")).strip(),
            gr_book_6_rating=str(row.get("GR Book 6 Rating", "")).strip(),
            gr_book_7_rating=str(row.get("GR Book 7 Rating", "")).strip(),
            gr_book_8_rating=str(row.get("GR Book 8 Rating", "")).strip(),
            gr_book_9_rating=str(row.get("GR Book 9 Rating", "")).strip(),
            gr_book_10_rating=str(row.get("GR Book 1O Rating", "")).strip(),
            final_list=str(row.get("Final List?", "")).strip(),
            rationale=str(row.get("Rationale", "")).strip(),
            goodreads_rating=str(row.get("Goodreads rating", "")).strip(),
            goodreads_rating_count=str(row.get("Goodreads no of rating", "")).strip(),
        )
        db.add(book)
        db.flush()
        contact = Contact(
            book_id=book.id,
            email_id=str(row.get("Email ID", "")).strip(),
            email_source_note=str(row.get("Email ID source", "")).strip(),
            email_type=str(row.get("Email type", "")).strip(),
            contact_forms=str(row.get("Contact Forms", "")).strip(),
            facebook_link=str(row.get("Facebook link", "")).strip(),
            publisher_details=str(row.get("Publisher's details", "")).strip(),
        )
        db.add(contact)
        imported += 1
    batch.source_sheet_url = sheet_url
    batch.source_sheet_worksheet = worksheet_name
    db.commit()
    return {"imported": imported}


def push_to_sheet(
    db: Session,
    batch: Batch,
    sheet_url: str = "",
    worksheet_name: str = "",
    families: list[str] | None = None,
    overwrite: bool = False,
) -> dict:
    _require_sheet_handler()
    target_url = sheet_url or batch.source_sheet_url or DEFAULT_SHEET_URL
    target_ws = worksheet_name or batch.source_sheet_worksheet or DEFAULT_WORKSHEET_NAME
    df = read_sheet_as_df(sheet_url=target_url, worksheet_name=target_ws)
    url_to_row = {str(row.get("URL", "")).strip(): idx + 2 for idx, row in df.iterrows()}
    title_to_row = {str(row.get("Title", "")).strip().lower(): idx + 2 for idx, row in df.iterrows()}

    selected_families = families or ["goodreads", "contact", "curation"]
    selected_fields = set()
    for family in selected_families:
        selected_fields |= SYNC_FAMILIES.get(family, set())

    updates: list[dict] = []
    books = db.query(Book).filter(Book.batch_id == batch.id).order_by(Book.id.asc()).all()
    for book in books:
        sheet_row = url_to_row.get((book.url or book.amazon_url or "").strip())
        if not sheet_row:
            sheet_row = title_to_row.get(book.title.strip().lower())
        if not sheet_row:
            continue
        for attr, column in BOOK_SHEET_COLUMN_MAP.items():
            if attr not in selected_fields:
                continue
            value = getattr(book, attr)
            if not overwrite and not value:
                continue
            updates.append({"sheet_row": sheet_row, "column_name": column, "value": value})
        if book.contact:
            for attr, column in CONTACT_SHEET_COLUMN_MAP.items():
                if attr not in selected_fields:
                    continue
                value = getattr(book.contact, attr)
                if not overwrite and not value:
                    continue
                updates.append({"sheet_row": sheet_row, "column_name": column, "value": value})
    batch_update_cells(updates, sheet_url=target_url, worksheet_name=target_ws)
    return {"updated_cells": len(updates), "sheet_url": target_url, "worksheet_name": target_ws}


def _to_int(value) -> int | None:
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return int(float(text.replace(",", "")))
    except ValueError:
        return None


def _to_float(value) -> float | None:
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from sqlalchemy.orm import Session

from ..models import Batch, Book, ExportRecord
from ..settings import GENERATED_DIR
from .data_quality_service import batch_data_quality
from .mapping_service import apply_benchmark_mapping, commissioning_tier_profile
from .reference_schema import get_reference_columns
from .storage_service import upload_export_file


SAMPLE_COMPATIBLE_COLUMNS = [
    "Title",
    "URL",
    "Rating",
    "no. of rating",
    "Publisher",
    "Publisher name",
    "Publication date",
    "Part of series",
    "Language",
    "Author",
    "Author name",
    "Best Sellers Rank",
    "Customer Reviews",
    "Goodreads rating",
    "Goodreads no of rating",
    "Tier",
    "GR Ratings",
    "Trope",
    "Length",
    "MG (Min)",
    "MG (Max)",
    "Rev share (min)",
    "Rev Share (max)",
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
    "GR Book 10 Rating",
    "Book 1 No Of Rating",
    "Book 2 No Of Rating",
    "Book 3 No Of Rating",
    "Book 4 No Of Rating",
    "Book 5 No Of Rating",
    "Book 6 No Of Rating",
    "Book 7 No Of Rating",
    "Book 8 No Of Rating",
    "Book 9 No Of Rating",
    "Book 10 No Of Rating",
    "Final List?",
    "Rationale",
    "Scope?",
    "Duplicate Check",
    "Unnamed: 44",
    "Email ID",
    "Email ID source",
    "Email type",
    "Author Email",
    "Agent Email",
    "Website",
    "Contact Forms",
    "Facebook link",
    "Publisher's details",
    "Tier Mapping",
    "Minimum Gurantee",
    "Rev share %",
]

BENCHMARK_EXPORT_COLUMNS = ["Sub-genre", "Type", "Series books", "Audio score"]
REQUESTED_MAPPED_COLUMNS = [
    "Sub Genre",
    "Title",
    "Author",
    "Published Year",
    "Language",
    "Publisher name",
    "Book Series",
    "Cleaned Series Name",
    "Num Primary Books",
    "Num Pages",
    "Genre Tag",
    "Synopsis",
    "Publication",
    "Book 1 Ratings",
    "Book 2 Ratings",
    "Book 3 Ratings",
    "Book 4 Ratings",
    "Book 5 Ratings",
    "Book 6 Ratings",
    "Book 7 Ratings",
    "Book 8 Ratings",
    "Book 9 Ratings",
    "Book 10 Ratings",
    "Book2 No Of Rating",
    "Book3 No Of Rating",
    "Book4 No Of Rating",
    "Book5 No Of Rating",
    "Book6 No Of Rating",
    "Book7 No Of Rating",
    "Book8 No Of Rating",
    "Book9 No Of Rating",
    "Book10 No Of Rating",
    "Book 1 Goodreads Link",
    "Series Link",
    "Book Url",
    "Unique?",
    "Series?",
    "# of hrs",
    "Book1 No Of Rating",
]
DIAGNOSTIC_EXPORT_COLUMNS = [
    "Data Quality Score",
    "Data Quality Critical Count",
    "Data Quality Warning Count",
    "Data Quality Issues",
    "Missing Fields",
    "Genre Source",
    "Genre Reason",
    "Source ASIN",
    "Detail ASIN",
    "Detail URL",
    "Source Format",
    "Detail Format",
    "Amazon Quality Flags",
]


def _series_rating_index_from_column(column: str) -> int | None:
    if column == "GR Book 1O Rating":
        return 10
    match = re.fullmatch(r"GR Book (\d+) Rating", column)
    if not match:
        return None
    return int(match.group(1))


def _positive_int(value) -> int | None:
    match = re.search(r"\d+", str(value or ""))
    if not match:
        return None
    parsed = int(match.group(0))
    return parsed if parsed > 0 else None


def _max_series_rating_index(books: list[Book] | None) -> int:
    max_index = 10
    for book in books or []:
        primary_book_count = _positive_int(book.primary_book_count)
        if primary_book_count:
            max_index = max(max_index, primary_book_count)
        payload = _goodreads_payload(book)
        ratings = payload.get("Goodreads Series Ratings")
        rating_counts = payload.get("Goodreads Series Rating Counts")
        if isinstance(ratings, list):
            max_index = max(max_index, len(ratings))
        if isinstance(rating_counts, list):
            max_index = max(max_index, len(rating_counts))
        for key, value in payload.items():
            if value in (None, ""):
                continue
            index = _series_rating_index_from_column(str(key))
            if index:
                max_index = max(max_index, index)
                continue
            match = re.fullmatch(r"Book\s*(\d+)\s+Ratings", str(key))
            if match:
                max_index = max(max_index, int(match.group(1)))
    return max_index


def _with_dynamic_series_rating_columns(columns: list[str], books: list[Book] | None) -> list[str]:
    max_index = _max_series_rating_index(books)
    if max_index <= 10:
        return columns

    anchor = "GR Book 10 Rating" if "GR Book 10 Rating" in columns else "GR Book 1O Rating"
    insert_at = columns.index(anchor) + 1 if anchor in columns else len(columns)
    for index in range(11, max_index + 1):
        column = f"GR Book {index} Rating"
        if column not in columns:
            columns.insert(insert_at, column)
            insert_at += 1
    return columns


def export_columns_for_profile(profile: str = "sample", books: list[Book] | None = None) -> list[str]:
    normalized = (profile or "sample").lower().replace("-", "_")
    if normalized in {"sample", "sample_compatible", "sample_csv", "final", "final_csv"}:
        return _with_dynamic_series_rating_columns(list(SAMPLE_COMPATIBLE_COLUMNS), books)
    if normalized in {"full", "diagnostic", "full_diagnostic"}:
        columns = _with_dynamic_series_rating_columns(list(SAMPLE_COMPATIBLE_COLUMNS), books)
        for column in [*BENCHMARK_EXPORT_COLUMNS, *REQUESTED_MAPPED_COLUMNS, *DIAGNOSTIC_EXPORT_COLUMNS]:
            if column not in columns:
                columns.append(column)
        return columns
    columns = list(get_reference_columns())
    for column in [*BENCHMARK_EXPORT_COLUMNS, *REQUESTED_MAPPED_COLUMNS]:
        if column not in columns:
            columns.append(column)
    return columns


def _set(row: dict, column: str, value) -> None:
    if column in row and value not in (None, ""):
        row[column] = value


def _clean_export_text(value) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\u200e", " ").replace("\u200f", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^[\s:;\-]+", "", text).strip()
    return text


def _filled(value, default: str = "N/A") -> str:
    text = _clean_export_text(value)
    return text if text else default


def _clean_title(value) -> str:
    title = _clean_export_text(value)
    title = re.sub(r"\s+Audible Audiobook\s*[-\u2013\u2014]\s*Unabridged$", "", title, flags=re.IGNORECASE)
    title = re.sub(
        r"\s+(Kindle Edition|Audible Audiobook|Paperback|Hardcover|Mass Market Paperback|Audio CD)\b.*$",
        "",
        title,
        flags=re.IGNORECASE,
    )
    return title.strip()


def _clean_rank(value) -> str:
    text = _clean_export_text(value)
    match = re.search(r"#\s*([\d,]+)", text)
    if match:
        return match.group(1).replace(",", "")
    if re.fullmatch(r"[\d,]+(?:\.0)?", text):
        return text.replace(",", "").replace(".0", "")
    return text


def _clean_format(value) -> str:
    text = _clean_export_text(value)
    text = re.sub(r"\s+INR\s+.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+\$[\d,.]+.*$", "", text)
    lowered = text.lower()
    if "kindle" in lowered:
        return "Kindle"
    if "mass market" in lowered:
        return "Mass Market Paperback"
    if "paperback" in lowered:
        return "Paperback"
    if "hardcover" in lowered:
        return "Hardcover"
    if "audio cd" in lowered:
        return "Audio CD"
    if "audible" in lowered or "audiobook" in lowered:
        return "Audiobook"
    return text


def _amazon_payload(book: Book) -> dict:
    payload = (book.provenance_json or {}).get("amazon", {})
    return payload if isinstance(payload, dict) else {}


def _year_from(*values) -> str:
    for value in values:
        match = re.search(r"\b(19|20)\d{2}\b", str(value or ""))
        if match:
            return match.group(0)
    return "N/A"


def _goodreads_payload(book: Book) -> dict:
    payload = (book.provenance_json or {}).get("goodreads", {})
    return payload if isinstance(payload, dict) else {}


def _payload_value(payload: dict, *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


_FIXED_SERIES_RATING_ATTRS = {
    1: "gr_book_1_rating",
    2: "gr_book_2_rating",
    3: "gr_book_3_rating",
    4: "gr_book_4_rating",
    5: "gr_book_5_rating",
    6: "gr_book_6_rating",
    7: "gr_book_7_rating",
    8: "gr_book_8_rating",
    9: "gr_book_9_rating",
    10: "gr_book_10_rating",
}


def _series_rating_for_export(book: Book, index: int, default: str = "") -> str:
    value = ""
    attr = _FIXED_SERIES_RATING_ATTRS.get(index)
    if attr:
        value = str(getattr(book, attr, "") or "")
    if index == 1:
        value = value or str(book.goodreads_rating or book.rating or "")

    payload = _goodreads_payload(book)
    legacy_key = "GR Book 1O Rating" if index == 10 else f"GR Book {index} Rating"
    value = value or _payload_value(payload, legacy_key, f"GR Book {index} Rating", f"Book {index} Ratings")
    ratings = payload.get("Goodreads Series Ratings")
    if not value and isinstance(ratings, list) and len(ratings) >= index:
        value = str(ratings[index - 1] or "")
    return _filled(value, default) if value not in (None, "") else default


def _rating_values(book: Book) -> list[str]:
    return [_series_rating_for_export(book, index, "N/A") for index in range(1, 11)]


def _rating_count_for(book: Book, index: int) -> str:
    payload = _goodreads_payload(book)
    compact = f"Book{index} No Of Rating"
    spaced = f"Book {index} No Of Rating"
    legacy = f"GR Book {index} Rating Count"
    if index == 10:
        legacy_alt = "GR Book 1O Rating Count"
    else:
        legacy_alt = legacy
    value = _payload_value(payload, spaced, compact, legacy, legacy_alt)
    if value:
        return _filled(value)
    counts = payload.get("Goodreads Series Rating Counts")
    if isinstance(counts, list) and len(counts) >= index and counts[index - 1] not in (None, ""):
        return _filled(counts[index - 1])
    if index == 1:
        return _filled(book.goodreads_rating_count or book.rating_count)
    return "N/A"


def apply_requested_mapped_columns(row: dict, book: Book) -> None:
    payload = _goodreads_payload(book)
    ratings = _rating_values(book)
    book_series = book.part_of_series or book.cleaned_series_name or ("Series" if book.series_flag == "Y" else "Standalone")
    cleaned_series = book.cleaned_series_name or book.part_of_series
    unique = "Yes" if (book.duplicates_basis_series or "Unique") == "Unique" else "No"

    values = {
        "Sub Genre": _filled(book.sub_genre),
        "Author name": _filled(book.author),
        "Published Year": _year_from(payload.get("Published Year"), payload.get("Publication"), book.publication_date, book.title),
        "Language": _filled(book.language, "English"),
        "Publisher name": _filled(book.publisher or payload.get("Publisher name")),
        "Book Series": _filled(book_series),
        "Cleaned Series Name": _filled(cleaned_series, ""),
        "Num Primary Books": _filled(book.primary_book_count or book.book_number or "1"),
        "Num Pages": _filled(book.total_pages_in_series or book.print_length),
        "Genre Tag": _filled(book.genre),
        "Synopsis": _filled(book.synopsis),
        "Publication": _filled(book.publication_date or payload.get("Publication") or book.format),
        "Book 1 Ratings": ratings[0],
        "Book 2 Ratings": ratings[1],
        "Book 3 Ratings": ratings[2],
        "Book 4 Ratings": ratings[3],
        "Book 5 Ratings": ratings[4],
        "Book 6 Ratings": ratings[5],
        "Book 7 Ratings": ratings[6],
        "Book 8 Ratings": ratings[7],
        "Book 9 Ratings": ratings[8],
        "Book 10 Ratings": ratings[9],
        "Book2 No Of Rating": _rating_count_for(book, 2),
        "Book3 No Of Rating": _rating_count_for(book, 3),
        "Book4 No Of Rating": _rating_count_for(book, 4),
        "Book5 No Of Rating": _rating_count_for(book, 5),
        "Book6 No Of Rating": _rating_count_for(book, 6),
        "Book7 No Of Rating": _rating_count_for(book, 7),
        "Book8 No Of Rating": _rating_count_for(book, 8),
        "Book9 No Of Rating": _rating_count_for(book, 9),
        "Book10 No Of Rating": _rating_count_for(book, 10),
        "Book 1 No Of Rating": _rating_count_for(book, 1),
        "Book 2 No Of Rating": _rating_count_for(book, 2),
        "Book 3 No Of Rating": _rating_count_for(book, 3),
        "Book 4 No Of Rating": _rating_count_for(book, 4),
        "Book 5 No Of Rating": _rating_count_for(book, 5),
        "Book 6 No Of Rating": _rating_count_for(book, 6),
        "Book 7 No Of Rating": _rating_count_for(book, 7),
        "Book 8 No Of Rating": _rating_count_for(book, 8),
        "Book 9 No Of Rating": _rating_count_for(book, 9),
        "Book 10 No Of Rating": _rating_count_for(book, 10),
        "Book 1 Goodreads Link": _filled(book.series_book_1 or book.goodread_link),
        "Series Link": _filled(book.series_link),
        "Book Url": _filled(book.amazon_url or book.url or book.series_book_1),
        "Unique?": unique,
        "Series?": _filled(book.series_flag or ("Y" if book.book_type == "Series" else "N")),
        "# of hrs": _filled(book.total_hours),
        "Book1 No Of Rating": _rating_count_for(book, 1),
    }
    for column, value in values.items():
        _set(row, column, value)


def _apply_sample_helper_columns(row: dict, book: Book, duplicate_status: str) -> None:
    if "Scope?" in row:
        contact = book.contact
        reachable = bool(contact and (contact.email_id or contact.contact_forms or contact.facebook_link))
        row["Scope?"] = "Reachable" if reachable else ""
    if "Duplicate Check" in row:
        row["Duplicate Check"] = duplicate_status
    if "Unnamed: 44" in row:
        row["Unnamed: 44"] = book.clean_author_names or book.author


def _apply_diagnostic_columns(row: dict, book: Book, quality_row: dict | None) -> None:
    if not quality_row:
        return
    issues = quality_row.get("issues") or []
    values = {
        "Data Quality Score": quality_row.get("quality_score"),
        "Data Quality Critical Count": quality_row.get("critical_count"),
        "Data Quality Warning Count": quality_row.get("warning_count"),
        "Data Quality Issues": "; ".join(issue.get("code", "") for issue in issues if issue.get("code")),
        "Missing Fields": "; ".join(quality_row.get("missing_fields") or []),
        "Genre Source": quality_row.get("genre_source", ""),
        "Genre Reason": quality_row.get("genre_reason", ""),
        "Source ASIN": quality_row.get("source_asin", ""),
        "Detail ASIN": quality_row.get("detail_asin", ""),
        "Detail URL": quality_row.get("detail_url", ""),
        "Source Format": quality_row.get("source_format", ""),
        "Detail Format": quality_row.get("detail_format", ""),
        "Amazon Quality Flags": "; ".join(quality_row.get("amazon_quality_flags") or []),
    }
    for column, value in values.items():
        _set(row, column, value)


def _tier_profile_for_export(book: Book) -> dict[str, str]:
    derived = commissioning_tier_profile(book)
    persisted = {
        "Tier": book.tier,
        "GR Ratings": book.gr_ratings,
        "Trope": book.trope,
        "Length": book.length,
        "MG (Min)": book.mg_min,
        "MG (Max)": book.mg_max,
        "Rev share (min)": book.rev_share_min,
        "Rev Share (max)": book.rev_share_max,
    }
    return {column: _clean_export_text(value) or derived[column] for column, value in persisted.items()}


def _range_label(min_value: str, max_value: str) -> str:
    left = _clean_export_text(min_value)
    right = _clean_export_text(max_value)
    if left and right:
        return left if left == right else f"{left}-{right}"
    return left or right


def flatten_book(book: Book, columns: list[str] | None = None) -> dict:
    row = {column: "" for column in (columns or get_reference_columns())}
    amazon_payload = _amazon_payload(book)
    tier_profile = _tier_profile_for_export(book)
    rating_value = book.rating
    rating_count_value = book.rating_count
    customer_reviews = _filled(amazon_payload.get("customer_reviews"), "")
    if not customer_reviews and rating_value is not None:
        customer_reviews = f"{rating_value:g} out of 5 stars"
        if rating_count_value is not None:
            customer_reviews += f"; {rating_count_value} ratings"
    rank_value = amazon_payload.get("best_sellers_rank_number") or book.best_sellers_rank
    _set(row, "Title", _clean_title(book.title))
    _set(row, "URL", book.amazon_url or book.url)
    _set(row, "Rating", rating_value)
    _set(row, "no. of rating", rating_count_value)
    _set(row, "Publisher", _clean_export_text(book.publisher))
    _set(row, "Publisher name", _clean_export_text(book.publisher))
    _set(row, "Publication date", _clean_export_text(book.publication_date))
    _set(row, "Part of series", book.part_of_series)
    _set(row, "Language", book.language)
    _set(row, "Author", book.author)
    _set(row, "Author name", book.author)
    _set(row, "Best Sellers Rank", _clean_rank(rank_value))
    _set(row, "Customer Reviews", customer_reviews)
    _set(row, "Goodreads rating", book.goodreads_rating)
    _set(row, "Goodreads no of rating", book.goodreads_rating_count)
    for column, value in tier_profile.items():
        _set(row, column, value)
    _set(row, "Tier Mapping", tier_profile["Tier"])
    _set(row, "Minimum Gurantee", _range_label(tier_profile["MG (Min)"], tier_profile["MG (Max)"]))
    _set(row, "Rev share %", _range_label(tier_profile["Rev share (min)"], tier_profile["Rev Share (max)"]))
    _set(row, "Print Length", book.print_length)
    _set(row, "Book number", book.book_number)
    _set(row, "Format", _clean_format(book.format))
    _set(row, "Synopsis/Summary", book.synopsis)
    _set(row, "Genre", book.genre)
    _set(row, "Sub-genre", book.sub_genre)
    _set(row, "Type", book.book_type)
    _set(row, "Cleaned Series Name", book.cleaned_series_name)
    _set(row, "Series?", book.series_flag)
    _set(row, "Duplicates basis series?", book.duplicates_basis_series)
    _set(row, "Author Check", book.author_check)
    _set(row, "Clean Author Names", book.clean_author_names)
    _set(row, "# of total pages in series", book.total_pages_in_series)
    _set(row, "# Total word count", book.total_word_count)
    _set(row, "# of Hrs", book.total_hours)
    _set(row, "Goodread Link", book.goodread_link)
    _set(row, "Series Book 1", book.series_book_1)
    _set(row, "Series Link", book.series_link)
    _set(row, "Remarks", book.remarks)
    _set(row, "# of primary book", book.primary_book_count)
    _set(row, "Series books", book.primary_book_count or book.book_number)
    _set(row, "Audio score", book.audio_score)
    _set(row, "GR Book 1 Rating", book.gr_book_1_rating)
    _set(row, "GR Book 2 Rating", book.gr_book_2_rating)
    _set(row, "GR Book 3 Rating", book.gr_book_3_rating)
    _set(row, "GR Book 4 Rating", book.gr_book_4_rating)
    _set(row, "GR Book 5 Rating", book.gr_book_5_rating)
    _set(row, "GR Book 6 Rating", book.gr_book_6_rating)
    _set(row, "GR Book 7 Rating", book.gr_book_7_rating)
    _set(row, "GR Book 8 Rating", book.gr_book_8_rating)
    _set(row, "GR Book 9 Rating", book.gr_book_9_rating)
    _set(row, "GR Book 1O Rating", book.gr_book_10_rating)
    _set(row, "GR Book 10 Rating", book.gr_book_10_rating)
    for column in list(row):
        index = _series_rating_index_from_column(column)
        if index:
            _set(row, column, _series_rating_for_export(book, index))
    _set(row, "Final List?", book.final_list)
    _set(row, "Rationale", book.rationale)
    apply_requested_mapped_columns(row, book)
    if book.contact:
        _set(row, "Email ID", book.contact.email_id)
        _set(row, "Email ID source", book.contact.email_source_note)
        _set(row, "Email Source", book.contact.email_source_note)
        _set(row, "Email type", book.contact.email_type)
        _set(row, "Contact Forms", book.contact.contact_forms)
        _set(row, "Facebook link", book.contact.facebook_link)
        _set(row, "Publisher's details", book.contact.publisher_details)
        _set(row, "Author Email", book.contact.author_email)
        _set(row, "Agent Email", book.contact.agent_email)
        _set(row, "Website", book.contact.website)
    return row


def _duplicate_statuses(books: list[Book]) -> dict[int, str]:
    seen: set[str] = set()
    statuses: dict[int, str] = {}
    for book in books:
        key = f"{_clean_export_text(book.title).lower()}|{_clean_export_text(book.author).lower()}"
        statuses[book.id] = "Duplicate" if key in seen else "First Entry"
        seen.add(key)
    return statuses


def _normalize_profile(profile: str, export_format: str) -> str:
    normalized = (profile or "").lower().replace("-", "_")
    if normalized:
        return normalized
    return "full_diagnostic" if export_format.lower() == "json" else "sample_compatible"


def generate_export(db: Session, batch: Batch, export_format: str, *, profile: str = "", require_ready: bool = False) -> ExportRecord:
    books = db.query(Book).filter(Book.batch_id == batch.id).order_by(Book.id.asc()).all()
    for book in books:
        apply_benchmark_mapping(book)
    db.commit()
    ext = export_format.lower()
    normalized_profile = _normalize_profile(profile, ext)
    quality = batch_data_quality(db, batch.id)
    if require_ready and not quality.get("ready"):
        raise ValueError("Batch is not export-ready. Resolve critical Data Quality issues first.")

    columns = export_columns_for_profile(normalized_profile, books=books)
    quality_by_id = {row["book_id"]: row for row in quality["rows"]}
    duplicate_status = _duplicate_statuses(books)
    rows = []
    for book in books:
        row = flatten_book(book, columns)
        _apply_sample_helper_columns(row, book, duplicate_status.get(book.id, "First Entry"))
        _apply_diagnostic_columns(row, book, quality_by_id.get(book.id))
        rows.append(row)
    frame = pd.DataFrame(rows, columns=columns)

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    profile_slug = normalized_profile.replace("_compatible", "").replace("_diagnostic", "_diag")
    workspace_slug = re.sub(r"[^A-Za-z0-9_.:-]+", "-", batch.workspace_id or "public").strip("-") or "public"
    output_dir = GENERATED_DIR / workspace_slug / f"batch_{batch.id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{profile_slug}_{stamp}.{ext}"

    if ext == "csv":
        frame.to_csv(path, index=False, encoding="utf-8")
    elif ext == "json":
        path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    elif ext == "xlsx":
        frame.to_excel(path, index=False)
    elif ext == "pdf":
        pdf = canvas.Canvas(str(path), pagesize=A4)
        pdf.setTitle(f"Commissioning Batch {batch.id}")
        y = 800
        pdf.drawString(40, y, f"Commissioning Batch {batch.id}: {batch.name}")
        y -= 24
        for row in rows[:25]:
            line = f"{row.get('Title', '')} | {row.get('Author', '')} | {row.get('Genre', '')} | {row.get('Goodreads Rating', '')}"
            pdf.drawString(40, y, line[:110])
            y -= 18
            if y < 60:
                pdf.showPage()
                y = 800
        pdf.save()
    else:
        raise ValueError(f"Unsupported export format: {export_format}")

    record = ExportRecord(
        batch_id=batch.id,
        export_format=ext,
        file_path=str(path),
        row_count=len(rows),
        metadata_json={
            "columns": list(frame.columns),
            "filename": path.name,
            "profile": normalized_profile,
            "workspace_id": batch.workspace_id,
            "quality_summary": {key: value for key, value in quality.items() if key != "rows"},
        },
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    storage_metadata = upload_export_file(path, batch, record)
    if storage_metadata:
        record.file_path = storage_metadata["gcs_uri"]
        record.metadata_json = {**(record.metadata_json or {}), **storage_metadata, "local_staging_deleted": True}
        try:
            path.unlink(missing_ok=True)
        except OSError:
            record.metadata_json = {**record.metadata_json, "local_staging_deleted": False}
        db.commit()
        db.refresh(record)
    return record

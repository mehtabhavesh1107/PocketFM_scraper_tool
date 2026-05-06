from __future__ import annotations

import os
import re
import threading
import gc
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import Batch, Book, BookSource, Contact, Job, JobEvent, SourceLink
from ..services.amazon_http import amazon_item_to_payload, discover_amazon_items, fetch_amazon_item_record
from ..services.contact_service import enrich_book_contacts
from ..services.discovery_service import discover_books
from ..services.export_service import generate_export
from ..services.goodreads_service import create_scraper, enrich_row
from ..services.mapping_service import apply_benchmark_mapping, apply_metric_mapping


def _env_int(name: str, default: int, *, minimum: int = 1, maximum: int = 16) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(value, maximum))


GOODREADS_LOOKUP_WORKERS = _env_int("GOODREADS_LOOKUP_WORKERS", 4, maximum=12)
GOODREADS_GC_EVERY = _env_int("GOODREADS_GC_EVERY", 5, minimum=1, maximum=50)
ASIN_TEXT_RE = re.compile(r"^[A-Z0-9]{10}$", re.IGNORECASE)


def _env_float(name: str, default: float, *, minimum: float = 0.0, maximum: float = 60.0) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(value, maximum))


DISTRIBUTED_AMAZON_DETAILS = os.getenv("COMMISSIONING_DISTRIBUTED_AMAZON_DETAILS", "1").strip().lower() not in {
    "0",
    "false",
    "no",
}
AMAZON_DETAIL_ITEM_DELAY_SECONDS = _env_float("AMAZON_DETAIL_ITEM_DELAY_SECONDS", 1.0, maximum=10.0)
CHILD_JOB_FALLBACK_AFTER_SECONDS = _env_float("COMMISSIONING_CHILD_JOB_FALLBACK_AFTER_SECONDS", 3.0, maximum=30.0)
CHILD_JOB_STAGE = "amazon_detail_item"


class JobLogger:
    def __init__(self, db: Session, job: Job):
        self.db = db
        self.job = job

    def event(self, level: str, message: str, *, progress_current: int | None = None, progress_total: int | None = None, failure_bucket: str = "", payload: dict | None = None) -> None:
        if progress_current is not None:
            self.job.progress_current = progress_current
        if progress_total is not None:
            self.job.progress_total = progress_total
        if self.job.progress_total:
            self.job.progress_percent = round((self.job.progress_current / self.job.progress_total) * 100, 2)
        self.job.message = message
        if failure_bucket:
            self.job.failure_bucket = failure_bucket
        self.db.add(
            JobEvent(
                job_id=self.job.id,
                level=level,
                message=message,
                payload_json=payload or {},
                progress_percent=self.job.progress_percent,
            )
        )
        self.db.commit()

    def start(self, message: str) -> None:
        self.job.status = "running"
        self.job.started_at = datetime.utcnow()
        self.event("info", message, progress_current=0)

    def finish(self, message: str) -> None:
        self.job.status = "completed"
        self.job.finished_at = datetime.utcnow()
        self.event("info", message)

    def fail(self, message: str, *, failure_bucket: str = "job_error") -> None:
        self.job.status = "failed"
        self.job.error = message
        self.job.finished_at = datetime.utcnow()
        self.event("error", message, failure_bucket=failure_bucket)


def _normalize_int(value) -> int | None:
    if value in (None, "", "N/A"):
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except ValueError:
        return None


def _normalize_float(value) -> float | None:
    if value in (None, "", "N/A"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _calculate_book_metrics(book: Book) -> None:
    apply_metric_mapping(book)


def _key_part(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


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


def _lead_author(author: str | None, contributors: list[dict] | None = None) -> str:
    for contributor in contributors or []:
        name = _key_part(str(contributor.get("name", "")))
        role = _key_part(str(contributor.get("role", "")))
        if name and "author" in role:
            return str(contributor.get("name", "")).strip()
    text = " ".join((author or "").split()).strip()
    if not text:
        return ""
    return re.split(r",|\band\b", text, maxsplit=1)[0].strip()


def _goodreads_cache_key(book: Book) -> str:
    title = _key_part(_clean_title_for_lookup(book.title))
    author = _key_part(book.clean_author_names or book.author)
    amazon = (book.provenance_json or {}).get("amazon", {})
    amazon = amazon if isinstance(amazon, dict) else {}
    isbn = _key_part(amazon.get("isbn_13") or amazon.get("isbn_10") or "")
    asin = _key_part(amazon.get("detail_asin") or amazon.get("source_asin") or "")
    return f"book:{title}|author:{author}|isbn:{isbn}|asin:{asin}"


def _goodreads_row(book: Book) -> dict:
    amazon = (book.provenance_json or {}).get("amazon", {})
    amazon = amazon if isinstance(amazon, dict) else {}
    return {
        "Title": _clean_title_for_lookup(book.title),
        "Author": book.clean_author_names or book.author,
        "Genre": book.genre,
        "Publisher": book.publisher,
        "Publication date": book.publication_date,
        "Print Length": book.print_length,
        "ISBN-10": amazon.get("isbn_10", ""),
        "ISBN-13": amazon.get("isbn_13", ""),
        "Book number": book.book_number,
        "Part of series": book.part_of_series,
        "Cleaned Series Name": book.cleaned_series_name,
        "Goodread Link": book.goodread_link,
        "Series Book 1": book.series_book_1,
        "Series Link": book.series_link,
    }


def _upsert_book(session: Session, batch_id: int, record: dict, source_type: str, source_url: str) -> Book:
    title = (record.get("title") or "").strip()
    author = (record.get("author") or "").strip()
    book = (
        session.query(Book)
        .filter(Book.batch_id == batch_id, Book.title == title, Book.author == author)
        .one_or_none()
    )
    if book is None:
        book = Book(batch_id=batch_id, title=title, author=author)
        session.add(book)
        session.flush()
    book.url = record.get("url") or book.url
    book.amazon_url = record.get("amazon_url") or book.amazon_url
    if source_type == "goodreads":
        if record.get("rating") is not None:
            book.goodreads_rating = str(record.get("rating") or "")
        if record.get("rating_count") is not None:
            book.goodreads_rating_count = str(record.get("rating_count") or "")
    else:
        book.rating = _normalize_float(record.get("rating")) if record.get("rating") is not None else book.rating
        book.rating_count = _normalize_int(record.get("rating_count")) if record.get("rating_count") is not None else book.rating_count
    book.publisher = record.get("publisher") or book.publisher
    book.publication_date = record.get("publication_date") or book.publication_date
    book.part_of_series = record.get("part_of_series") or book.part_of_series
    book.language = record.get("language") or book.language
    book.best_sellers_rank = record.get("best_sellers_rank") or book.best_sellers_rank
    book.print_length = record.get("print_length") or book.print_length
    book.book_number = record.get("book_number") or book.book_number
    book.format = record.get("format") or book.format
    book.synopsis = record.get("synopsis") or book.synopsis
    book.genre = record.get("genre") or book.genre
    book.sub_genre = record.get("sub_genre") or book.sub_genre
    book.cleaned_series_name = record.get("cleaned_series_name") or book.cleaned_series_name
    book.series_flag = record.get("series_flag") or book.series_flag
    book.goodread_link = record.get("goodread_link") or book.goodread_link
    for field in (
        "total_pages_in_series",
        "total_word_count",
        "total_hours",
        "tier",
        "gr_ratings",
        "trope",
        "length",
        "mg_min",
        "mg_max",
        "rev_share_min",
        "rev_share_max",
        "series_book_1",
        "series_link",
        "remarks",
        "primary_book_count",
        "final_list",
        "rationale",
    ):
        value = record.get(field)
        if value not in (None, ""):
            setattr(book, field, str(value))
    book.goodreads_rating = str(record.get("goodreads_rating") or book.goodreads_rating or "")
    book.goodreads_rating_count = str(record.get("goodreads_rating_count") or book.goodreads_rating_count or "")
    lead_author = _lead_author(author, record.get("contributors") if isinstance(record.get("contributors"), list) else None)
    if lead_author:
        book.clean_author_names = lead_author
    provenance = dict(book.provenance_json or {})
    source_payload = record.get("source_payload") or {}
    provenance[source_type] = {
        "source_url": source_url,
        "title": title,
        "author": author,
        "source_asin": record.get("source_asin") or source_payload.get("source_asin") or source_payload.get("asin", ""),
        "detail_asin": record.get("detail_asin") or source_payload.get("detail_asin", ""),
        "detail_url": record.get("detail_url") or source_payload.get("detail_url", ""),
        "detail_fetched": bool(source_payload.get("detail_fetched")),
        "source_format": record.get("source_format") or source_payload.get("source_format", ""),
        "detail_format": record.get("detail_format") or source_payload.get("detail_format", ""),
        "isbn_10": record.get("isbn_10") or source_payload.get("isbn_10", ""),
        "isbn_13": record.get("isbn_13") or source_payload.get("isbn_13", ""),
        "best_sellers_rank_number": record.get("best_sellers_rank_number") or source_payload.get("best_sellers_rank_number", ""),
        "best_sellers_rank_text": record.get("best_sellers_rank_text") or source_payload.get("best_sellers_rank_text", ""),
        "customer_reviews": record.get("customer_reviews") or source_payload.get("customer_reviews", ""),
        "contributors": record.get("contributors") or source_payload.get("contributors", []),
        "amazon_quality_flags": record.get("amazon_quality_flags") or source_payload.get("amazon_quality_flags", []),
        "normalized": {
            key: record.get(key)
            for key in (
                "title",
                "author",
                "url",
                "amazon_url",
                "rating",
                "rating_count",
                "publisher",
                "publication_date",
                "best_sellers_rank",
                "print_length",
                "format",
                "genre",
                "isbn_10",
                "isbn_13",
            )
            if key in record
        },
    }
    book.provenance_json = provenance
    _calculate_book_metrics(book)
    apply_benchmark_mapping(book)

    source = (
        session.query(BookSource)
        .filter(BookSource.book_id == book.id, BookSource.source_type == source_type, BookSource.source_url == source_url)
        .one_or_none()
    )
    if source is None:
        source = BookSource(book_id=book.id, source_type=source_type, source_url=source_url)
        session.add(source)
    source.external_id = record.get("source_asin") or source_payload.get("source_asin") or source_payload.get("asin", "") or source.external_id
    source.raw_payload_json = record.get("source_payload") or {}
    source.normalized_payload_json = record
    session.commit()
    session.refresh(book)
    return book


def _apply_goodreads_updates(book: Book, updates: dict) -> None:
    gr_count = _normalize_int(updates.get("Goodreads no of rating"))
    if gr_count is not None and gr_count <= 1 and (book.rating_count or 0) >= 1000:
        updates = {
            key: value
            for key, value in updates.items()
            if key.startswith("Goodreads Match") or key in {"Goodreads Candidates", "Goodreads Search Attempts", "Goodreads ISBNs Used"}
        } | {"Goodread Link": updates.get("Goodread Link", book.goodread_link)}
    match_status = str(updates.get("Goodreads Match Status", "") or "").lower()
    resolved_book_url = updates.get("Resolved Goodreads Book") or updates.get("Series Book 1")
    if match_status in {"matched", "accepted"} and resolved_book_url:
        book.goodread_link = resolved_book_url
    else:
        book.goodread_link = updates.get("Goodread Link", book.goodread_link)
    book.series_book_1 = updates.get("Series Book 1", book.series_book_1)
    book.series_link = updates.get("Series Link", book.series_link)
    book.primary_book_count = str(updates.get("# of primary book", book.primary_book_count) or "")
    book.total_pages_in_series = str(updates.get("# of total pages in series", book.total_pages_in_series) or "")
    book.gr_book_1_rating = str(updates.get("GR Book 1 Rating", book.gr_book_1_rating) or "")
    book.gr_book_2_rating = str(updates.get("GR Book 2 Rating", book.gr_book_2_rating) or "")
    book.gr_book_3_rating = str(updates.get("GR Book 3 Rating", book.gr_book_3_rating) or "")
    book.gr_book_4_rating = str(updates.get("GR Book 4 Rating", book.gr_book_4_rating) or "")
    book.gr_book_5_rating = str(updates.get("GR Book 5 Rating", book.gr_book_5_rating) or "")
    book.gr_book_6_rating = str(updates.get("GR Book 6 Rating", book.gr_book_6_rating) or "")
    book.gr_book_7_rating = str(updates.get("GR Book 7 Rating", book.gr_book_7_rating) or "")
    book.gr_book_8_rating = str(updates.get("GR Book 8 Rating", book.gr_book_8_rating) or "")
    book.gr_book_9_rating = str(updates.get("GR Book 9 Rating", book.gr_book_9_rating) or "")
    book.gr_book_10_rating = str(updates.get("GR Book 1O Rating", book.gr_book_10_rating) or "")
    book.goodreads_rating = str(updates.get("Goodreads rating", book.goodreads_rating) or "")
    book.goodreads_rating_count = str(updates.get("Goodreads no of rating", book.goodreads_rating_count) or "")
    _calculate_book_metrics(book)
    apply_benchmark_mapping(book)
    provenance = dict(book.provenance_json or {})
    provenance["goodreads"] = updates
    book.provenance_json = provenance


def _has_goodreads_match(updates: dict) -> bool:
    if not updates:
        return False
    if str(updates.get("Goodreads Match Status", "") or "").lower() in {"matched", "accepted"}:
        return True
    if any(
        _value_present(updates.get(key))
        for key in (
            "Goodreads rating",
            "Goodreads no of rating",
            "Series Book 1",
            "GR Book 1 Rating",
            "Published Year",
            "Publisher name",
        )
    ):
        return True
    link = str(updates.get("Goodread Link", "") or "")
    return "/book/show/" in link or "/work/editions/" in link


def _enrich_goodreads_for_batch(db: Session, logger: JobLogger, batch_id: int, *, auto: bool = False) -> int:
    books = db.query(Book).filter(Book.batch_id == batch_id).order_by(Book.id.asc()).all()
    if not books:
        return 0
    label = "Auto-mapping Goodreads" if auto else "Enriching Goodreads"
    groups: dict[str, dict] = {}
    books_by_id = {book.id: book for book in books}
    for book in books:
        key = _goodreads_cache_key(book)
        group = groups.setdefault(
            key,
            {
                "row": _goodreads_row(book),
                "book_ids": [],
                "label": book.cleaned_series_name or book.part_of_series or book.title,
            },
        )
        group["book_ids"].append(book.id)

    logger.event(
        "info",
        f"{label} for {len(books)} books across {len(groups)} unique Goodreads lookups.",
        progress_current=0,
        progress_total=max(len(groups), 1),
    )

    worker_state = threading.local()

    def resolve_group(key: str, row: dict) -> tuple[str, dict]:
        scraper = getattr(worker_state, "scraper", None)
        if scraper is None:
            scraper = create_scraper()
            worker_state.scraper = scraper
        return key, enrich_row(row, scraper)

    def apply_group(key: str, updates: dict) -> int:
        applied = 0
        for book_id in groups[key]["book_ids"]:
            book = books_by_id[book_id]
            _apply_goodreads_updates(book, updates)
            applied += 1
        db.commit()
        return applied

    enriched = 0
    completed = 0
    items = list(groups.items())
    worker_count = min(GOODREADS_LOOKUP_WORKERS, len(items))
    if worker_count <= 1:
        for key, group in items:
            completed += 1
            try:
                _, updates = resolve_group(key, group["row"])
                applied = apply_group(key, updates)
                matched = _has_goodreads_match(updates)
                if matched:
                    enriched += applied
                logger.event(
                    "info",
                    f"Goodreads {'matched' if matched else 'found no confident match for'} {group['label']} and applied to {applied} book(s).",
                    progress_current=completed,
                    progress_total=len(items),
                )
                if completed % GOODREADS_GC_EVERY == 0:
                    gc.collect()
            except Exception as exc:
                logger.event(
                    "warning",
                    f"Goodreads mapping failed for {group['label']}: {exc}",
                    progress_current=completed,
                    progress_total=len(items),
                    failure_bucket="goodreads_no_match",
                )
        return enriched

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="goodreads-map") as executor:
        futures = {
            executor.submit(resolve_group, key, group["row"]): key
            for key, group in items
        }
        for future in as_completed(futures):
            key = futures[future]
            group = groups[key]
            completed += 1
            try:
                _, updates = future.result()
                applied = apply_group(key, updates)
                matched = _has_goodreads_match(updates)
                if matched:
                    enriched += applied
                logger.event(
                    "info",
                    f"Goodreads {'matched' if matched else 'found no confident match for'} {group['label']} and applied to {applied} book(s).",
                    progress_current=completed,
                    progress_total=len(items),
                )
                if completed % GOODREADS_GC_EVERY == 0:
                    gc.collect()
            except Exception as exc:
                logger.event(
                    "warning",
                    f"Goodreads mapping failed for {group['label']}: {exc}",
                    progress_current=completed,
                    progress_total=len(items),
                    failure_bucket="goodreads_no_match",
                )
    return enriched


def _value_present(value) -> bool:
    if value is None:
        return False
    return str(value).strip() not in {"", "N/A", "None", "nan"}


def _scrape_coverage(books: list[Book]) -> dict:
    fields = {
        "title": lambda book: "" if ASIN_TEXT_RE.fullmatch(book.title or "") else book.title,
        "author": lambda book: book.author,
        "rating": lambda book: book.rating,
        "publisher": lambda book: book.publisher,
        "publication_date": lambda book: book.publication_date,
        "best_sellers_rank": lambda book: book.best_sellers_rank,
        "print_length": lambda book: book.print_length,
        "synopsis": lambda book: book.synopsis,
        "goodreads_rating": lambda book: book.goodreads_rating,
        "series_link": lambda book: book.series_link,
    }
    total = len(books)
    coverage = {
        name: sum(1 for book in books if _value_present(getter(book)))
        for name, getter in fields.items()
    }
    missing = {name: total - count for name, count in coverage.items()}
    return {"total": total, "coverage": coverage, "missing": missing}


def _amazon_books(books: list[Book]) -> list[Book]:
    amazon_books = []
    for book in books:
        provenance = book.provenance_json or {}
        amazon = provenance.get("amazon", {}) if isinstance(provenance, dict) else {}
        if isinstance(amazon, dict) and amazon.get("source_asin"):
            amazon_books.append(book)
    return amazon_books


def _amazon_detail_coverage_too_low(books: list[Book], summary: dict) -> bool:
    total = summary["total"] or 0
    if total < 10:
        return False
    coverage = summary["coverage"]
    return (
        coverage["title"] < total * 0.9
        or coverage["author"] < total * 0.8
        or coverage["publisher"] < total * 0.5
        or coverage["publication_date"] < total * 0.5
    )


def _coverage_message(summary: dict) -> str:
    total = summary["total"] or 0
    coverage = summary["coverage"]
    return (
        "Amazon core coverage: "
        f"publisher {coverage['publisher']}/{total}, "
        f"date {coverage['publication_date']}/{total}, "
        f"rank {coverage['best_sellers_rank']}/{total}, "
        f"length {coverage['print_length']}/{total}, "
        f"synopsis {coverage['synopsis']}/{total}; "
        f"Goodreads rating {coverage['goodreads_rating']}/{total}, "
        f"series link {coverage['series_link']}/{total}."
    )


def _child_stats(db: Session, child_job_ids: list[str]) -> dict[str, int]:
    if not child_job_ids:
        return {"total": 0, "completed": 0, "failed": 0, "running": 0, "queued": 0}
    rows = db.query(Job.status).filter(Job.id.in_(child_job_ids)).all()
    stats = {"total": len(child_job_ids), "completed": 0, "failed": 0, "running": 0, "queued": 0}
    for (status,) in rows:
        if status in stats:
            stats[status] += 1
    return stats


def _parent_checkpoint(job: Job) -> dict:
    checkpoint = dict(job.checkpoint_json or {})
    checkpoint.setdefault("child_job_ids", [])
    return checkpoint


def _update_parent_from_children(db: Session, parent_job_id: str, *, message: str | None = None) -> None:
    parent = db.get(Job, parent_job_id)
    if parent is None or parent.status not in {"queued", "running"}:
        return
    checkpoint = _parent_checkpoint(parent)
    child_job_ids = list(checkpoint.get("child_job_ids") or [])
    stats = _child_stats(db, child_job_ids)
    parent.progress_current = stats["completed"] + stats["failed"]
    parent.progress_total = max(stats["total"], 1)
    parent.progress_percent = round((parent.progress_current / parent.progress_total) * 100, 2)
    parent.message = message or (
        f"Fetched Amazon details {parent.progress_current}/{stats['total']} "
        f"({stats['failed']} failed/deferred)."
    )
    db.add(
        JobEvent(
            job_id=parent.id,
            level="info" if not stats["failed"] else "warning",
            message=parent.message,
            payload_json={"child_stats": stats},
            progress_percent=parent.progress_percent,
        )
    )
    db.commit()


def _queue_amazon_detail_jobs(
    db: Session,
    *,
    parent_job: Job,
    batch_id: int,
    source: SourceLink,
    items: list,
) -> list[Job]:
    child_jobs: list[Job] = []
    total = len(items)
    for index, item in enumerate(items, start=1):
        child = Job(
            batch_id=batch_id,
            stage=CHILD_JOB_STAGE,
            status="queued",
            message=f"Amazon detail queued {index}/{total}: {item.asin or item.title}",
            payload_json={
                "parent_job_id": parent_job.id,
                "source_id": source.id,
                "source_url": source.url,
                "source_type": source.source_type,
                "item_index": index,
                "item_total": total,
                "item": amazon_item_to_payload(item),
            },
        )
        db.add(child)
        child_jobs.append(child)
    checkpoint = _parent_checkpoint(parent_job)
    db.flush()
    checkpoint["child_job_ids"] = [child.id for child in child_jobs]
    checkpoint["distributed_amazon_details"] = True
    parent_job.checkpoint_json = checkpoint
    db.commit()
    for child in child_jobs:
        db.refresh(child)
    return child_jobs


def _submit_inline_child_jobs(child_jobs: list[Job], batch_id: int) -> bool:
    from .manager import job_manager

    if not job_manager.runs_inline:
        return False
    max_workers = getattr(job_manager.executor, "_max_workers", 1) if job_manager.executor is not None else 1
    if max_workers <= 1:
        return False
    for child in child_jobs:
        job_manager.submit(child.id, run_amazon_detail_item_job, child.id, batch_id)
    return True


def _claim_next_child_job(db: Session, child_job_ids: list[str]) -> str | None:
    if not child_job_ids:
        return None
    child = (
        db.query(Job)
        .filter(Job.id.in_(child_job_ids), Job.status == "queued", Job.stage == CHILD_JOB_STAGE)
        .order_by(Job.created_at.asc())
        .first()
    )
    if child is None:
        return None
    child.status = "running"
    child.message = "Claimed by parent scrape orchestrator."
    db.add(
        JobEvent(
            job_id=child.id,
            level="info",
            message=child.message,
            payload_json={"claimed_by": "parent_orchestrator"},
            progress_percent=child.progress_percent,
        )
    )
    db.commit()
    return child.id


def _wait_for_amazon_detail_jobs(db: Session, logger: JobLogger, child_jobs: list[Job], batch_id: int) -> dict[str, int]:
    child_job_ids = [child.id for child in child_jobs]
    if not child_job_ids:
        return {"total": 0, "completed": 0, "failed": 0, "running": 0, "queued": 0}

    inline_children_submitted = _submit_inline_child_jobs(child_jobs, batch_id)
    last_progress = -1
    fallback_started_at = time.monotonic()
    while True:
        stats = _child_stats(db, child_job_ids)
        done = stats["completed"] + stats["failed"]
        if done != last_progress:
            last_progress = done
            logger.event(
                "info" if stats["failed"] == 0 else "warning",
                f"Fetched Amazon details {done}/{stats['total']} ({stats['failed']} failed/deferred).",
                progress_current=done,
                progress_total=stats["total"],
                payload={"child_stats": stats},
            )
        if done >= stats["total"]:
            return stats

        # Database-backed deployments need more than one worker for true parallelism.
        # This fallback prevents a one-worker demo from deadlocking while other workers
        # still get first chance to claim queued detail jobs.
        if not inline_children_submitted and time.monotonic() - fallback_started_at >= CHILD_JOB_FALLBACK_AFTER_SECONDS:
            claimed_child_id = _claim_next_child_job(db, child_job_ids)
            if claimed_child_id:
                run_amazon_detail_item_job(claimed_child_id, batch_id)
                continue
        time.sleep(0.75)


def _distributed_amazon_source(
    db: Session,
    logger: JobLogger,
    *,
    parent_job: Job,
    batch_id: int,
    source: SourceLink,
) -> dict[str, int]:
    items = discover_amazon_items(source.url, max_results=0)
    total = len(items)
    logger.event(
        "info",
        f"Found {total} Amazon books. Queuing product detail workers...",
        progress_current=0,
        progress_total=max(total, 1),
        payload={"source_id": source.id, "source_type": source.source_type, "item_total": total},
    )
    if not items:
        return {"total": 0, "completed": 0, "failed": 0, "running": 0, "queued": 0}
    child_jobs = _queue_amazon_detail_jobs(db, parent_job=parent_job, batch_id=batch_id, source=source, items=items)
    return _wait_for_amazon_detail_jobs(db, logger, child_jobs, batch_id)


def run_amazon_detail_item_job(job_id: str, batch_id: int) -> None:
    db = SessionLocal()
    job = db.get(Job, job_id)
    logger = JobLogger(db, job)
    try:
        payload = dict(job.payload_json or {})
        item_payload = payload.get("item") if isinstance(payload.get("item"), dict) else {}
        item_index = int(payload.get("item_index") or 0)
        item_total = int(payload.get("item_total") or 0)
        parent_job_id = str(payload.get("parent_job_id") or "")
        source_url = str(payload.get("source_url") or "")

        logger.start(f"Fetching Amazon details {item_index}/{item_total}.")
        if AMAZON_DETAIL_ITEM_DELAY_SECONDS:
            stagger = (sum(ord(char) for char in job_id[-6:]) % 400) / 1000
            time.sleep(AMAZON_DETAIL_ITEM_DELAY_SECONDS + stagger)
        record = fetch_amazon_item_record(item_payload)
        book = _upsert_book(db, batch_id, record, "amazon", source_url)
        title = record.get("title") or record.get("amazon_url") or "Amazon book"
        logger.finish(f"Fetched Amazon details {item_index}/{item_total}: {title}")
        if parent_job_id:
            _update_parent_from_children(
                db,
                parent_job_id,
                message=f"Fetched Amazon details {item_index}/{item_total}: {title}",
            )
        del book
    except Exception as exc:
        logger.fail(f"Amazon detail item failed: {exc}", failure_bucket="amazon_detail_item_failed")
        parent_job_id = str((job.payload_json or {}).get("parent_job_id") or "") if job else ""
        if parent_job_id:
            _update_parent_from_children(db, parent_job_id)
    finally:
        db.close()


def _run_scrape_job(job_id: str, batch_id: int, *, auto_goodreads: bool) -> None:
    db = SessionLocal()
    job = db.get(Job, job_id)
    batch = db.get(Batch, batch_id)
    logger = JobLogger(db, job)
    try:
        source_links = db.query(SourceLink).filter(SourceLink.batch_id == batch_id).order_by(SourceLink.id.asc()).all()
        mode_label = "scrape" if auto_goodreads else "fast scrape"
        logger.start(f"Starting {mode_label} job for batch '{batch.name}'.")
        logger.event("info", f"Processing {len(source_links)} source links.", progress_total=max(len(source_links), 1))
        discovered_total = 0
        empty_sources: list[str] = []
        blocked_sources: list[str] = []
        failed_sources: list[str] = []
        for idx, source in enumerate(source_links, start=1):
            try:
                if source.source_type == "amazon" and DISTRIBUTED_AMAZON_DETAILS:
                    source.status = "processing"
                    db.commit()
                    stats = _distributed_amazon_source(db, logger, parent_job=job, batch_id=batch_id, source=source)
                    record_count = stats["completed"]
                    if record_count:
                        source.status = "processed"
                    elif stats["total"] and stats["failed"]:
                        source.status = "failed"
                        failed_sources.append(f"{source.url}: all Amazon detail workers failed")
                    else:
                        source.status = "empty"
                        empty_sources.append(source.url)
                else:
                    def source_progress(item_index: int, item_total: int, record: dict | None) -> None:
                        if source.source_type != "amazon" or item_total <= 0:
                            return
                        if item_index <= 0:
                            message = f"Found {item_total} Amazon books. Fetching product details..."
                        else:
                            title = (record or {}).get("title") or (record or {}).get("amazon_url") or "Amazon book"
                            message = f"Fetched Amazon details {item_index}/{item_total}: {title}"
                        logger.event(
                            "info",
                            message,
                            progress_current=item_index,
                            progress_total=item_total,
                            payload={"source_id": source.id, "source_type": source.source_type, "item_total": item_total},
                        )

                    records = discover_books(source.source_type, source.url, 0, on_progress=source_progress)
                    if records:
                        source.status = "processed"
                    else:
                        source.status = "empty"
                        empty_sources.append(source.url)
                    record_count = len(records)
                    for record in records:
                        _upsert_book(db, batch_id, record, source.source_type, source.url)
                    del records
                discovered_total += record_count
                logger.event(
                    "info" if record_count else "warning",
                    f"Discovered {record_count} books from {source.source_type} source {source.id}." if record_count
                    else f"No books found at {source.url} (selectors may not match this URL format).",
                    progress_current=idx,
                    progress_total=len(source_links),
                    payload={"source_id": source.id, "discovered": record_count, "url": source.url},
                    failure_bucket="" if record_count else "source_returned_empty",
                )
                db.commit()
                gc.collect()
            except Exception as exc:
                error_text = str(exc)
                is_blocked = source.source_type == "amazon" and (
                    "rate-limited" in error_text.lower()
                    or "anti-bot" in error_text.lower()
                    or "captcha" in error_text.lower()
                    or "blocked" in error_text.lower()
                )
                source.status = "blocked" if is_blocked else "failed"
                if is_blocked:
                    blocked_sources.append(f"{source.url}: {exc}")
                else:
                    failed_sources.append(f"{source.url}: {exc}")
                db.commit()
                logger.event(
                    "warning",
                    f"Source {source.id} {'blocked/deferred' if is_blocked else 'failed'}: {exc}",
                    progress_current=idx,
                    progress_total=len(source_links),
                    failure_bucket="source_blocked" if is_blocked else "source_discovery_failed",
                    payload={"source_id": source.id, "url": source.url, "error": str(exc)},
                )
        completion_bits = [f"Scrape job completed. Discovered {discovered_total} books."]
        if blocked_sources:
            completion_bits.append(f"{len(blocked_sources)} Amazon source(s) were blocked/rate-limited and left deferred.")
        if failed_sources:
            completion_bits.append(f"{len(failed_sources)} source(s) failed.")
        if empty_sources:
            completion_bits.append(f"{len(empty_sources)} source(s) returned no books.")
        if discovered_total:
            books = db.query(Book).filter(Book.batch_id == batch_id).order_by(Book.id.asc()).all()
            amazon_books = _amazon_books(books)
            if amazon_books:
                amazon_coverage = _scrape_coverage(amazon_books)
                if _amazon_detail_coverage_too_low(amazon_books, amazon_coverage):
                    coverage_message = _coverage_message(amazon_coverage)
                    logger.fail(
                        f"{coverage_message} Amazon detail coverage is too low after retries, so no CSV was generated.",
                        failure_bucket="amazon_detail_coverage_low",
                    )
                    return
            mapped = 0
            if auto_goodreads:
                mapped = _enrich_goodreads_for_batch(db, logger, batch_id, auto=True)
            else:
                logger.event(
                    "info",
                    "Fast scrape completed Amazon detail fetching; Goodreads mapping was skipped for speed.",
                    progress_current=1,
                    progress_total=1,
                    payload={"auto_goodreads": False},
                )
            books = db.query(Book).filter(Book.batch_id == batch_id).order_by(Book.id.asc()).all()
            coverage = _scrape_coverage(books)
            coverage_message = _coverage_message(coverage)
            logger.event(
                "info",
                coverage_message,
                payload={
                    "source_asin_count": sum(
                        1
                        for book in books
                        if ((book.provenance_json or {}).get("amazon", {}) or {}).get("source_asin")
                    ),
                    "detail_pages_fetched": sum(
                        1
                        for book in books
                        if ((book.provenance_json or {}).get("amazon", {}) or {}).get("detail_fetched")
                    ),
                    "format_switch_count": sum(
                        1
                        for book in books
                        if ((book.provenance_json or {}).get("amazon", {}) or {}).get("source_asin")
                        and ((book.provenance_json or {}).get("amazon", {}) or {}).get("detail_asin")
                        and ((book.provenance_json or {}).get("amazon", {}) or {}).get("source_asin")
                        != ((book.provenance_json or {}).get("amazon", {}) or {}).get("detail_asin")
                    ),
                    "missing_core_field_counts": coverage["missing"],
                    "coverage": coverage["coverage"],
                    "total": coverage["total"],
                },
            )
            export = generate_export(db, batch, "csv", profile="sample_compatible")
            if auto_goodreads:
                completion_bits.append(f"Goodreads matched for {mapped} books.")
            else:
                completion_bits.append("Goodreads mapping skipped for fast mode; run Enrich Goodreads to fill Goodreads columns.")
            completion_bits.append(coverage_message)
            completion_bits.append(f"Comprehensive CSV generated: {export.file_path}.")
        logger.finish(" ".join(completion_bits))
    except Exception as exc:
        logger.fail(f"Scrape job failed: {exc}", failure_bucket="scrape_job_failed")
    finally:
        db.close()


def run_scrape_job(job_id: str, batch_id: int) -> None:
    _run_scrape_job(job_id, batch_id, auto_goodreads=True)


def run_fast_scrape_job(job_id: str, batch_id: int) -> None:
    _run_scrape_job(job_id, batch_id, auto_goodreads=False)


def run_goodreads_job(job_id: str, batch_id: int) -> None:
    db = SessionLocal()
    job = db.get(Job, job_id)
    logger = JobLogger(db, job)
    try:
        logger.start("Starting Goodreads enrichment.")
        enriched = _enrich_goodreads_for_batch(db, logger, batch_id)
        export = generate_export(db, db.get(Batch, batch_id), "csv")
        logger.finish(f"Goodreads enrichment completed for {enriched} books. Comprehensive CSV generated: {export.file_path}.")
    except Exception as exc:
        logger.fail(f"Goodreads job failed: {exc}", failure_bucket="goodreads_job_failed")
    finally:
        db.close()


def run_contact_job(job_id: str, batch_id: int) -> None:
    db = SessionLocal()
    job = db.get(Job, job_id)
    logger = JobLogger(db, job)
    contact_update_fields = (
        "email_id",
        "email_source_note",
        "email_type",
        "contact_forms",
        "facebook_link",
        "publisher_details",
        "website",
        "author_email",
        "agent_email",
    )
    try:
        books = db.query(Book).filter(Book.batch_id == batch_id).order_by(Book.id.asc()).all()
        logger.start("Starting author contact enrichment.")
        logger.event("info", f"Researching contacts for {len(books)} books.", progress_total=max(len(books), 1))
        for idx, book in enumerate(books, start=1):
            try:
                updates = enrich_book_contacts(book)
                contact = book.contact or Contact(book_id=book.id)
                if book.contact is None:
                    db.add(contact)
                for field in contact_update_fields:
                    value = updates.get(field)
                    if value not in (None, ""):
                        setattr(contact, field, value)
                db.commit()
                logger.event("info", f"Contact enrichment completed for '{book.title}'.", progress_current=idx, progress_total=len(books))
            except Exception as exc:
                logger.event(
                    "warning",
                    f"Contact enrichment failed for '{book.title}': {exc}",
                    progress_current=idx,
                    progress_total=len(books),
                    failure_bucket="contact_not_found",
                )
        logger.finish("Author contact enrichment completed.")
    except Exception as exc:
        logger.fail(f"Contact job failed: {exc}", failure_bucket="contact_job_failed")
    finally:
        db.close()

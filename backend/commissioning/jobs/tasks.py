from __future__ import annotations

import os
import re
import threading
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import Batch, Book, BookSource, Contact, Job, JobEvent, SourceLink
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
    series = _key_part(book.cleaned_series_name or book.part_of_series)
    author = _key_part(book.clean_author_names or book.author)
    if series and author:
        return f"series:{series}|author:{author}"
    return f"book:{_key_part(_clean_title_for_lookup(book.title))}|author:{author}"


def _goodreads_row(book: Book) -> dict:
    return {
        "Title": _clean_title_for_lookup(book.title),
        "Author": book.clean_author_names or book.author,
        "Genre": book.genre,
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
    book.cleaned_series_name = record.get("cleaned_series_name") or book.cleaned_series_name
    book.series_flag = record.get("series_flag") or book.series_flag
    book.goodread_link = record.get("goodread_link") or book.goodread_link
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
        updates = {"Goodread Link": updates.get("Goodread Link", book.goodread_link)}
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
        failed_sources: list[str] = []
        for idx, source in enumerate(source_links, start=1):
            try:
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
                del records
                gc.collect()
            except Exception as exc:
                source.status = "failed"
                failed_sources.append(f"{source.url}: {exc}")
                db.commit()
                logger.event(
                    "warning",
                    f"Source {source.id} failed: {exc}",
                    progress_current=idx,
                    progress_total=len(source_links),
                    failure_bucket="source_discovery_failed",
                    payload={"source_id": source.id, "url": source.url, "error": str(exc)},
                )
        completion_bits = [f"Scrape job completed. Discovered {discovered_total} books."]
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
                contact.email_id = updates.get("email_id", contact.email_id)
                contact.email_source_note = updates.get("email_source_note", contact.email_source_note)
                contact.email_type = updates.get("email_type", contact.email_type)
                contact.contact_forms = updates.get("contact_forms", contact.contact_forms)
                contact.facebook_link = updates.get("facebook_link", contact.facebook_link)
                contact.publisher_details = updates.get("publisher_details", contact.publisher_details)
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

from __future__ import annotations

import asyncio
import os
import re
import secrets
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session

from ..db import get_db
from ..jobs.manager import job_manager
from ..jobs.tasks import run_contact_job, run_fast_scrape_job, run_goodreads_job, run_scrape_job
from ..models import Batch, Book, Job, JobEvent, OutreachMessage, SourceLink, StoredSchema
from ..schemas import (
    BatchCreate,
    BatchRead,
    BatchSummary,
    BenchmarkRequest,
    BenchmarkResponse,
    BookPatch,
    BookRead,
    BooksPage,
    EvaluationPatch,
    ExportRead,
    ExportRequest,
    GoodreadsCandidateAccept,
    JobCreateResponse,
    JobRead,
    OutreachDraftRequest,
    OutreachPatch,
    SchemaRead,
    SchemaUpdate,
    SheetSyncRequest,
    SourceLinkCreate,
    SourceLinkRead,
    TierMappingResponse,
)
from ..services.curation_service import (
    apply_benchmark,
    apply_tier_mapping_to_batch,
    batch_summary,
    build_outreach_draft,
    get_outreach_items,
    list_books,
    patch_book,
    patch_evaluation,
    patch_outreach,
)
from ..services.data_quality_service import batch_data_quality
from ..services.export_service import generate_export
from ..services.goodreads_service import candidate_updates_for_book
from ..services.mapping_service import apply_benchmark_mapping, apply_metric_mapping
from ..services.batch_service import DEFAULT_BATCH_NAME, DEFAULT_WORKSPACE_ID, ensure_working_batch
from ..services.reference_schema import reference_column_fields
from ..services.schema_service import create_schema
from ..services.sheet_sync_service import pull_from_sheet, push_to_sheet
from ..settings import DEFAULT_SHEET_URL, DEFAULT_WORKSHEET_NAME

router = APIRouter(prefix="/api", tags=["commissioning"])


def get_workspace_id(
    x_workspace_id: str | None = Header(default=None, alias="X-Workspace-Id"),
    workspace_id: str | None = Query(default=None),
) -> str:
    raw = (x_workspace_id or workspace_id or "").strip()
    if not raw:
        return DEFAULT_WORKSPACE_ID
    cleaned = re.sub(r"[^A-Za-z0-9_.:-]+", "-", raw)[:100].strip("-")
    return cleaned or DEFAULT_WORKSPACE_ID


def require_admin_access(
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    admin_token: str | None = Query(default=None),
) -> None:
    expected = os.getenv("COMMISSIONING_ADMIN_TOKEN", "").strip()
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="Admin diagnostics are disabled. Set COMMISSIONING_ADMIN_TOKEN to enable them.",
        )
    provided = (x_admin_token or admin_token or "").strip()
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=403, detail="Invalid admin token")


def _get_batch_or_404(db: Session, batch_id: int, workspace_id: str) -> Batch:
    batch = db.get(Batch, batch_id)
    if batch is None:
        try:
            batch = ensure_working_batch(db, workspace_id=workspace_id, batch_id=batch_id)
        except RuntimeError as exc:
            raise HTTPException(status_code=404, detail="Batch not found") from exc
    if batch.workspace_id != workspace_id:
        raise HTTPException(status_code=404, detail="Batch not found")
    return batch


def _get_book_or_404(db: Session, book_id: int, workspace_id: str) -> Book:
    book = db.get(Book, book_id)
    if book is None or book.batch.workspace_id != workspace_id:
        raise HTTPException(status_code=404, detail="Book not found")
    return book


def _get_job_or_404(db: Session, job_id: str, workspace_id: str) -> Job:
    job = db.get(Job, job_id)
    if job is None or job.batch.workspace_id != workspace_id:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


def _active_job_for_batch(db: Session, batch_id: int) -> Job | None:
    return (
        db.query(Job)
        .filter(Job.batch_id == batch_id, Job.status.in_(("queued", "running")))
        .order_by(Job.created_at.desc())
        .first()
    )


def _job_payload(job: Job, db: Session, *, event_limit: int = 3) -> dict:
    events = (
        db.query(JobEvent)
        .filter(JobEvent.job_id == job.id)
        .order_by(JobEvent.id.desc())
        .limit(max(0, event_limit))
        .all()
    )
    return {
        "id": job.id,
        "batch_id": job.batch_id,
        "workspace_id": job.batch.workspace_id if job.batch else "",
        "batch_name": job.batch.name if job.batch else "",
        "stage": job.stage,
        "status": job.status,
        "message": job.message,
        "error": job.error,
        "failure_bucket": job.failure_bucket,
        "progress_current": job.progress_current,
        "progress_total": job.progress_total,
        "progress_percent": job.progress_percent,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "recent_events": [
            {
                "id": event.id,
                "level": event.level,
                "message": event.message,
                "progress_percent": event.progress_percent,
                "payload": event.payload_json,
                "created_at": event.created_at,
            }
            for event in reversed(events)
        ],
    }


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


@router.get("/admin/jobs")
def admin_list_jobs(
    limit: int = Query(default=25, ge=1, le=100),
    event_limit: int = Query(default=3, ge=0, le=20),
    _: None = Depends(require_admin_access),
    db: Session = Depends(get_db),
) -> dict:
    jobs = db.query(Job).order_by(Job.created_at.desc()).limit(limit).all()
    return {"jobs": [_job_payload(job, db, event_limit=event_limit) for job in jobs]}


@router.get("/admin/jobs/{job_id}/events")
def admin_job_events(
    job_id: str,
    limit: int = Query(default=200, ge=1, le=1000),
    _: None = Depends(require_admin_access),
    db: Session = Depends(get_db),
) -> dict:
    job = db.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    events = (
        db.query(JobEvent)
        .filter(JobEvent.job_id == job_id)
        .order_by(JobEvent.id.desc())
        .limit(limit)
        .all()
    )
    return {
        "job": _job_payload(job, db, event_limit=0),
        "events": [
            {
                "id": event.id,
                "level": event.level,
                "message": event.message,
                "progress_percent": event.progress_percent,
                "payload": event.payload_json,
                "created_at": event.created_at,
            }
            for event in reversed(events)
        ],
    }


@router.get("/admin/batches")
def admin_list_batches(
    limit: int = Query(default=25, ge=1, le=100),
    _: None = Depends(require_admin_access),
    db: Session = Depends(get_db),
) -> dict:
    batches = db.query(Batch).order_by(Batch.updated_at.desc(), Batch.id.desc()).limit(limit).all()
    items = []
    for batch in batches:
        active_job = _active_job_for_batch(db, batch.id)
        recent_job = db.query(Job).filter(Job.batch_id == batch.id).order_by(Job.created_at.desc()).first()
        items.append(
            {
                "id": batch.id,
                "workspace_id": batch.workspace_id,
                "name": batch.name,
                "genre": batch.genre,
                "subgenre": batch.subgenre,
                "status": batch.status,
                "source_count": db.query(SourceLink).filter(SourceLink.batch_id == batch.id).count(),
                "book_count": db.query(Book).filter(Book.batch_id == batch.id).count(),
                "active_job": _job_payload(active_job, db, event_limit=1) if active_job else None,
                "recent_job": _job_payload(recent_job, db, event_limit=1) if recent_job else None,
                "created_at": batch.created_at,
                "updated_at": batch.updated_at,
            }
        )
    return {"batches": items}


@router.get("/admin/batches/{batch_id}/snapshot")
def admin_batch_snapshot(
    batch_id: int,
    _: None = Depends(require_admin_access),
    db: Session = Depends(get_db),
) -> dict:
    batch = db.get(Batch, batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="Batch not found")
    sources = (
        db.query(SourceLink)
        .filter(SourceLink.batch_id == batch.id)
        .order_by(SourceLink.id.asc())
        .all()
    )
    jobs = db.query(Job).filter(Job.batch_id == batch.id).order_by(Job.created_at.desc()).limit(10).all()
    books = db.query(Book).filter(Book.batch_id == batch.id).order_by(Book.id.asc()).limit(10).all()
    return {
        "batch": BatchRead.model_validate(batch),
        "summary": batch_summary(db, batch),
        "sources": [
            {
                "id": source.id,
                "source_type": source.source_type,
                "status": source.status,
                "max_results": source.max_results,
                "url": source.url,
                "created_at": source.created_at,
                "updated_at": source.updated_at,
            }
            for source in sources
        ],
        "jobs": [_job_payload(job, db, event_limit=5) for job in jobs],
        "book_sample": [
            {
                "id": book.id,
                "title": book.title,
                "author": book.author,
                "amazon_url": book.amazon_url,
                "rating": book.rating,
                "rating_count": book.rating_count,
                "publisher": book.publisher,
                "publication_date": book.publication_date,
                "goodreads_rating": book.goodreads_rating,
                "goodreads_rating_count": book.goodreads_rating_count,
                "amazon_flags": ((book.provenance_json or {}).get("amazon", {}) or {}).get("amazon_quality_flags", []),
            }
            for book in books
        ],
    }


@router.post("/bootstrap")
def bootstrap(workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)) -> dict:
    batch = (
        db.query(Batch)
        .filter(Batch.workspace_id == workspace_id)
        .order_by(Batch.updated_at.desc(), Batch.id.desc())
        .first()
    )
    if batch is None:
        batch = ensure_working_batch(db, workspace_id=workspace_id)
    active_job = _active_job_for_batch(db, batch.id)
    runs = db.query(Batch).filter(Batch.workspace_id == workspace_id).order_by(Batch.updated_at.desc(), Batch.id.desc()).all()
    return {
        "batch": BatchRead.model_validate(batch),
        "runs": [BatchRead.model_validate(item) for item in runs],
        "summary": batch_summary(db, batch),
        "active_job": JobRead.model_validate(active_job) if active_job else None,
    }


@router.get("/reference-schema")
def reference_schema() -> dict:
    return {"fields": reference_column_fields()}


@router.post("/schemas/upload", response_model=SchemaRead)
async def upload_schema(
    source_type: str = Form(...),
    batch_id: int | None = Form(default=None),
    name: str = Form(default=""),
    file: UploadFile = File(...),
    workspace_id: str = Depends(get_workspace_id),
    db: Session = Depends(get_db),
):
    actual_batch_id = batch_id
    if batch_id is not None:
        actual_batch_id = _get_batch_or_404(db, batch_id, workspace_id).id
    content = (await file.read()).decode("utf-8")
    schema = create_schema(db, source_type=source_type, file_name=file.filename or "schema.csv", content=content, batch_id=actual_batch_id, name=name)
    return schema


@router.get("/schemas/{schema_id}", response_model=SchemaRead)
def get_schema(schema_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    schema = db.get(StoredSchema, schema_id)
    if schema is None:
        raise HTTPException(status_code=404, detail="Schema not found")
    if schema.batch_id is not None:
        _get_batch_or_404(db, schema.batch_id, workspace_id)
    return schema


@router.patch("/schemas/{schema_id}", response_model=SchemaRead)
def update_schema(schema_id: int, payload: SchemaUpdate, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    schema = db.get(StoredSchema, schema_id)
    if schema is None:
        raise HTTPException(status_code=404, detail="Schema not found")
    if schema.batch_id is not None:
        _get_batch_or_404(db, schema.batch_id, workspace_id)
    if payload.name is not None:
        schema.name = payload.name
    if "selected_fields" in payload.model_fields_set:
        schema.selected_fields_json = payload.selected_fields
    db.commit()
    db.refresh(schema)
    return schema


@router.post("/batches", response_model=BatchRead)
def create_batch(payload: BatchCreate, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = Batch(**payload.model_dump(), workspace_id=workspace_id, status="active")
    db.add(batch)
    db.commit()
    db.refresh(batch)
    return batch


@router.get("/batches", response_model=list[BatchRead])
def list_batches(workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    return db.query(Batch).filter(Batch.workspace_id == workspace_id).order_by(Batch.updated_at.desc(), Batch.id.desc()).all()


@router.get("/batches/{batch_id}", response_model=BatchRead)
def get_batch(batch_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    return _get_batch_or_404(db, batch_id, workspace_id)


@router.get("/batches/{batch_id}/summary", response_model=BatchSummary)
def get_batch_summary(batch_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    return batch_summary(db, batch)


@router.get("/batches/{batch_id}/data-quality")
def get_data_quality(batch_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    return batch_data_quality(db, batch.id)


@router.post("/batches/{batch_id}/sources", response_model=list[SourceLinkRead])
def add_sources(batch_id: int, payload: list[SourceLinkCreate], workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    items = []
    for entry in payload:
        source = SourceLink(
            batch_id=batch.id,
            source_type=entry.source_type.lower(),
            url=entry.url,
            max_results=entry.max_results,
            output_format=entry.output_format,
            metadata_json=entry.metadata,
        )
        db.add(source)
        items.append(source)
    db.commit()
    for item in items:
        db.refresh(item)
    return items


@router.put("/batches/{batch_id}/sources", response_model=list[SourceLinkRead])
def replace_sources(batch_id: int, payload: list[SourceLinkCreate], workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    db.query(SourceLink).filter(SourceLink.batch_id == batch.id).delete(synchronize_session=False)
    db.commit()
    return add_sources(batch.id, payload, workspace_id, db)


@router.get("/batches/{batch_id}/sources", response_model=list[SourceLinkRead])
def get_sources(batch_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    return db.query(SourceLink).filter(SourceLink.batch_id == batch.id).order_by(SourceLink.id.asc()).all()


def _queue_job(db: Session, *, batch_id: int, stage: str, task) -> Job:
    active_job = _active_job_for_batch(db, batch_id)
    if active_job:
        return active_job
    job = Job(batch_id=batch_id, stage=stage, status="queued", message=f"{stage} queued")
    db.add(job)
    db.commit()
    db.refresh(job)
    job_manager.submit(job.id, task, job.id, batch_id)
    return job


@router.post("/batches/{batch_id}/jobs/scrape", response_model=JobCreateResponse)
def queue_scrape_job(batch_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    return {"job": _queue_job(db, batch_id=batch.id, stage="scrape", task=run_scrape_job)}


@router.post("/batches/{batch_id}/jobs/scrape-fast", response_model=JobCreateResponse)
def queue_fast_scrape_job(batch_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    return {"job": _queue_job(db, batch_id=batch.id, stage="fast_scrape", task=run_fast_scrape_job)}


@router.post("/batches/{batch_id}/jobs/enrich-goodreads", response_model=JobCreateResponse)
def queue_goodreads_job(batch_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    return {"job": _queue_job(db, batch_id=batch.id, stage="enrich_goodreads", task=run_goodreads_job)}


@router.post("/batches/{batch_id}/jobs/enrich-contacts", response_model=JobCreateResponse)
def queue_contact_job(batch_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    return {"job": _queue_job(db, batch_id=batch.id, stage="enrich_contacts", task=run_contact_job)}


@router.get("/jobs/{job_id}", response_model=JobRead)
def get_job(job_id: str, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    return _get_job_or_404(db, job_id, workspace_id)


@router.post("/jobs/{job_id}/cancel", response_model=JobRead)
def cancel_job(job_id: str, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    job = _get_job_or_404(db, job_id, workspace_id)
    if job.status in {"queued", "running"}:
        job.status = "failed"
        job.error = "Job cancelled by user."
        job.message = "Job cancelled."
        db.add(
            JobEvent(
                job_id=job.id,
                level="warning",
                message=job.message,
                payload_json={"cancelled": True},
                progress_percent=job.progress_percent,
            )
        )
        db.commit()
        db.refresh(job)
    return job


@router.get("/jobs/{job_id}/events")
async def stream_job_events(job_id: str, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    _get_job_or_404(db, job_id, workspace_id)

    async def event_stream():
        last_event_id = 0
        while True:
            events = (
                db.query(JobEvent)
                .filter(JobEvent.job_id == job_id, JobEvent.id > last_event_id)
                .order_by(JobEvent.id.asc())
                .all()
            )
            for event in events:
                last_event_id = event.id
                yield (
                    f"id: {event.id}\n"
                    f"event: {event.level}\n"
                    f"data: {event.message} | {event.progress_percent}\n\n"
                )
            job = db.get(Job, job_id)
            if job and job.status in {"completed", "failed"} and not events:
                yield f"event: done\ndata: {job.status}\n\n"
                break
            await asyncio.sleep(1)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.get("/batches/{batch_id}/books", response_model=BooksPage)
def get_books(
    batch_id: int,
    page: int = 1,
    page_size: int = 25,
    search: str = "",
    genre: str = "",
    source_type: str = "",
    shortlisted: bool | None = None,
    workspace_id: str = Depends(get_workspace_id),
    db: Session = Depends(get_db),
):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    total, items = list_books(
        db,
        batch_id=batch.id,
        page=page,
        page_size=page_size,
        search=search,
        genre=genre,
        source_type=source_type,
        shortlisted=shortlisted,
    )
    return {"total": total, "items": [BookRead.model_validate(item) for item in items]}


@router.patch("/books/{book_id}", response_model=BookRead)
def update_book(book_id: int, payload: BookPatch, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    book = _get_book_or_404(db, book_id, workspace_id)
    updated = patch_book(db, book, payload.model_dump())
    return BookRead.model_validate(updated)


@router.post("/batches/{batch_id}/benchmark/apply", response_model=BenchmarkResponse)
def benchmark_batch(batch_id: int, payload: BenchmarkRequest, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    matched_ids = apply_benchmark(db, batch.id, payload.model_dump())
    return {"total": len(matched_ids), "matched_ids": matched_ids}


@router.post("/batches/{batch_id}/tier-mapping/apply", response_model=TierMappingResponse)
def apply_tier_mapping_batch(batch_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    return apply_tier_mapping_to_batch(db, batch.id)


@router.get("/batches/{batch_id}/outreach", response_model=list[BookRead])
def get_outreach(batch_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    items = get_outreach_items(db, batch.id)
    return [BookRead.model_validate(item) for item in items]


@router.patch("/books/{book_id}/outreach", response_model=BookRead)
def update_outreach(book_id: int, payload: OutreachPatch, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    book = _get_book_or_404(db, book_id, workspace_id)
    message = book.outreach_messages[0] if book.outreach_messages else None
    patch_outreach(db, message, book, payload.model_dump())
    db.refresh(book)
    return BookRead.model_validate(book)


@router.patch("/books/{book_id}/evaluation", response_model=BookRead)
def update_evaluation(book_id: int, payload: EvaluationPatch, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    book = _get_book_or_404(db, book_id, workspace_id)
    patch_evaluation(db, book, payload.model_dump())
    db.refresh(book)
    return BookRead.model_validate(book)


@router.post("/books/{book_id}/outreach/draft", response_model=BookRead)
def create_outreach_draft(book_id: int, payload: OutreachDraftRequest, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    book = _get_book_or_404(db, book_id, workspace_id)
    build_outreach_draft(db, book, payload.template, payload.sender_name, payload.sender_email)
    db.refresh(book)
    return BookRead.model_validate(book)


@router.post("/books/{book_id}/goodreads/accept", response_model=BookRead)
def accept_goodreads_candidate(book_id: int, payload: GoodreadsCandidateAccept, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    book = _get_book_or_404(db, book_id, workspace_id)
    updates = candidate_updates_for_book(book, payload.model_dump())
    book.goodread_link = updates.get("Resolved Goodreads Book") or updates.get("Series Book 1") or book.goodread_link
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
    provenance = dict(book.provenance_json or {})
    provenance["goodreads"] = updates
    book.provenance_json = provenance
    apply_metric_mapping(book)
    apply_benchmark_mapping(book)
    db.commit()
    db.refresh(book)
    return BookRead.model_validate(book)


@router.post("/batches/{batch_id}/exports", response_model=ExportRead)
def create_export(batch_id: int, payload: ExportRequest, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    try:
        export = generate_export(
            db,
            batch,
            payload.export_format,
            profile=payload.profile,
            require_ready=payload.require_ready,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return export


@router.get("/exports/{export_id}/download")
def download_export(export_id: int, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    from ..models import ExportRecord

    export = db.get(ExportRecord, export_id)
    if export is None or export.batch.workspace_id != workspace_id:
        raise HTTPException(status_code=404, detail="Export not found")
    path = Path(export.file_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Export file missing")
    return FileResponse(path, filename=path.name)


@router.post("/batches/{batch_id}/sync/google-sheet")
def sync_google_sheet(batch_id: int, payload: SheetSyncRequest, workspace_id: str = Depends(get_workspace_id), db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id, workspace_id)
    sheet_url = payload.sheet_url or batch.source_sheet_url or DEFAULT_SHEET_URL
    worksheet_name = payload.worksheet_name or batch.source_sheet_worksheet or DEFAULT_WORKSHEET_NAME
    try:
        if payload.mode == "pull-from-sheet":
            return pull_from_sheet(db, batch, sheet_url, worksheet_name)
        if payload.mode == "push-selected-fields":
            return push_to_sheet(
                db,
                batch,
                sheet_url=sheet_url,
                worksheet_name=worksheet_name,
                families=payload.families,
                overwrite=payload.overwrite,
            )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    raise HTTPException(status_code=400, detail="Unsupported sync mode")

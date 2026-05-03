from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session

from ..db import get_db
from ..jobs.manager import job_manager
from ..jobs.tasks import run_contact_job, run_goodreads_job, run_scrape_job
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
    JobCreateResponse,
    JobRead,
    OutreachDraftRequest,
    OutreachPatch,
    SchemaRead,
    SchemaUpdate,
    SheetSyncRequest,
    SourceLinkCreate,
    SourceLinkRead,
)
from ..services.curation_service import (
    apply_benchmark,
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
from ..services.batch_service import DEFAULT_BATCH_NAME, ensure_working_batch
from ..services.reference_schema import reference_column_fields
from ..services.schema_service import create_schema
from ..services.sheet_sync_service import pull_from_sheet, push_to_sheet
from ..settings import DEFAULT_SHEET_URL, DEFAULT_WORKSHEET_NAME

router = APIRouter(prefix="/api", tags=["commissioning"])


def _get_batch_or_404(db: Session, batch_id: int) -> Batch:
    batch = db.get(Batch, batch_id)
    if batch is None:
        batch = ensure_working_batch(db, batch_id=batch_id)
    return batch


def _get_book_or_404(db: Session, book_id: int) -> Book:
    book = db.get(Book, book_id)
    if book is None:
        raise HTTPException(status_code=404, detail="Book not found")
    return book


def _get_job_or_404(db: Session, job_id: str) -> Job:
    job = db.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


def _active_job_for_batch(db: Session, batch_id: int) -> Job | None:
    return (
        db.query(Job)
        .filter(Job.batch_id == batch_id, Job.status.in_(("queued", "running")))
        .order_by(Job.created_at.desc())
        .first()
    )


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


@router.post("/bootstrap")
def bootstrap(db: Session = Depends(get_db)) -> dict:
    batch = db.query(Batch).filter(Batch.name == DEFAULT_BATCH_NAME).order_by(Batch.id.asc()).first()
    if batch is None:
        batch = ensure_working_batch(db)
    active_job = _active_job_for_batch(db, batch.id)
    return {
        "batch": BatchRead.model_validate(batch),
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
    db: Session = Depends(get_db),
):
    content = (await file.read()).decode("utf-8")
    schema = create_schema(db, source_type=source_type, file_name=file.filename or "schema.csv", content=content, batch_id=batch_id, name=name)
    return schema


@router.get("/schemas/{schema_id}", response_model=SchemaRead)
def get_schema(schema_id: int, db: Session = Depends(get_db)):
    schema = db.get(StoredSchema, schema_id)
    if schema is None:
        raise HTTPException(status_code=404, detail="Schema not found")
    return schema


@router.patch("/schemas/{schema_id}", response_model=SchemaRead)
def update_schema(schema_id: int, payload: SchemaUpdate, db: Session = Depends(get_db)):
    schema = db.get(StoredSchema, schema_id)
    if schema is None:
        raise HTTPException(status_code=404, detail="Schema not found")
    if payload.name is not None:
        schema.name = payload.name
    if "selected_fields" in payload.model_fields_set:
        schema.selected_fields_json = payload.selected_fields
    db.commit()
    db.refresh(schema)
    return schema


@router.post("/batches", response_model=BatchRead)
def create_batch(payload: BatchCreate, db: Session = Depends(get_db)):
    batch = Batch(**payload.model_dump(), status="draft")
    db.add(batch)
    db.commit()
    db.refresh(batch)
    return batch


@router.get("/batches", response_model=list[BatchRead])
def list_batches(db: Session = Depends(get_db)):
    return db.query(Batch).order_by(Batch.updated_at.desc(), Batch.id.desc()).all()


@router.get("/batches/{batch_id}", response_model=BatchRead)
def get_batch(batch_id: int, db: Session = Depends(get_db)):
    return _get_batch_or_404(db, batch_id)


@router.get("/batches/{batch_id}/summary", response_model=BatchSummary)
def get_batch_summary(batch_id: int, db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id)
    return batch_summary(db, batch)


@router.get("/batches/{batch_id}/data-quality")
def get_data_quality(batch_id: int, db: Session = Depends(get_db)):
    _get_batch_or_404(db, batch_id)
    return batch_data_quality(db, batch_id)


@router.post("/batches/{batch_id}/sources", response_model=list[SourceLinkRead])
def add_sources(batch_id: int, payload: list[SourceLinkCreate], db: Session = Depends(get_db)):
    _get_batch_or_404(db, batch_id)
    items = []
    for entry in payload:
        source = SourceLink(
            batch_id=batch_id,
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
def replace_sources(batch_id: int, payload: list[SourceLinkCreate], db: Session = Depends(get_db)):
    _get_batch_or_404(db, batch_id)
    db.query(SourceLink).filter(SourceLink.batch_id == batch_id).delete(synchronize_session=False)
    db.commit()
    return add_sources(batch_id, payload, db)


@router.get("/batches/{batch_id}/sources", response_model=list[SourceLinkRead])
def get_sources(batch_id: int, db: Session = Depends(get_db)):
    _get_batch_or_404(db, batch_id)
    return db.query(SourceLink).filter(SourceLink.batch_id == batch_id).order_by(SourceLink.id.asc()).all()


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
def queue_scrape_job(batch_id: int, db: Session = Depends(get_db)):
    _get_batch_or_404(db, batch_id)
    return {"job": _queue_job(db, batch_id=batch_id, stage="scrape", task=run_scrape_job)}


@router.post("/batches/{batch_id}/jobs/enrich-goodreads", response_model=JobCreateResponse)
def queue_goodreads_job(batch_id: int, db: Session = Depends(get_db)):
    _get_batch_or_404(db, batch_id)
    return {"job": _queue_job(db, batch_id=batch_id, stage="enrich_goodreads", task=run_goodreads_job)}


@router.post("/batches/{batch_id}/jobs/enrich-contacts", response_model=JobCreateResponse)
def queue_contact_job(batch_id: int, db: Session = Depends(get_db)):
    _get_batch_or_404(db, batch_id)
    return {"job": _queue_job(db, batch_id=batch_id, stage="enrich_contacts", task=run_contact_job)}


@router.get("/jobs/{job_id}", response_model=JobRead)
def get_job(job_id: str, db: Session = Depends(get_db)):
    return _get_job_or_404(db, job_id)


@router.get("/jobs/{job_id}/events")
async def stream_job_events(job_id: str, db: Session = Depends(get_db)):
    _get_job_or_404(db, job_id)

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
    db: Session = Depends(get_db),
):
    _get_batch_or_404(db, batch_id)
    total, items = list_books(
        db,
        batch_id=batch_id,
        page=page,
        page_size=page_size,
        search=search,
        genre=genre,
        source_type=source_type,
        shortlisted=shortlisted,
    )
    return {"total": total, "items": [BookRead.model_validate(item) for item in items]}


@router.patch("/books/{book_id}", response_model=BookRead)
def update_book(book_id: int, payload: BookPatch, db: Session = Depends(get_db)):
    book = _get_book_or_404(db, book_id)
    updated = patch_book(db, book, payload.model_dump())
    return BookRead.model_validate(updated)


@router.post("/batches/{batch_id}/benchmark/apply", response_model=BenchmarkResponse)
def benchmark_batch(batch_id: int, payload: BenchmarkRequest, db: Session = Depends(get_db)):
    _get_batch_or_404(db, batch_id)
    matched_ids = apply_benchmark(db, batch_id, payload.model_dump())
    return {"total": len(matched_ids), "matched_ids": matched_ids}


@router.get("/batches/{batch_id}/outreach", response_model=list[BookRead])
def get_outreach(batch_id: int, db: Session = Depends(get_db)):
    _get_batch_or_404(db, batch_id)
    items = get_outreach_items(db, batch_id)
    return [BookRead.model_validate(item) for item in items]


@router.patch("/books/{book_id}/outreach", response_model=BookRead)
def update_outreach(book_id: int, payload: OutreachPatch, db: Session = Depends(get_db)):
    book = _get_book_or_404(db, book_id)
    message = book.outreach_messages[0] if book.outreach_messages else None
    patch_outreach(db, message, book, payload.model_dump())
    db.refresh(book)
    return BookRead.model_validate(book)


@router.patch("/books/{book_id}/evaluation", response_model=BookRead)
def update_evaluation(book_id: int, payload: EvaluationPatch, db: Session = Depends(get_db)):
    book = _get_book_or_404(db, book_id)
    patch_evaluation(db, book, payload.model_dump())
    db.refresh(book)
    return BookRead.model_validate(book)


@router.post("/books/{book_id}/outreach/draft", response_model=BookRead)
def create_outreach_draft(book_id: int, payload: OutreachDraftRequest, db: Session = Depends(get_db)):
    book = _get_book_or_404(db, book_id)
    build_outreach_draft(db, book, payload.template, payload.sender_name, payload.sender_email)
    db.refresh(book)
    return BookRead.model_validate(book)


@router.post("/batches/{batch_id}/exports", response_model=ExportRead)
def create_export(batch_id: int, payload: ExportRequest, db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id)
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
def download_export(export_id: int, db: Session = Depends(get_db)):
    from ..models import ExportRecord

    export = db.get(ExportRecord, export_id)
    if export is None:
        raise HTTPException(status_code=404, detail="Export not found")
    path = Path(export.file_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Export file missing")
    return FileResponse(path, filename=path.name)


@router.post("/batches/{batch_id}/sync/google-sheet")
def sync_google_sheet(batch_id: int, payload: SheetSyncRequest, db: Session = Depends(get_db)):
    batch = _get_batch_or_404(db, batch_id)
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

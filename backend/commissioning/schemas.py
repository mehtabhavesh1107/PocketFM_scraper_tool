from __future__ import annotations

import html as html_lib
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _is_amazon_host(host: str) -> bool:
    normalized = (host or "").split(":", 1)[0].lower()
    return (
        normalized.startswith("amazon.")
        or ".amazon." in normalized
        or normalized == "amzn.com"
        or normalized.endswith(".amzn.com")
    )


class FieldDefinition(BaseModel):
    name: str
    label: str
    type: str = "string"
    required: bool = False
    on: bool = True


class BatchCreate(BaseModel):
    name: str
    genre: str = ""
    subgenre: str = ""
    description: str = ""
    source_sheet_url: str = ""
    source_sheet_worksheet: str = ""


class BatchRead(BatchCreate):
    model_config = ConfigDict(from_attributes=True)

    id: int
    workspace_id: str = "public"
    status: str
    created_at: datetime
    updated_at: datetime


class BatchSummary(BaseModel):
    batch_id: int
    name: str
    total_sources: int
    total_books: int
    shortlisted_books: int
    outreach_ready: int
    emails_found: int
    job_counts: dict[str, int]


class SchemaRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    batch_id: int | None
    source_type: str
    name: str
    file_name: str
    file_format: str
    fields_json: list[dict[str, Any]]
    selected_fields_json: list[str]
    created_at: datetime


class SchemaUpdate(BaseModel):
    name: str | None = None
    selected_fields: list[str] = Field(default_factory=list)


class SourceLinkCreate(BaseModel):
    source_type: str
    url: str
    # `max_results` is kept for storage compatibility but is treated as a soft safety cap;
    # 0 (or unset) means "scrape every page the source exposes" up to a hard ceiling.
    max_results: int = 0
    output_format: str = "CSV"
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("source_type")
    @classmethod
    def _validate_source_type(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"amazon", "goodreads"}:
            raise ValueError("source_type must be 'amazon' or 'goodreads'")
        return normalized

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        cleaned = re.sub(r"\s+", "", html_lib.unescape(value or "").strip())
        if not cleaned:
            raise ValueError("url is required")
        parsed = urlparse(cleaned)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("url must start with http:// or https://")
        if not parsed.netloc:
            raise ValueError("url is missing a host")
        return cleaned

    @field_validator("max_results")
    @classmethod
    def _validate_max_results(cls, value: int) -> int:
        if value < 0:
            raise ValueError("max_results cannot be negative")
        # Hard ceiling so a runaway scrape can't pull tens of thousands at once.
        if value > 5000:
            raise ValueError("max_results capped at 5000 per source")
        return value

    @field_validator("output_format")
    @classmethod
    def _validate_output_format(cls, value: str) -> str:
        normalized = (value or "CSV").strip().upper()
        if normalized not in {"CSV", "XLSX", "JSON", "PDF"}:
            raise ValueError("output_format must be one of CSV, XLSX, JSON, PDF")
        return normalized

    @field_validator("url")
    @classmethod
    def _check_host_matches_source(cls, value: str, info) -> str:  # type: ignore[override]
        # Runs after `_validate_url` (Pydantic chains validators on the same field).
        # Cross-checks that the host matches the declared source_type if available.
        source_type = (info.data or {}).get("source_type") if info else None
        if not source_type:
            return value
        host = urlparse(value).netloc.lower()
        if source_type == "amazon" and not _is_amazon_host(host):
            raise ValueError("Amazon source must use an amazon.* or amzn.com URL")
        if source_type == "goodreads" and "goodreads.com" not in host:
            raise ValueError("Goodreads source must use a goodreads.com URL")
        return value


class SourceLinkRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    batch_id: int
    source_type: str
    url: str
    max_results: int
    output_format: str
    status: str
    metadata_json: dict[str, Any]


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    batch_id: int
    stage: str
    status: str
    message: str
    progress_current: int
    progress_total: int
    progress_percent: float
    checkpoint_json: dict[str, Any]
    payload_json: dict[str, Any]
    failure_bucket: str
    error: str
    started_at: datetime | None
    finished_at: datetime | None
    created_at: datetime


class JobCreateResponse(BaseModel):
    job: JobRead


class ContactRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    email_id: str = ""
    email_source_note: str = ""
    email_type: str = ""
    contact_forms: str = ""
    facebook_link: str = ""
    publisher_details: str = ""
    website: str = ""
    author_email: str = ""
    agent_email: str = ""


class EvaluationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    story_score: int | None = None
    characters_score: int | None = None
    hooks_score: int | None = None
    series_potential_score: int | None = None
    audio_adaptability_score: int | None = None
    india_fit_score: int | None = None
    notes: str = ""


class EvaluationPatch(BaseModel):
    story_score: int | None = None
    characters_score: int | None = None
    hooks_score: int | None = None
    series_potential_score: int | None = None
    audio_adaptability_score: int | None = None
    india_fit_score: int | None = None
    notes: str | None = None


class OutreachRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    recipient: str = ""
    cc: str = ""
    subject: str = ""
    body: str = ""
    template: str = ""
    status: str = ""
    sent_at: datetime | None = None


class BookRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    batch_id: int
    title: str
    author: str = ""
    url: str = ""
    amazon_url: str = ""
    rating: float | None = None
    rating_count: int | None = None
    publisher: str = ""
    publication_date: str = ""
    part_of_series: str = ""
    language: str = ""
    best_sellers_rank: str = ""
    print_length: str = ""
    book_number: str = ""
    format: str = ""
    synopsis: str = ""
    genre: str = ""
    sub_genre: str = ""
    cleaned_series_name: str = ""
    series_flag: str = ""
    duplicates_basis_series: str = ""
    author_check: str = ""
    clean_author_names: str = ""
    total_pages_in_series: str = ""
    total_word_count: str = ""
    total_hours: str = ""
    goodread_link: str = ""
    series_book_1: str = ""
    series_link: str = ""
    remarks: str = ""
    primary_book_count: str = ""
    gr_book_1_rating: str = ""
    gr_book_2_rating: str = ""
    gr_book_3_rating: str = ""
    gr_book_4_rating: str = ""
    gr_book_5_rating: str = ""
    gr_book_6_rating: str = ""
    gr_book_7_rating: str = ""
    gr_book_8_rating: str = ""
    gr_book_9_rating: str = ""
    gr_book_10_rating: str = ""
    final_list: str = ""
    rationale: str = ""
    goodreads_rating: str = ""
    goodreads_rating_count: str = ""
    word_count: int | None = None
    audio_score: int | None = None
    book_type: str = ""
    benchmark_match: bool = False
    shortlisted: bool = False
    provenance_json: dict[str, Any] = Field(default_factory=dict)
    contact: ContactRead | None = None
    evaluation: EvaluationRead | None = None
    outreach_messages: list[OutreachRead] = Field(default_factory=list)


class BookPatch(BaseModel):
    genre: str | None = None
    sub_genre: str | None = None
    synopsis: str | None = None
    remarks: str | None = None
    final_list: str | None = None
    rationale: str | None = None
    series_flag: str | None = None
    cleaned_series_name: str | None = None
    duplicates_basis_series: str | None = None
    author_check: str | None = None
    clean_author_names: str | None = None
    word_count: int | None = None
    audio_score: int | None = None
    book_type: str | None = None
    shortlisted: bool | None = None
    benchmark_match: bool | None = None


class BooksPage(BaseModel):
    total: int
    items: list[BookRead]


class BenchmarkRequest(BaseModel):
    min_rating: float = 0
    min_reviews: int = 0
    min_word_count: int = 0
    max_series_books: int = 999999
    min_audio_score: int = 0
    genres: list[str] = Field(default_factory=list)
    types: list[str] = Field(default_factory=list)


class BenchmarkResponse(BaseModel):
    total: int
    matched_ids: list[int]


class OutreachPatch(BaseModel):
    recipient: str | None = None
    cc: str | None = None
    subject: str | None = None
    body: str | None = None
    template: str | None = None
    status: str | None = None


class OutreachDraftRequest(BaseModel):
    template: str = "formal"
    sender_name: str = "Astha Singh"
    sender_email: str = "astha.singh@pocketfm.com"


class GoodreadsCandidateAccept(BaseModel):
    url: str
    title: str = ""
    author: str = ""
    series_name: str = ""
    series_url: str = ""
    rating: str = ""
    rating_count: str = ""
    pages: str = ""
    published_year: str = ""
    publication: str = ""
    publisher: str = ""
    isbn_10: str = ""
    isbn_13: str = ""
    score: float = 1
    search_url: str = ""


class ExportRequest(BaseModel):
    export_format: str = Field(default="csv")
    profile: str = Field(default="sample_compatible")
    require_ready: bool = False


class ExportRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    batch_id: int
    export_format: str
    file_path: str
    status: str
    row_count: int
    metadata_json: dict[str, Any]
    created_at: datetime


class SheetSyncRequest(BaseModel):
    mode: str = "push-selected-fields"
    sheet_url: str = ""
    worksheet_name: str = ""
    families: list[str] = Field(default_factory=lambda: ["goodreads", "contact", "curation"])
    overwrite: bool = False

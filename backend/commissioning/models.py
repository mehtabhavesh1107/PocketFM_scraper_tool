from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from .db import Base


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class Batch(TimestampMixin, Base):
    __tablename__ = "batches"

    id: Mapped[int] = mapped_column(primary_key=True)
    workspace_id: Mapped[str] = mapped_column(String(100), default="public", index=True)
    name: Mapped[str] = mapped_column(String(200))
    genre: Mapped[str] = mapped_column(String(200), default="")
    subgenre: Mapped[str] = mapped_column(String(200), default="")
    description: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(50), default="draft")
    source_sheet_url: Mapped[str] = mapped_column(Text, default="")
    source_sheet_worksheet: Mapped[str] = mapped_column(String(200), default="")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)

    schemas: Mapped[list["StoredSchema"]] = relationship(back_populates="batch", cascade="all, delete-orphan")
    source_links: Mapped[list["SourceLink"]] = relationship(back_populates="batch", cascade="all, delete-orphan")
    jobs: Mapped[list["Job"]] = relationship(back_populates="batch", cascade="all, delete-orphan")
    books: Mapped[list["Book"]] = relationship(back_populates="batch", cascade="all, delete-orphan")
    exports: Mapped[list["ExportRecord"]] = relationship(back_populates="batch", cascade="all, delete-orphan")


class StoredSchema(TimestampMixin, Base):
    __tablename__ = "schemas"

    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[int | None] = mapped_column(ForeignKey("batches.id"))
    source_type: Mapped[str] = mapped_column(String(50))
    name: Mapped[str] = mapped_column(String(200), default="")
    file_name: Mapped[str] = mapped_column(String(255), default="")
    file_format: Mapped[str] = mapped_column(String(50), default="")
    fields_json: Mapped[list] = mapped_column(JSON, default=list)
    selected_fields_json: Mapped[list] = mapped_column(JSON, default=list)
    raw_content: Mapped[str] = mapped_column(Text, default="")

    batch: Mapped[Batch | None] = relationship(back_populates="schemas")


class SourceLink(TimestampMixin, Base):
    __tablename__ = "source_links"

    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("batches.id"))
    source_type: Mapped[str] = mapped_column(String(50))
    url: Mapped[str] = mapped_column(Text)
    max_results: Mapped[int] = mapped_column(Integer, default=0)
    output_format: Mapped[str] = mapped_column(String(50), default="CSV")
    status: Mapped[str] = mapped_column(String(50), default="pending")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)

    batch: Mapped[Batch] = relationship(back_populates="source_links")


class Job(TimestampMixin, Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    batch_id: Mapped[int] = mapped_column(ForeignKey("batches.id"))
    stage: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(50), default="queued")
    message: Mapped[str] = mapped_column(Text, default="")
    progress_current: Mapped[int] = mapped_column(Integer, default=0)
    progress_total: Mapped[int] = mapped_column(Integer, default=0)
    progress_percent: Mapped[float] = mapped_column(Float, default=0.0)
    checkpoint_json: Mapped[dict] = mapped_column(JSON, default=dict)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    failure_bucket: Mapped[str] = mapped_column(String(100), default="")
    error: Mapped[str] = mapped_column(Text, default="")
    started_at: Mapped[datetime | None] = mapped_column(DateTime)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)

    batch: Mapped[Batch] = relationship(back_populates="jobs")
    events: Mapped[list["JobEvent"]] = relationship(back_populates="job", cascade="all, delete-orphan")


class JobEvent(Base):
    __tablename__ = "job_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"))
    level: Mapped[str] = mapped_column(String(20), default="info")
    message: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    progress_percent: Mapped[float] = mapped_column(Float, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    job: Mapped[Job] = relationship(back_populates="events")


class Book(TimestampMixin, Base):
    __tablename__ = "books"

    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("batches.id"))
    title: Mapped[str] = mapped_column(String(500))
    author: Mapped[str] = mapped_column(String(300), default="")
    url: Mapped[str] = mapped_column(Text, default="")
    amazon_url: Mapped[str] = mapped_column(Text, default="")
    rating: Mapped[float | None] = mapped_column(Float)
    rating_count: Mapped[int | None] = mapped_column(Integer)
    publisher: Mapped[str] = mapped_column(String(255), default="")
    publication_date: Mapped[str] = mapped_column(String(255), default="")
    part_of_series: Mapped[str] = mapped_column(String(255), default="")
    language: Mapped[str] = mapped_column(String(100), default="")
    best_sellers_rank: Mapped[str] = mapped_column(String(255), default="")
    print_length: Mapped[str] = mapped_column(String(100), default="")
    book_number: Mapped[str] = mapped_column(String(50), default="")
    format: Mapped[str] = mapped_column(String(100), default="")
    synopsis: Mapped[str] = mapped_column(Text, default="")
    genre: Mapped[str] = mapped_column(String(200), default="")
    sub_genre: Mapped[str] = mapped_column(String(200), default="")
    cleaned_series_name: Mapped[str] = mapped_column(String(255), default="")
    series_flag: Mapped[str] = mapped_column(String(50), default="")
    duplicates_basis_series: Mapped[str] = mapped_column(String(100), default="")
    author_check: Mapped[str] = mapped_column(String(100), default="")
    clean_author_names: Mapped[str] = mapped_column(String(255), default="")
    total_pages_in_series: Mapped[str] = mapped_column(String(100), default="")
    total_word_count: Mapped[str] = mapped_column(String(100), default="")
    total_hours: Mapped[str] = mapped_column(String(100), default="")
    tier: Mapped[str] = mapped_column(String(50), default="")
    gr_ratings: Mapped[str] = mapped_column(String(100), default="")
    trope: Mapped[str] = mapped_column(String(100), default="")
    length: Mapped[str] = mapped_column(String(100), default="")
    mg_min: Mapped[str] = mapped_column(String(50), default="")
    mg_max: Mapped[str] = mapped_column(String(50), default="")
    rev_share_min: Mapped[str] = mapped_column(String(50), default="")
    rev_share_max: Mapped[str] = mapped_column(String(50), default="")
    goodread_link: Mapped[str] = mapped_column(Text, default="")
    series_book_1: Mapped[str] = mapped_column(Text, default="")
    series_link: Mapped[str] = mapped_column(Text, default="")
    remarks: Mapped[str] = mapped_column(Text, default="")
    primary_book_count: Mapped[str] = mapped_column(String(100), default="")
    gr_book_1_rating: Mapped[str] = mapped_column(String(50), default="")
    gr_book_2_rating: Mapped[str] = mapped_column(String(50), default="")
    gr_book_3_rating: Mapped[str] = mapped_column(String(50), default="")
    gr_book_4_rating: Mapped[str] = mapped_column(String(50), default="")
    gr_book_5_rating: Mapped[str] = mapped_column(String(50), default="")
    gr_book_6_rating: Mapped[str] = mapped_column(String(50), default="")
    gr_book_7_rating: Mapped[str] = mapped_column(String(50), default="")
    gr_book_8_rating: Mapped[str] = mapped_column(String(50), default="")
    gr_book_9_rating: Mapped[str] = mapped_column(String(50), default="")
    gr_book_10_rating: Mapped[str] = mapped_column(String(50), default="")
    final_list: Mapped[str] = mapped_column(String(20), default="")
    rationale: Mapped[str] = mapped_column(Text, default="")
    goodreads_rating: Mapped[str] = mapped_column(String(50), default="")
    goodreads_rating_count: Mapped[str] = mapped_column(String(50), default="")
    word_count: Mapped[int | None] = mapped_column(Integer)
    audio_score: Mapped[int | None] = mapped_column(Integer)
    book_type: Mapped[str] = mapped_column(String(50), default="")
    benchmark_match: Mapped[bool] = mapped_column(Boolean, default=False)
    shortlisted: Mapped[bool] = mapped_column(Boolean, default=False)
    provenance_json: Mapped[dict] = mapped_column(JSON, default=dict)

    batch: Mapped[Batch] = relationship(back_populates="books")
    sources: Mapped[list["BookSource"]] = relationship(back_populates="book", cascade="all, delete-orphan")
    contact: Mapped["Contact | None"] = relationship(back_populates="book", cascade="all, delete-orphan", uselist=False)
    evaluation: Mapped["Evaluation | None"] = relationship(back_populates="book", cascade="all, delete-orphan", uselist=False)
    outreach_messages: Mapped[list["OutreachMessage"]] = relationship(back_populates="book", cascade="all, delete-orphan")


class BookSource(TimestampMixin, Base):
    __tablename__ = "book_sources"

    id: Mapped[int] = mapped_column(primary_key=True)
    book_id: Mapped[int] = mapped_column(ForeignKey("books.id"))
    source_type: Mapped[str] = mapped_column(String(50))
    source_url: Mapped[str] = mapped_column(Text, default="")
    external_id: Mapped[str] = mapped_column(String(255), default="")
    raw_payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    normalized_payload_json: Mapped[dict] = mapped_column(JSON, default=dict)

    book: Mapped[Book] = relationship(back_populates="sources")


class Contact(TimestampMixin, Base):
    __tablename__ = "contacts"

    id: Mapped[int] = mapped_column(primary_key=True)
    book_id: Mapped[int] = mapped_column(ForeignKey("books.id"), unique=True)
    email_id: Mapped[str] = mapped_column(String(255), default="")
    email_source_note: Mapped[str] = mapped_column(Text, default="")
    email_type: Mapped[str] = mapped_column(String(255), default="")
    contact_forms: Mapped[str] = mapped_column(Text, default="")
    facebook_link: Mapped[str] = mapped_column(Text, default="")
    publisher_details: Mapped[str] = mapped_column(Text, default="")
    website: Mapped[str] = mapped_column(Text, default="")
    author_email: Mapped[str] = mapped_column(String(255), default="")
    agent_email: Mapped[str] = mapped_column(String(255), default="")

    book: Mapped[Book] = relationship(back_populates="contact")


class Evaluation(TimestampMixin, Base):
    __tablename__ = "evaluations"

    id: Mapped[int] = mapped_column(primary_key=True)
    book_id: Mapped[int] = mapped_column(ForeignKey("books.id"), unique=True)
    story_score: Mapped[int | None] = mapped_column(Integer)
    characters_score: Mapped[int | None] = mapped_column(Integer)
    hooks_score: Mapped[int | None] = mapped_column(Integer)
    series_potential_score: Mapped[int | None] = mapped_column(Integer)
    audio_adaptability_score: Mapped[int | None] = mapped_column(Integer)
    india_fit_score: Mapped[int | None] = mapped_column(Integer)
    notes: Mapped[str] = mapped_column(Text, default="")

    book: Mapped[Book] = relationship(back_populates="evaluation")


class OutreachMessage(TimestampMixin, Base):
    __tablename__ = "outreach_messages"

    id: Mapped[int] = mapped_column(primary_key=True)
    book_id: Mapped[int] = mapped_column(ForeignKey("books.id"))
    recipient: Mapped[str] = mapped_column(String(255), default="")
    cc: Mapped[str] = mapped_column(String(255), default="")
    subject: Mapped[str] = mapped_column(String(255), default="")
    body: Mapped[str] = mapped_column(Text, default="")
    template: Mapped[str] = mapped_column(String(100), default="formal")
    status: Mapped[str] = mapped_column(String(50), default="draft")
    sent_at: Mapped[datetime | None] = mapped_column(DateTime)

    book: Mapped[Book] = relationship(back_populates="outreach_messages")


class ExportRecord(TimestampMixin, Base):
    __tablename__ = "exports"

    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("batches.id"))
    export_format: Mapped[str] = mapped_column(String(20))
    file_path: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(50), default="completed")
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)

    batch: Mapped[Batch] = relationship(back_populates="exports")

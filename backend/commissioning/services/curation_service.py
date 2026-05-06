from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload

from ..models import Batch, Book, Contact, Evaluation, Job, OutreachMessage
from .mapping_service import apply_benchmark_mapping, apply_tier_mapping

OUTREACH_TEMPLATES = {
    "formal": (
        "Dear {author}'s Literary Team,\n\n"
        "I'm reaching out from Pocket FM regarding {title}. "
        "We would love to explore audio rights and commissioning possibilities.\n\n"
        "Warm regards,\n{sender_name}\n{sender_email}"
    ),
    "casual": (
        "Hi {author}'s team,\n\n"
        "Writing from Pocket FM because {title} looks like a strong fit for audio adaptation.\n\n"
        "Best,\n{sender_name}\n{sender_email}"
    ),
    "rights": (
        "Dear Rights Team,\n\n"
        "This is a formal inquiry regarding audio rights for {title} by {author}.\n\n"
        "Regards,\n{sender_name}\n{sender_email}"
    ),
}


def batch_summary(db: Session, batch: Batch) -> dict:
    total_books = db.query(func.count(Book.id)).filter(Book.batch_id == batch.id).scalar() or 0
    shortlisted = db.query(func.count(Book.id)).filter(Book.batch_id == batch.id, Book.shortlisted.is_(True)).scalar() or 0
    outreach_ready = (
        db.query(func.count(Book.id))
        .join(Contact, Contact.book_id == Book.id, isouter=True)
        .filter(Book.batch_id == batch.id)
        .filter(or_(Contact.email_id != "", Contact.contact_forms != "", Contact.facebook_link != ""))
        .scalar()
        or 0
    )
    emails_found = (
        db.query(func.count(Contact.id))
        .join(Book, Book.id == Contact.book_id)
        .filter(Book.batch_id == batch.id, Contact.email_id != "")
        .scalar()
        or 0
    )
    jobs = db.query(Job.status, func.count(Job.id)).filter(Job.batch_id == batch.id).group_by(Job.status).all()
    return {
        "batch_id": batch.id,
        "name": batch.name,
        "total_sources": len(batch.source_links),
        "total_books": total_books,
        "shortlisted_books": shortlisted,
        "outreach_ready": outreach_ready,
        "emails_found": emails_found,
        "job_counts": {row[0]: row[1] for row in jobs},
    }


def list_books(
    db: Session,
    *,
    batch_id: int,
    page: int,
    page_size: int,
    search: str = "",
    genre: str = "",
    source_type: str = "",
    shortlisted: bool | None = None,
) -> tuple[int, list[Book]]:
    query = (
        db.query(Book)
        .options(joinedload(Book.contact), joinedload(Book.evaluation), joinedload(Book.outreach_messages))
        .filter(Book.batch_id == batch_id)
    )
    if search:
        term = f"%{search}%"
        query = query.filter(or_(Book.title.ilike(term), Book.author.ilike(term), Book.genre.ilike(term)))
    if genre:
        query = query.filter(Book.genre == genre)
    if source_type == "amazon":
        query = query.filter(Book.amazon_url != "")
    if source_type == "goodreads":
        query = query.filter(Book.goodread_link != "")
    if shortlisted is not None:
        query = query.filter(Book.shortlisted.is_(shortlisted))
    total = query.count()
    items = query.order_by(Book.id.asc()).offset((page - 1) * page_size).limit(page_size).all()
    return total, items


def patch_book(db: Session, book: Book, payload: dict) -> Book:
    for field, value in payload.items():
        if value is not None and hasattr(book, field):
            setattr(book, field, value)
    db.commit()
    db.refresh(book)
    return book


def _parse_int(value) -> int | None:
    if value in (None, "", "N/A"):
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except ValueError:
        return None


def apply_benchmark(db: Session, batch_id: int, filters: dict) -> list[int]:
    genres = set(filters.get("genres") or [])
    types = set(filters.get("types") or [])
    matched_ids = set()
    books = db.query(Book).filter(Book.batch_id == batch_id).order_by(Book.id.asc()).all()
    for book in books:
        apply_benchmark_mapping(book)
        series_count = _parse_int(book.primary_book_count) or _parse_int(book.book_number) or 1
        checks = [
            book.rating is not None and book.rating >= filters["min_rating"],
            book.rating_count is not None and book.rating_count >= filters["min_reviews"],
            book.word_count is not None and book.word_count >= filters["min_word_count"],
            series_count <= filters["max_series_books"],
            book.audio_score is not None and book.audio_score >= filters["min_audio_score"],
            not genres or book.genre in genres,
            not types or book.book_type in types,
        ]
        if all(checks):
            matched_ids.add(book.id)
    for book in db.query(Book).filter(Book.batch_id == batch_id).all():
        book.benchmark_match = book.id in matched_ids
        book.shortlisted = book.id in matched_ids
    db.commit()
    return sorted(matched_ids)


def apply_tier_mapping_to_batch(db: Session, batch_id: int, rules: list[dict] | None = None, shortlisted_only: bool = False) -> dict:
    query = db.query(Book).filter(Book.batch_id == batch_id)
    if shortlisted_only:
        query = query.filter(Book.shortlisted.is_(True))
    books = query.order_by(Book.id.asc()).all()
    tier_counts: dict[str, int] = {}
    for book in books:
        apply_benchmark_mapping(book)
        profile = apply_tier_mapping(book, rules)
        tier = profile["Tier"] or "Unmapped"
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
    batch = db.get(Batch, batch_id)
    if batch is not None and rules:
        metadata = dict(batch.metadata_json or {})
        metadata["tier_rules"] = rules
        metadata["tier_mapping_scope"] = "shortlisted" if shortlisted_only else "all"
        batch.metadata_json = metadata
    db.commit()
    return {"total": len(books), "tier_counts": tier_counts}


def get_outreach_items(db: Session, batch_id: int) -> list[Book]:
    return (
        db.query(Book)
        .options(joinedload(Book.contact), joinedload(Book.outreach_messages))
        .filter(Book.batch_id == batch_id)
        .order_by(Book.shortlisted.desc(), Book.id.asc())
        .all()
    )


def build_outreach_draft(db: Session, book: Book, template: str, sender_name: str, sender_email: str) -> OutreachMessage:
    body = OUTREACH_TEMPLATES.get(template, OUTREACH_TEMPLATES["formal"]).format(
        author=book.author or "Author",
        title=book.title,
        sender_name=sender_name,
        sender_email=sender_email,
    )
    recipient = book.contact.email_id if book.contact and book.contact.email_id else ""
    message = OutreachMessage(
        book_id=book.id,
        recipient=recipient,
        subject=f"Commissioning inquiry — {book.title} · Pocket FM",
        body=body,
        template=template,
        status="draft",
    )
    db.add(message)
    db.commit()
    db.refresh(message)
    return message


def patch_outreach(db: Session, message: OutreachMessage | None, book: Book, payload: dict) -> OutreachMessage:
    if message is None:
        message = OutreachMessage(book_id=book.id)
        db.add(message)
    for field, value in payload.items():
        if value is not None and hasattr(message, field):
            setattr(message, field, value)
    if payload.get("status") == "sent":
        message.sent_at = datetime.utcnow()
    db.commit()
    db.refresh(message)
    return message


def patch_evaluation(db: Session, book: Book, payload: dict) -> Evaluation:
    evaluation = book.evaluation
    if evaluation is None:
        evaluation = Evaluation(book_id=book.id)
        db.add(evaluation)
    for field, value in payload.items():
        if value is not None and hasattr(evaluation, field):
            setattr(evaluation, field, value)
    db.commit()
    db.refresh(evaluation)
    return evaluation

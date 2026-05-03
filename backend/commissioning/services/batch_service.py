from __future__ import annotations

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..models import Batch


DEFAULT_BATCH_NAME = "April 2026 Commissioning Sprint"


def ensure_working_batch(db: Session, batch_id: int | None = None) -> Batch:
    if batch_id is not None:
        batch = db.get(Batch, batch_id)
        if batch is not None:
            return batch
    else:
        batch = db.query(Batch).filter(Batch.name == DEFAULT_BATCH_NAME).order_by(Batch.id.asc()).first()
        if batch is not None:
            return batch

    batch = Batch(
        name=DEFAULT_BATCH_NAME if batch_id is None else f"{DEFAULT_BATCH_NAME} #{batch_id}",
        genre="",
        subgenre="",
        description="",
        status="active",
    )
    if batch_id is not None:
        batch.id = batch_id
    db.add(batch)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        batch = db.get(Batch, batch_id) if batch_id is not None else None
        if batch is None:
            batch = db.query(Batch).filter(Batch.name == DEFAULT_BATCH_NAME).order_by(Batch.id.asc()).first()
    if batch is None:
        raise RuntimeError("Could not create a working batch")
    db.refresh(batch)
    return batch

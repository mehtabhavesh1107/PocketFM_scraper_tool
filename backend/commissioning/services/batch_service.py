from __future__ import annotations

from sqlalchemy.orm import Session

from ..models import Batch


DEFAULT_BATCH_NAME = "April 2026 Commissioning Sprint"


def ensure_working_batch(db: Session) -> Batch:
    batch = db.query(Batch).filter(Batch.name == DEFAULT_BATCH_NAME).order_by(Batch.id.asc()).first()
    if batch is None:
        batch = Batch(
            name=DEFAULT_BATCH_NAME,
            genre="",
            subgenre="",
            description="",
            status="active",
        )
        db.add(batch)
        db.commit()
        db.refresh(batch)
    return batch

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from .settings import DATABASE_URL, ensure_directories


class Base(DeclarativeBase):
    pass


ensure_directories()
connect_args = {"check_same_thread": False, "timeout": 30} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args, future=True, pool_pre_ping=True)


if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _configure_sqlite(dbapi_connection, connection_record):  # pragma: no cover - exercised at runtime
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    from . import models  # noqa: F401

    Base.metadata.create_all(bind=engine)
    _ensure_runtime_columns()


def _ensure_runtime_columns() -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "batches" not in table_names:
        return
    batch_columns = {column["name"] for column in inspector.get_columns("batches")}
    book_columns = {column["name"] for column in inspector.get_columns("books")} if "books" in table_names else set()
    tier_columns = {
        "tier": "VARCHAR(50)",
        "gr_ratings": "VARCHAR(100)",
        "trope": "VARCHAR(100)",
        "length": "VARCHAR(100)",
        "mg_min": "VARCHAR(50)",
        "mg_max": "VARCHAR(50)",
        "rev_share_min": "VARCHAR(50)",
        "rev_share_max": "VARCHAR(50)",
    }
    with engine.begin() as connection:
        if "workspace_id" not in batch_columns:
            connection.execute(text("ALTER TABLE batches ADD COLUMN workspace_id VARCHAR(100) DEFAULT 'public' NOT NULL"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_batches_workspace_id ON batches (workspace_id)"))
        for column_name, column_type in tier_columns.items():
            if "books" in table_names and column_name not in book_columns:
                connection.execute(text(f"ALTER TABLE books ADD COLUMN {column_name} {column_type} DEFAULT '' NOT NULL"))

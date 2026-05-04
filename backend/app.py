from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from commissioning.api.routes import router
from commissioning.db import SessionLocal, init_db
from commissioning.jobs.manager import job_manager
from commissioning.models import Job, JobEvent
from commissioning.settings import ALLOWED_ORIGINS, ensure_directories


def _recover_interrupted_jobs() -> None:
    """Fail queued/running DB jobs that cannot survive a backend restart.

    Background work runs in this process' ThreadPoolExecutor. If the backend
    process stops, any DB job left as queued/running no longer has an in-memory
    worker attached, so leaving it active makes the UI poll forever.
    """
    db = SessionLocal()
    try:
        jobs = db.query(Job).filter(Job.status.in_(("queued", "running"))).all()
        if not jobs:
            return
        now = datetime.utcnow()
        for job in jobs:
            job.status = "failed"
            job.error = "Job was interrupted by a service restart. Please run it again."
            job.message = "Job interrupted by service restart."
            job.finished_at = now
            db.add(
                JobEvent(
                    job_id=job.id,
                    level="error",
                    message=job.message,
                    payload_json={"recovered_on_startup": True},
                    progress_percent=job.progress_percent,
                )
            )
        db.commit()
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_directories()
    init_db()
    _recover_interrupted_jobs()
    yield
    job_manager.shutdown()


app = FastAPI(
    title="Pocket FM Commissioning Backend",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)

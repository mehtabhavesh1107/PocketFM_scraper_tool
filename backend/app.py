from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

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
    if job_manager.runs_inline:
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


STATIC_DIR = Path(__file__).resolve().parent / "static"
if STATIC_DIR.exists():
    assets_dir = STATIC_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_frontend(full_path: str):
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="API route not found")
        requested = STATIC_DIR / full_path
        if requested.is_file():
            return FileResponse(requested)
        return FileResponse(STATIC_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)

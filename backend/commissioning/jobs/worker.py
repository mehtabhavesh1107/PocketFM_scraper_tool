from __future__ import annotations

import os
import socket
import time
import threading
from collections.abc import Callable

from sqlalchemy.orm import Session

from ..db import SessionLocal, init_db
from ..models import Job, JobEvent
from .tasks import run_amazon_detail_item_job, run_contact_job, run_fast_scrape_job, run_goodreads_job, run_scrape_job


TASKS: dict[str, Callable[[str, int], None]] = {
    "scrape": run_scrape_job,
    "fast_scrape": run_fast_scrape_job,
    "amazon_detail_item": run_amazon_detail_item_job,
    "enrich_goodreads": run_goodreads_job,
    "enrich_contacts": run_contact_job,
}


def _worker_name() -> str:
    return os.getenv("COMMISSIONING_WORKER_NAME") or socket.gethostname() or "worker"


def _claim_next_job(db: Session) -> Job | None:
    job = (
        db.query(Job)
        .filter(Job.status == "queued", Job.stage.in_(TASKS.keys()))
        .order_by(Job.created_at.asc())
        .with_for_update(skip_locked=True)
        .first()
    )
    if job is None:
        return None
    job.status = "running"
    job.message = f"Claimed by {_worker_name()}."
    db.add(
        JobEvent(
            job_id=job.id,
            level="info",
            message=job.message,
            payload_json={"worker": _worker_name()},
            progress_percent=job.progress_percent,
        )
    )
    db.commit()
    db.refresh(job)
    return job


def _fail_unknown_stage(job_id: str, stage: str) -> None:
    db = SessionLocal()
    try:
        job = db.get(Job, job_id)
        if job is None:
            return
        job.status = "failed"
        job.error = f"No worker task registered for stage '{stage}'."
        job.message = job.error
        db.add(
            JobEvent(
                job_id=job.id,
                level="error",
                message=job.message,
                payload_json={"stage": stage},
                progress_percent=job.progress_percent,
            )
        )
        db.commit()
    finally:
        db.close()


def run_one_job() -> bool:
    db = SessionLocal()
    try:
        job = _claim_next_job(db)
        if job is None:
            return False
        job_id = job.id
        batch_id = job.batch_id
        stage = job.stage
    finally:
        db.close()

    task = TASKS.get(stage)
    if task is None:
        _fail_unknown_stage(job_id, stage)
        return True
    task(job_id, batch_id)
    return True


def worker_loop(
    *,
    max_jobs: int | None = None,
    poll_seconds: float | None = None,
    stop_event: threading.Event | None = None,
) -> None:
    init_db()
    if poll_seconds is None:
        try:
            poll_seconds = float(os.getenv("COMMISSIONING_WORKER_POLL_SECONDS", "2.0"))
        except ValueError:
            poll_seconds = 2.0
    jobs_run = 0
    while not (stop_event and stop_event.is_set()):
        did_work = run_one_job()
        if did_work:
            jobs_run += 1
            if max_jobs is not None and jobs_run >= max_jobs:
                return
            continue
        if max_jobs is not None:
            return
        wait_seconds = max(0.5, poll_seconds)
        if stop_event:
            stop_event.wait(wait_seconds)
        else:
            time.sleep(wait_seconds)


def main() -> None:
    raw_max_jobs = os.getenv("COMMISSIONING_WORKER_MAX_JOBS", "").strip()
    max_jobs = int(raw_max_jobs) if raw_max_jobs else None
    worker_loop(max_jobs=max_jobs)


if __name__ == "__main__":
    main()

from __future__ import annotations

import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from commissioning.jobs.worker import worker_loop


stop_event = threading.Event()
worker_thread: threading.Thread | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_thread
    stop_event.clear()
    worker_thread = threading.Thread(
        target=worker_loop,
        kwargs={"stop_event": stop_event},
        name="commissioning-db-worker",
        daemon=True,
    )
    worker_thread.start()
    yield
    stop_event.set()
    if worker_thread:
        worker_thread.join(timeout=10)


app = FastAPI(title="Pocket FM Commissioning Worker", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "worker": "running" if worker_thread and worker_thread.is_alive() else "stopped"}

from __future__ import annotations

import os
from concurrent.futures import Future, ThreadPoolExecutor

from ..settings import JOB_BACKEND


class QueuedJobFuture(Future):
    def __init__(self, job_id: str):
        super().__init__()
        self.job_id = job_id
        self.set_result(job_id)


class JobManager:
    def __init__(self, max_workers: int | None = None):
        self.backend = JOB_BACKEND if JOB_BACKEND in {"thread", "database"} else "thread"
        self.executor: ThreadPoolExecutor | None = None
        self.futures: dict[str, Future] = {}
        if self.backend == "database":
            return
        if max_workers is None:
            try:
                max_workers = int(os.getenv("COMMISSIONING_JOB_WORKERS", "4"))
            except ValueError:
                max_workers = 4
        max_workers = max(1, min(max_workers, 32))
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="commissioning-job")

    @property
    def runs_inline(self) -> bool:
        return self.backend == "thread"

    def submit(self, job_id: str, func, *args, **kwargs) -> Future:
        if self.backend == "database":
            future = QueuedJobFuture(job_id)
            self.futures[job_id] = future
            return future
        if self.executor is None:
            raise RuntimeError("Thread job backend is not initialized.")
        future = self.executor.submit(func, *args, **kwargs)
        self.futures[job_id] = future
        return future

    def shutdown(self) -> None:
        if self.executor is not None:
            self.executor.shutdown(wait=False, cancel_futures=False)


job_manager = JobManager()

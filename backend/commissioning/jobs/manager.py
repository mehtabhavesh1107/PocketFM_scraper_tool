from __future__ import annotations

import os
from concurrent.futures import Future, ThreadPoolExecutor


class JobManager:
    def __init__(self, max_workers: int | None = None):
        if max_workers is None:
            try:
                max_workers = int(os.getenv("COMMISSIONING_JOB_WORKERS", "4"))
            except ValueError:
                max_workers = 4
        max_workers = max(1, min(max_workers, 32))
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="commissioning-job")
        self.futures: dict[str, Future] = {}

    def submit(self, job_id: str, func, *args, **kwargs) -> Future:
        future = self.executor.submit(func, *args, **kwargs)
        self.futures[job_id] = future
        return future

    def shutdown(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=False)


job_manager = JobManager()

from __future__ import annotations

import argparse
import sys
import time
import uuid

import requests


def api(base_url: str, workspace_id: str, method: str, path: str, **kwargs):
    headers = kwargs.pop("headers", {})
    headers["X-Workspace-Id"] = workspace_id
    response = requests.request(method, f"{base_url.rstrip('/')}{path}", headers=headers, timeout=60, **kwargs)
    response.raise_for_status()
    if response.content:
        return response.json()
    return {}


def poll_job(base_url: str, workspace_id: str, job_id: str, timeout_seconds: int) -> dict:
    started = time.time()
    while time.time() - started < timeout_seconds:
        job = api(base_url, workspace_id, "GET", f"/api/jobs/{job_id}")
        print(f"{job['stage']} {job['status']}: {job.get('progress_percent', 0)}% {job.get('message', '')}")
        if job["status"] in {"completed", "failed"}:
            return job
        time.sleep(5)
    raise TimeoutError(f"Job {job_id} did not finish within {timeout_seconds} seconds.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Cloud Run smoke test for Pocket FM commissioning.")
    parser.add_argument("--base-url", required=True, help="Cloud Run service URL, for example https://service-xyz.run.app")
    parser.add_argument("--amazon-url", required=True, help="Small Amazon source URL for the smoke scrape.")
    parser.add_argument("--goodreads-url", required=True, help="Small Goodreads source URL for the smoke scrape.")
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    args = parser.parse_args()

    workspace_id = f"smoke-{uuid.uuid4()}"
    health = requests.get(f"{args.base_url.rstrip('/')}/api/health", timeout=30)
    health.raise_for_status()
    print(f"Health OK: {health.json()}")

    batch = api(args.base_url, workspace_id, "POST", "/api/bootstrap")["batch"]
    batch_id = batch["id"]
    print(f"Workspace {workspace_id}, batch {batch_id}")

    api(
        args.base_url,
        workspace_id,
        "PUT",
        f"/api/batches/{batch_id}/sources",
        json=[
            {"source_type": "amazon", "url": args.amazon_url, "max_results": 0, "output_format": "CSV"},
            {"source_type": "goodreads", "url": args.goodreads_url, "max_results": 0, "output_format": "CSV"},
        ],
    )

    fast_job = api(args.base_url, workspace_id, "POST", f"/api/batches/{batch_id}/jobs/scrape-fast")["job"]
    fast_result = poll_job(args.base_url, workspace_id, fast_job["id"], args.timeout_seconds)
    if fast_result["status"] != "completed":
        print(f"Fast scrape failed: {fast_result.get('error') or fast_result.get('message')}")
        return 2

    gr_job = api(args.base_url, workspace_id, "POST", f"/api/batches/{batch_id}/jobs/enrich-goodreads")["job"]
    gr_result = poll_job(args.base_url, workspace_id, gr_job["id"], args.timeout_seconds)
    if gr_result["status"] != "completed":
        print(f"Goodreads enrichment failed: {gr_result.get('error') or gr_result.get('message')}")
        return 3

    books = api(args.base_url, workspace_id, "GET", f"/api/batches/{batch_id}/books?page_size=100")
    sources = api(args.base_url, workspace_id, "GET", f"/api/batches/{batch_id}/sources")
    quality = api(args.base_url, workspace_id, "GET", f"/api/batches/{batch_id}/data-quality")
    print(f"Books scraped: {books['total']}")
    print(f"Sources: {[(item['source_type'], item['status']) for item in sources]}")
    print(f"Quality ready: {quality['ready']} critical={quality['critical_count']} warnings={quality['warning_count']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

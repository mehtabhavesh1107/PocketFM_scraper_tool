from __future__ import annotations

import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

TEST_ROOT = Path(tempfile.gettempdir()) / f"pocketfm_commissioning_tests_{os.getpid()}"
os.environ["COMMISSIONING_DATA_DIR"] = str(TEST_ROOT / "backend_data")
os.environ["COMMISSIONING_GENERATED_DIR"] = str(TEST_ROOT / "generated")
os.environ["COMMISSIONING_DATABASE_URL"] = f"sqlite:///{(TEST_ROOT / 'backend_data' / 'commissioning.db').as_posix()}"

from app import app
from commissioning.db import SessionLocal, engine, init_db
from commissioning.models import Base, Batch, Book, Contact, Job
from commissioning.services.discovery_service import discover_amazon_books
from commissioning.services.amazon_http import discover_amazon_items


class CommissioningApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Base.metadata.drop_all(bind=engine)
        init_db()

    @classmethod
    def tearDownClass(cls):
        engine.dispose()

    def setUp(self):
        Base.metadata.drop_all(bind=engine)
        init_db()

    async def _request(self, method: str, url: str, **kwargs):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.request(method, url, **kwargs)

    def test_create_batch_and_sources(self):
        async def run():
            response = await self._request(
                "POST",
                "/api/batches",
                json={"name": "KU Horror", "genre": "Horror", "subgenre": "Crime Thriller"},
            )
            self.assertEqual(response.status_code, 200)
            batch = response.json()
            sources = await self._request(
                "POST",
                f"/api/batches/{batch['id']}/sources",
                json=[
                    {"source_type": "amazon", "url": "https://www.amazon.com/list", "max_results": 0},
                    {"source_type": "amazon", "url": "https://www.amzn.com/Best-Sellers-Mystery%2C-Thriller-Suspense/zgbs/books/18", "max_results": 0},
                    {"source_type": "goodreads", "url": "https://www.goodreads.com/list/show/1", "max_results": 0},
                ],
            )
            self.assertEqual(sources.status_code, 200)
            self.assertEqual(len(sources.json()), 3)

            schema = await self._request("GET", "/api/reference-schema")
            self.assertEqual(schema.status_code, 200)
            self.assertIn("Title", [field["label"] for field in schema.json()["fields"]])

        import asyncio

        asyncio.run(run())

    def test_amzn_short_domain_supported_by_discovery_service(self):
        with patch("commissioning.services.discovery_service.discover_amazon_records", return_value=[{"title": "Short Domain"}]):
            records = discover_amazon_books("https://www.amzn.com/Best-Sellers-Mystery%2C-Thriller-Suspense/zgbs/books/18", 1)

        self.assertEqual(records, [{"title": "Short Domain"}])

    def test_source_urls_are_unescaped_before_storage(self):
        async def run():
            response = await self._request("POST", "/api/batches", json={"name": "Escaped URL"})
            self.assertEqual(response.status_code, 200)
            batch = response.json()
            sources = await self._request(
                "POST",
                f"/api/batches/{batch['id']}/sources",
                json=[
                    {
                        "source_type": "amazon",
                        "url": " https://www.amazon.com/amz-books/seeMore/?_encoding=UTF8&amp;asins=B0FKTTYMVG%2C B0FJNF8PP6 ",
                        "max_results": 0,
                    }
                ],
            )
            self.assertEqual(sources.status_code, 200)
            stored_url = sources.json()[0]["url"]
            self.assertIn("&asins=", stored_url)
            self.assertNotIn("&amp;", stored_url)
            self.assertNotIn(" ", stored_url)

        import asyncio

        asyncio.run(run())

    def test_fast_scrape_endpoint_queues_fast_stage(self):
        async def run():
            response = await self._request(
                "POST",
                "/api/batches",
                json={"name": "Fast Queue", "genre": "Thriller", "subgenre": "Domestic"},
            )
            self.assertEqual(response.status_code, 200)
            batch = response.json()
            with patch("commissioning.api.routes.job_manager.submit") as submit:
                queued = await self._request("POST", f"/api/batches/{batch['id']}/jobs/scrape-fast")
            self.assertEqual(queued.status_code, 200)
            self.assertEqual(queued.json()["job"]["stage"], "fast_scrape")
            submit.assert_called_once()

        import asyncio

        asyncio.run(run())

    def test_missing_batch_is_auto_created_for_local_state_recovery(self):
        async def run():
            sources = await self._request("GET", "/api/batches/987/sources")
            self.assertEqual(sources.status_code, 200)
            self.assertEqual(sources.json(), [])

            db = SessionLocal()
            recovered = db.get(Batch, 987)
            db.close()
            self.assertIsNotNone(recovered)

            save = await self._request(
                "PUT",
                "/api/batches/987/sources",
                json=[{"source_type": "amazon", "url": "https://www.amazon.com/dp/B0TEST1234", "max_results": 0}],
            )
            self.assertEqual(save.status_code, 200)
            self.assertEqual(len(save.json()), 1)

        import asyncio

        asyncio.run(run())

    def test_anonymous_workspaces_have_isolated_runs(self):
        async def run():
            workspace_a = {"X-Workspace-Id": "ws-api-a"}
            workspace_b = {"X-Workspace-Id": "ws-api-b"}

            boot_a = await self._request("POST", "/api/bootstrap", headers=workspace_a)
            boot_b = await self._request("POST", "/api/bootstrap", headers=workspace_b)
            self.assertEqual(boot_a.status_code, 200)
            self.assertEqual(boot_b.status_code, 200)
            batch_a = boot_a.json()["batch"]
            batch_b = boot_b.json()["batch"]
            self.assertNotEqual(batch_a["id"], batch_b["id"])
            self.assertEqual(batch_a["workspace_id"], "ws-api-a")
            self.assertEqual(batch_b["workspace_id"], "ws-api-b")

            save_a = await self._request(
                "PUT",
                f"/api/batches/{batch_a['id']}/sources",
                headers=workspace_a,
                json=[{"source_type": "amazon", "url": "https://www.amazon.com/dp/B0TEST1234", "max_results": 0}],
            )
            self.assertEqual(save_a.status_code, 200)

            list_a = await self._request("GET", "/api/batches", headers=workspace_a)
            list_b = await self._request("GET", "/api/batches", headers=workspace_b)
            self.assertEqual([item["workspace_id"] for item in list_a.json()], ["ws-api-a"])
            self.assertEqual([item["workspace_id"] for item in list_b.json()], ["ws-api-b"])

            blocked = await self._request("GET", f"/api/batches/{batch_a['id']}/sources", headers=workspace_b)
            self.assertEqual(blocked.status_code, 404)

        import asyncio

        asyncio.run(run())

    def test_upload_schema(self):
        async def run():
            response = await self._request(
                "POST",
                "/api/schemas/upload",
                files={"file": ("schema.csv", io.BytesIO(b"Title,Author,Genre"))},
                data={"source_type": "amazon"},
            )
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["source_type"], "amazon")
            self.assertEqual(len(payload["fields_json"]), 3)

        import asyncio

        asyncio.run(run())

    def test_csv_fallback_imports_manual_rows(self):
        async def run():
            response = await self._request("POST", "/api/batches", json={"name": "Manual CSV"})
            self.assertEqual(response.status_code, 200)
            batch = response.json()
            csv_body = b"Title,Author,URL,Rating,no. of rating,Goodread Link\nManual Book,Jane Author,https://www.amazon.com/dp/B0MANUAL01,4.3,1200,https://www.goodreads.com/book/show/1\n"
            imported = await self._request(
                "POST",
                f"/api/batches/{batch['id']}/imports/csv",
                files={"file": ("manual.csv", io.BytesIO(csv_body), "text/csv")},
            )
            self.assertEqual(imported.status_code, 200)
            self.assertEqual(imported.json()["imported"], 1)

            books = await self._request("GET", f"/api/batches/{batch['id']}/books")
            self.assertEqual(books.status_code, 200)
            item = books.json()["items"][0]
            self.assertEqual(item["title"], "Manual Book")
            self.assertEqual(item["rating_count"], 1200)
            self.assertEqual(item["goodread_link"], "https://www.goodreads.com/book/show/1")

            sources = await self._request("GET", f"/api/batches/{batch['id']}/sources")
            self.assertTrue(any(row["source_type"] == "manual_csv" for row in sources.json()))

        import asyncio

        asyncio.run(run())

    def test_database_worker_claims_queued_job(self):
        import commissioning.jobs.worker as worker

        db = SessionLocal()
        try:
            batch = Batch(name="Worker Queue")
            db.add(batch)
            db.flush()
            job = Job(batch_id=batch.id, stage="fast_scrape", status="queued")
            db.add(job)
            db.commit()
            job_id = job.id
            batch_id = batch.id
        finally:
            db.close()

        original_task = worker.TASKS["fast_scrape"]

        def fake_task(claimed_job_id: str, claimed_batch_id: int) -> None:
            self.assertEqual(claimed_job_id, job_id)
            self.assertEqual(claimed_batch_id, batch_id)
            task_db = SessionLocal()
            try:
                claimed = task_db.get(Job, claimed_job_id)
                claimed.status = "completed"
                claimed.message = "fake worker done"
                task_db.commit()
            finally:
                task_db.close()

        try:
            worker.TASKS["fast_scrape"] = fake_task
            self.assertTrue(worker.run_one_job())
            db = SessionLocal()
            try:
                completed = db.get(Job, job_id)
                self.assertEqual(completed.status, "completed")
                self.assertEqual(completed.message, "fake worker done")
            finally:
                db.close()
        finally:
            worker.TASKS["fast_scrape"] = original_task

    def test_books_and_benchmark(self):
        db = SessionLocal()
        batch = Batch(name="Test Batch", genre="Horror", subgenre="Thriller")
        db.add(batch)
        db.commit()
        db.refresh(batch)
        book = Book(
            batch_id=batch.id,
            title="Demo Book",
            author="Author One",
            genre="Thriller",
            rating=4.5,
            rating_count=15000,
            goodreads_rating_count="25000",
            word_count=90000,
            total_pages_in_series="3400",
            total_word_count="850000",
            total_hours="85",
            audio_score=85,
            book_type="Series",
        )
        db.add(book)
        db.flush()
        db.add(Contact(book_id=book.id, email_id="author@example.com"))
        db.commit()
        batch_id = batch.id
        db.close()

        async def run():
            books = await self._request("GET", f"/api/batches/{batch_id}/books")
            self.assertEqual(books.status_code, 200)
            self.assertEqual(books.json()["total"], 1)

            benchmark = await self._request(
                "POST",
                f"/api/batches/{batch_id}/benchmark/apply",
                json={
                    "min_rating": 4.0,
                    "min_reviews": 1000,
                    "min_word_count": 50000,
                    "max_series_books": 10,
                    "min_audio_score": 60,
                    "genres": ["Thriller"],
                    "types": ["Series"],
                },
            )
            self.assertEqual(benchmark.status_code, 200)
            self.assertEqual(benchmark.json()["total"], 1)

            tier = await self._request("POST", f"/api/batches/{batch_id}/tier-mapping/apply")
            self.assertEqual(tier.status_code, 200)
            self.assertEqual(tier.json()["tier_counts"]["Tier 1"], 1)

            refreshed = await self._request("GET", f"/api/batches/{batch_id}/books")
            row = refreshed.json()["items"][0]
            self.assertEqual(row["tier"], "Tier 1")
            self.assertEqual(row["length"], "85")
            self.assertEqual(row["mg_min"], "10k")
            self.assertEqual(row["mg_max"], "15k")

            export = await self._request(
                "POST",
                f"/api/batches/{batch_id}/exports",
                json={"export_format": "csv"},
            )
            self.assertEqual(export.status_code, 200)
            export_path = Path(export.json()["file_path"])
            self.assertTrue(export_path.exists())
            header = export_path.read_text(encoding="utf-8").splitlines()[0]
            self.assertIn("# of Hrs", header)
            self.assertIn("Goodread Link", header)

        import asyncio

        asyncio.run(run())

    def test_accept_goodreads_candidate_updates_book(self):
        db = SessionLocal()
        batch = Batch(name="Goodreads Review")
        db.add(batch)
        db.flush()
        book = Book(
            batch_id=batch.id,
            title="Review Candidate",
            author="Jane Writer",
            publisher="Pocket House",
            publication_date="May 1, 2026",
            print_length="320",
            provenance_json={"amazon": {"isbn_10": "123456789X"}},
        )
        db.add(book)
        db.commit()
        book_id = book.id
        db.close()

        async def run():
            with patch("commissioning.services.goodreads_service.GoodreadsScraper.fetch_book", side_effect=RuntimeError("offline")):
                response = await self._request(
                    "POST",
                    f"/api/books/{book_id}/goodreads/accept",
                    json={
                        "url": "https://www.goodreads.com/book/show/123-review-candidate",
                        "title": "Review Candidate",
                        "author": "Jane Writer",
                        "rating": "4.22",
                        "rating_count": "4567",
                        "pages": "320",
                        "published_year": "2026",
                        "publisher": "Pocket House",
                        "score": 0.86,
                    },
                )
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["goodreads_rating"], "4.22")
            self.assertEqual(payload["goodreads_rating_count"], "4567")
            self.assertEqual(payload["provenance_json"]["goodreads"]["Goodreads Match Status"], "accepted")

        import asyncio

        asyncio.run(run())

    def test_amazon_see_more_asins_expand_without_listing_fetch(self):
        url = "https://www.amazon.com/amz-books/seeMore/?asins=B0FKTTYMVG%2C0316569801"

        items = discover_amazon_items(url, max_results=0)

        self.assertEqual([item.asin for item in items], ["B0FKTTYMVG", "0316569801"])
        self.assertEqual(items[0].url, "https://www.amazon.com/dp/B0FKTTYMVG")
        self.assertEqual(items[1].url, "https://www.amazon.com/dp/0316569801")


if __name__ == "__main__":
    unittest.main()

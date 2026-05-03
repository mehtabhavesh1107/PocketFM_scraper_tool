from __future__ import annotations

import io
import os
import sys
import tempfile
import unittest
from pathlib import Path

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
from commissioning.models import Base, Batch, Book, Contact
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
                    {"source_type": "goodreads", "url": "https://www.goodreads.com/list/show/1", "max_results": 0},
                ],
            )
            self.assertEqual(sources.status_code, 200)
            self.assertEqual(len(sources.json()), 2)

            schema = await self._request("GET", "/api/reference-schema")
            self.assertEqual(schema.status_code, 200)
            self.assertIn("Title", [field["label"] for field in schema.json()["fields"]])

        import asyncio

        asyncio.run(run())

    def test_missing_batch_is_auto_created_for_cloud_storage_recovery(self):
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
            word_count=90000,
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

    def test_amazon_see_more_asins_expand_without_listing_fetch(self):
        url = "https://www.amazon.com/amz-books/seeMore/?asins=B0FKTTYMVG%2C0316569801"

        items = discover_amazon_items(url, max_results=0)

        self.assertEqual([item.asin for item in items], ["B0FKTTYMVG", "0316569801"])
        self.assertEqual(items[0].url, "https://www.amazon.com/dp/B0FKTTYMVG")
        self.assertEqual(items[1].url, "https://www.amazon.com/dp/0316569801")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import csv
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

TEST_ROOT = Path(tempfile.gettempdir()) / f"pocketfm_commissioning_tests_{os.getpid()}"
os.environ["COMMISSIONING_DATA_DIR"] = str(TEST_ROOT / "backend_data")
os.environ["COMMISSIONING_GENERATED_DIR"] = str(TEST_ROOT / "generated")
os.environ["COMMISSIONING_DATABASE_URL"] = f"sqlite:///{(TEST_ROOT / 'backend_data' / 'commissioning.db').as_posix()}"

from commissioning.db import SessionLocal, engine, init_db
from commissioning.jobs.tasks import run_fast_scrape_job, run_scrape_job
from commissioning.models import Base, Batch, Book, ExportRecord, Job, JobEvent, SourceLink
from commissioning.services.amazon_http import AmazonItem
from commissioning.services.export_service import SAMPLE_COMPATIBLE_COLUMNS


class PipelineSmokeTests(unittest.TestCase):
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

    def test_scrape_auto_maps_goodreads_hours_and_export(self):
        db = SessionLocal()
        batch = Batch(name="Pipeline Smoke", status="active")
        db.add(batch)
        db.flush()
        source = SourceLink(
            batch_id=batch.id,
            source_type="amazon",
            url="https://www.amazon.com/amz-books/seeMore/?asins=B0SMOKE001%2CB0SMOKE002&category=Mystery%2C+Thriller+%26+Suspense",
            max_results=0,
        )
        job = Job(batch_id=batch.id, stage="scrape", status="queued")
        db.add_all([source, job])
        db.commit()
        batch_id = batch.id
        job_id = job.id
        db.close()

        records = [
            {
                "title": "Smoke Trail",
                "author": "Ava North, Co Author",
                "url": "https://www.amazon.com/Smoke-Trail-ebook/dp/B0SMOKEK01/ref=tmm_kin_swatch_0",
                "amazon_url": "https://www.amazon.com/Smoke-Trail-ebook/dp/B0SMOKEK01/ref=tmm_kin_swatch_0",
                "rating": 4.5,
                "rating_count": 1200,
                "customer_reviews": "4.5 out of 5 stars; 1200 ratings",
                "publisher": "Smoke House",
                "publication_date": "April 30, 2026",
                "part_of_series": "Smoke Trail",
                "language": "English",
                "best_sellers_rank": "1",
                "best_sellers_rank_number": "1",
                "best_sellers_rank_text": "#1 in Smoke Tests",
                "print_length": "276",
                "book_number": "1",
                "format": "Kindle",
                "synopsis": "A controlled scraper record.",
                "genre": "Domestic Thrillers",
                "cleaned_series_name": "Smoke Trail",
                "series_flag": "Y",
                "source_asin": "B0SMOKE001",
                "detail_asin": "B0SMOKEK01",
                "detail_url": "https://www.amazon.com/Smoke-Trail-ebook/dp/B0SMOKEK01/ref=tmm_kin_swatch_0",
                "source_format": "Audiobook",
                "detail_format": "Kindle",
                "contributors": [
                    {"name": "Ava North", "role": "Author"},
                    {"name": "Co Author", "role": "Author"},
                    {"name": "Narrator One", "role": "Narrator"},
                ],
                "amazon_quality_flags": [],
                "source_payload": {
                    "asin": "B0SMOKE001",
                    "source_asin": "B0SMOKE001",
                    "detail_asin": "B0SMOKEK01",
                    "detail_url": "https://www.amazon.com/Smoke-Trail-ebook/dp/B0SMOKEK01/ref=tmm_kin_swatch_0",
                    "used_format_switch": True,
                    "customer_reviews": "4.5 out of 5 stars; 1200 ratings",
                    "contributors": [
                        {"name": "Ava North", "role": "Author"},
                        {"name": "Co Author", "role": "Author"},
                        {"name": "Narrator One", "role": "Narrator"},
                    ],
                },
            },
            {
                "title": "Smoke Trail Two",
                "author": "Ava North",
                "url": "https://www.amazon.com/dp/B0SMOKE002",
                "amazon_url": "https://www.amazon.com/dp/B0SMOKE002",
                "rating": 4.4,
                "rating_count": 900,
                "customer_reviews": "4.4 out of 5 stars; 900 ratings",
                "publisher": "Smoke House",
                "publication_date": "April 30, 2026",
                "part_of_series": "Smoke Trail",
                "language": "English",
                "best_sellers_rank": "2",
                "best_sellers_rank_number": "2",
                "best_sellers_rank_text": "#2 in Smoke Tests",
                "print_length": "250",
                "book_number": "2",
                "format": "Kindle",
                "synopsis": "A second controlled scraper record.",
                "genre": "Domestic Thrillers",
                "cleaned_series_name": "Smoke Trail",
                "series_flag": "Y",
                "source_asin": "B0SMOKE002",
                "detail_asin": "B0SMOKE002",
                "detail_url": "https://www.amazon.com/dp/B0SMOKE002",
                "source_format": "Kindle",
                "detail_format": "Kindle",
                "contributors": [{"name": "Ava North", "role": "Author"}],
                "amazon_quality_flags": [],
                "source_payload": {
                    "asin": "B0SMOKE002",
                    "source_asin": "B0SMOKE002",
                    "detail_asin": "B0SMOKE002",
                    "detail_url": "https://www.amazon.com/dp/B0SMOKE002",
                    "used_format_switch": False,
                    "customer_reviews": "4.4 out of 5 stars; 900 ratings",
                },
            },
        ]

        items = [
            AmazonItem(
                asin=record["source_asin"],
                title=record["title"],
                author=record["author"],
                url=record["source_payload"].get("source_url") or record["url"],
                raw={"source": "test"},
            )
            for record in records
        ]
        records_by_asin = {record["source_asin"]: record for record in records}

        def fake_discover_items(url: str, max_results: int = 0):
            self.assertEqual(max_results, 0)
            self.assertIn("asins=", url)
            return items

        def fake_fetch_item_record(item_payload: dict):
            return records_by_asin[item_payload["asin"]]

        def fake_enrich(row: dict, scraper):
            slug = row["Title"].lower().replace(" ", "-")
            return {
                "Goodread Link": f"https://www.goodreads.com/search?q={row['Title']}+{row['Author']}",
                "Resolved Goodreads Book": f"https://www.goodreads.com/book/show/{slug}",
                "Series Book 1": "https://www.goodreads.com/book/show/smoke-trail-book-1",
                "Series Link": "https://www.goodreads.com/series/smoke-trail",
                "# of primary book": "8",
                "# of total pages in series": "2541",
                "GR Book 1 Rating": "4.20",
                "Goodreads rating": "4.31",
                "Goodreads no of rating": "6978",
                "Goodreads Match Status": "matched",
                "Goodreads Match Confidence": 0.98,
            }

        fake_enrich_mock = Mock(side_effect=fake_enrich)

        with (
            patch("commissioning.jobs.tasks.discover_amazon_items", side_effect=fake_discover_items),
            patch("commissioning.jobs.tasks.fetch_amazon_item_record", side_effect=fake_fetch_item_record),
            patch("commissioning.jobs.tasks.create_scraper", return_value=object()),
            patch("commissioning.jobs.tasks.enrich_row", side_effect=fake_enrich_mock),
        ):
            run_scrape_job(job_id, batch_id)

        db = SessionLocal()
        completed = db.get(Job, job_id)
        books = db.query(Book).filter(Book.batch_id == batch_id).order_by(Book.id.asc()).all()
        export = db.query(ExportRecord).filter(ExportRecord.batch_id == batch_id).order_by(ExportRecord.id.desc()).first()
        self.assertEqual(completed.status, "completed")
        self.assertIn("Discovered 2 books", completed.message)
        self.assertIn("Goodreads matched for 2 books", completed.message)
        event_messages = [
            row[0]
            for row in db.query(JobEvent.message)
            .filter(JobEvent.job_id == job_id)
            .order_by(JobEvent.id.asc())
            .all()
        ]
        self.assertTrue(any("Found 2 Amazon books" in message for message in event_messages))
        self.assertTrue(any("Fetched Amazon details 1/2" in message for message in event_messages))
        self.assertTrue(any("Amazon core coverage: publisher 2/2" in message for message in event_messages))
        self.assertEqual(fake_enrich_mock.call_count, 2)
        self.assertEqual(len(books), 2)
        self.assertTrue(all(book.goodread_link for book in books))
        self.assertNotEqual(books[0].goodread_link, books[1].goodread_link)
        self.assertTrue(all(book.genre == "Thriller" for book in books))
        self.assertTrue(all(book.sub_genre == "Domestic Thrillers" for book in books))
        self.assertTrue(all(book.book_type == "Series" for book in books))
        self.assertTrue(all((book.audio_score or 0) > 0 for book in books))
        smoke_trail = next(book for book in books if book.title == "Smoke Trail")
        self.assertEqual(smoke_trail.clean_author_names, "Ava North")
        self.assertEqual((smoke_trail.provenance_json or {}).get("amazon", {}).get("source_asin"), "B0SMOKE001")
        self.assertEqual((smoke_trail.provenance_json or {}).get("amazon", {}).get("detail_asin"), "B0SMOKEK01")
        self.assertEqual(smoke_trail.total_pages_in_series, "2541")
        self.assertEqual(smoke_trail.total_word_count, "635250")
        self.assertEqual(smoke_trail.total_hours, "64")
        self.assertIsNotNone(export)
        self.assertEqual(export.row_count, 2)
        export_path = Path(export.file_path)
        self.assertTrue(export_path.exists())
        header = export_path.read_text(encoding="utf-8").splitlines()[0]
        self.assertEqual(header.split(","), SAMPLE_COMPATIBLE_COLUMNS)
        self.assertIn("# Total word count", header)
        self.assertIn("Goodread Link", header)
        self.assertIn("Duplicate Check", header)
        self.assertNotIn("Sub-genre", header)
        self.assertNotIn("Data Quality Issues", header)
        with export_path.open("r", encoding="utf-8", newline="") as handle:
            exported = list(csv.DictReader(handle))
        exported_by_title = {row["Title"]: row for row in exported}
        self.assertEqual(
            exported_by_title["Smoke Trail"]["URL"],
            "https://www.amazon.com/Smoke-Trail-ebook/dp/B0SMOKEK01/ref=tmm_kin_swatch_0",
        )
        self.assertEqual(exported_by_title["Smoke Trail"]["Publisher"], "Smoke House")
        self.assertEqual(exported_by_title["Smoke Trail"]["Best Sellers Rank"], "1")
        self.assertEqual(exported_by_title["Smoke Trail"]["Customer Reviews"], "4.5 out of 5 stars; 1200 ratings")
        self.assertEqual(exported_by_title["Smoke Trail"]["Goodreads rating"], "4.31")
        db.close()

    def test_fast_scrape_skips_goodreads_but_exports_amazon_details(self):
        db = SessionLocal()
        batch = Batch(name="Fast Smoke", status="active")
        db.add(batch)
        db.flush()
        source = SourceLink(
            batch_id=batch.id,
            source_type="amazon",
            url="https://www.amazon.com/amz-books/seeMore/?asins=B0FAST0001",
            max_results=0,
        )
        job = Job(batch_id=batch.id, stage="fast_scrape", status="queued")
        db.add_all([source, job])
        db.commit()
        batch_id = batch.id
        job_id = job.id
        db.close()

        records = [
            {
                "title": "Fast Accurate",
                "author": "Ava North",
                "url": "https://www.amazon.com/dp/B0FAST0001",
                "amazon_url": "https://www.amazon.com/dp/B0FAST0001",
                "rating": 4.6,
                "rating_count": 120,
                "customer_reviews": "4.6 out of 5 stars; 120 ratings",
                "publisher": "Fast House",
                "publication_date": "May 1, 2026",
                "part_of_series": "Fast Accurate",
                "language": "English",
                "best_sellers_rank": "1",
                "best_sellers_rank_number": "1",
                "print_length": "300",
                "book_number": "1",
                "format": "Kindle",
                "synopsis": "Full Amazon details are still fetched.",
                "genre": "Domestic Thrillers",
                "cleaned_series_name": "Fast Accurate",
                "series_flag": "Y",
                "source_asin": "B0FAST0001",
                "detail_asin": "B0FAST0001",
                "detail_url": "https://www.amazon.com/dp/B0FAST0001",
                "source_format": "Kindle",
                "detail_format": "Kindle",
                "contributors": [{"name": "Ava North", "role": "Author"}],
                "amazon_quality_flags": [],
                "source_payload": {
                    "asin": "B0FAST0001",
                    "source_asin": "B0FAST0001",
                    "detail_asin": "B0FAST0001",
                    "detail_url": "https://www.amazon.com/dp/B0FAST0001",
                    "detail_fetched": True,
                },
            }
        ]

        items = [
            AmazonItem(
                asin=records[0]["source_asin"],
                title=records[0]["title"],
                author=records[0]["author"],
                url=records[0]["url"],
                raw={"source": "test"},
            )
        ]

        def fake_discover_items(url: str, max_results: int = 0):
            return items

        def fake_fetch_item_record(item_payload: dict):
            return records[0]

        with (
            patch("commissioning.jobs.tasks.discover_amazon_items", side_effect=fake_discover_items),
            patch("commissioning.jobs.tasks.fetch_amazon_item_record", side_effect=fake_fetch_item_record),
            patch("commissioning.jobs.tasks.create_scraper", side_effect=AssertionError("Goodreads should not run")),
            patch("commissioning.jobs.tasks.enrich_row", side_effect=AssertionError("Goodreads should not run")),
        ):
            run_fast_scrape_job(job_id, batch_id)

        db = SessionLocal()
        completed = db.get(Job, job_id)
        books = db.query(Book).filter(Book.batch_id == batch_id).order_by(Book.id.asc()).all()
        export = db.query(ExportRecord).filter(ExportRecord.batch_id == batch_id).order_by(ExportRecord.id.desc()).first()
        self.assertEqual(completed.status, "completed")
        self.assertIn("Discovered 1 books", completed.message)
        self.assertIn("Goodreads mapping skipped for fast mode", completed.message)
        self.assertEqual(len(books), 1)
        self.assertEqual(books[0].publisher, "Fast House")
        self.assertEqual(books[0].goodreads_rating, "")
        self.assertIsNotNone(export)
        self.assertEqual(export.row_count, 1)
        db.close()


if __name__ == "__main__":
    unittest.main()

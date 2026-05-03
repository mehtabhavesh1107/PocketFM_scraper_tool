from __future__ import annotations

import csv
import os
import sys
import tempfile
import unittest
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

TEST_ROOT = Path(tempfile.gettempdir()) / f"pocketfm_commissioning_tests_{os.getpid()}"
os.environ["COMMISSIONING_DATA_DIR"] = str(TEST_ROOT / "backend_data")
os.environ["COMMISSIONING_GENERATED_DIR"] = str(TEST_ROOT / "generated")
os.environ["COMMISSIONING_DATABASE_URL"] = f"sqlite:///{(TEST_ROOT / 'backend_data' / 'commissioning.db').as_posix()}"

from commissioning.db import SessionLocal, engine, init_db  # noqa: E402
from commissioning.models import Base, Batch, Book  # noqa: E402
from commissioning.services.export_service import SAMPLE_COMPATIBLE_COLUMNS, generate_export  # noqa: E402


class ExportProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Base.metadata.drop_all(bind=engine)
        init_db()

    def setUp(self):
        Base.metadata.drop_all(bind=engine)
        init_db()

    def test_sample_profile_uses_exact_sample_schema(self):
        db = SessionLocal()
        try:
            batch = Batch(name="Export Profile")
            db.add(batch)
            db.flush()
            db.add(
                Book(
                    batch_id=batch.id,
                    title="Clean Book",
                    author="Jane Writer",
                    url="https://www.amazon.com/dp/B0CLEAN001",
                    amazon_url="https://www.amazon.com/dp/B0CLEAN001",
                    rating=4.2,
                    rating_count=1234,
                    publisher="Example Press",
                    publication_date="May 1, 2026",
                    language="English",
                    best_sellers_rank="12",
                    print_length="321",
                    format="Kindle",
                    synopsis="A clean export row.",
                    genre="Thriller",
                    sub_genre="Domestic Thrillers",
                    goodreads_rating="4.10",
                    goodreads_rating_count="4321",
                    provenance_json={
                        "amazon": {
                            "source_asin": "B0CLEAN001",
                            "detail_asin": "B0CLEAN001",
                            "best_sellers_rank_text": "#12 in Domestic Thrillers",
                            "normalized": {"genre": "Domestic Thrillers"},
                        }
                    },
                )
            )
            db.commit()

            export = generate_export(db, batch, "csv", profile="sample_compatible")
            with open(export.file_path, newline="", encoding="utf-8") as handle:
                reader = csv.reader(handle)
                headers = next(reader)

            self.assertEqual(headers, SAMPLE_COMPATIBLE_COLUMNS)
            self.assertEqual(export.metadata_json["profile"], "sample_compatible")
            self.assertEqual(export.metadata_json["quality_summary"]["total"], 1)
        finally:
            db.close()

    def test_diagnostic_profile_includes_quality_columns(self):
        db = SessionLocal()
        try:
            batch = Batch(name="Diagnostic Profile")
            db.add(batch)
            db.flush()
            db.add(Book(batch_id=batch.id, title="B0BAD00001", author=""))
            db.commit()

            export = generate_export(db, batch, "csv", profile="full_diagnostic")
            with open(export.file_path, newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                row = next(reader)

            self.assertIn("Data Quality Issues", reader.fieldnames or [])
            self.assertIn("placeholder_title", row["Data Quality Issues"])
            self.assertGreater(export.metadata_json["quality_summary"]["critical_count"], 0)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()

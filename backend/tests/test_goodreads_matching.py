from __future__ import annotations

import sys
import unittest
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from commissioning.services.goodreads_scraper import GoodreadsScraper  # noqa: E402


class StubGoodreadsScraper(GoodreadsScraper):
    def __init__(self, pages: dict[str, str]):
        super().__init__()
        self.pages = pages
        self.requested: list[str] = []

    def _fetch_html(self, url: str) -> str:
        self.requested.append(url)
        for key, html in self.pages.items():
            if key in url:
                return html
        return "<html><body></body></html>"


def search_page(*hrefs: str) -> str:
    links = "".join(f'<a href="{href}">candidate</a>' for href in hrefs)
    return f"<html><body>{links}</body></html>"


def book_page(
    *,
    title: str,
    author: str,
    rating: str = "4.21",
    rating_count: str = "12345",
    pages: str = "352",
    published: str = "2026-05-04",
    publisher: str = "Little, Brown and Company",
    isbn: str = "0316569801",
) -> str:
    return f"""
    <html>
      <head>
        <script type="application/ld+json">
        {{
          "@type": "Book",
          "name": "{title}",
          "author": {{"@type": "Person", "name": "{author}"}},
          "aggregateRating": {{"ratingValue": "{rating}", "ratingCount": "{rating_count}"}},
          "numberOfPages": "{pages}",
          "datePublished": "{published}",
          "publisher": {{"@type": "Organization", "name": "{publisher}"}},
          "isbn": "{isbn}"
        }}
        </script>
      </head>
      <body><a href="/series/123-women-s-murder-club">Women's Murder Club</a></body>
    </html>
    """


class GoodreadsMatchingTests(unittest.TestCase):
    def test_isbn_search_produces_confident_match_with_ratings(self):
        scraper = StubGoodreadsScraper(
            {
                "search?q=0316569801": search_page("/book/show/123-26-beauties"),
                "/book/show/123-26-beauties": book_page(
                    title="26 Beauties: A Women's Murder Club Thriller",
                    author="James Patterson",
                    isbn="0316569801",
                ),
                "/series/123-women-s-murder-club": """
                    <html><body>
                    26 primary works
                    <a href="/book/show/123-26-beauties">26 Beauties</a>
                    <div>352 pages 4.21 avg rating 12,345 ratings</div>
                    </body></html>
                """,
            }
        )

        updates = scraper.resolve_row(
            {
                "Title": "26 Beauties: A Women's Murder Club Thriller",
                "Author": "James Patterson",
                "ISBN-10": "0316569801",
                "Publisher": "Little, Brown and Company",
                "Publication date": "May 4, 2026",
                "Print Length": "352",
                "Part of series": "Women's Murder Club",
            }
        )

        self.assertEqual(updates["Goodreads Match Status"], "matched")
        self.assertEqual(updates["Goodreads Match Method"], "isbn_search")
        self.assertGreaterEqual(updates["Goodreads Match Confidence"], 0.72)
        self.assertEqual(updates["Goodreads rating"], "4.21")
        self.assertEqual(updates["Goodreads no of rating"], "12345")
        self.assertIn("/book/show/123-26-beauties", updates["Resolved Goodreads Book"])

    def test_low_confidence_candidates_are_returned_for_review(self):
        scraper = StubGoodreadsScraper(
            {
                "search?q=Jane+Writer+Midnight+Witness": search_page("/book/show/456-midnight-witness-summary"),
                "/book/show/456-midnight-witness-summary": book_page(
                    title="Summary of Midnight Witness",
                    author="Jane Writer",
                    rating="3.1",
                    rating_count="12",
                    pages="50",
                    published="2020-01-01",
                    publisher="Study Notes",
                    isbn="",
                ),
            }
        )

        updates = scraper.resolve_row(
            {
                "Title": "Midnight Witness",
                "Author": "Jane Writer",
                "Publisher": "Pocket House",
                "Publication date": "2026",
                "Print Length": "310",
            }
        )

        self.assertEqual(updates["Goodreads Match Status"], "review")
        self.assertGreater(len(updates["Goodreads Candidates"]), 0)
        self.assertIn("manual review", updates["Goodreads Match Reason"])


if __name__ == "__main__":
    unittest.main()

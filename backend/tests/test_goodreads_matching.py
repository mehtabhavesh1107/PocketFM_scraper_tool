from __future__ import annotations

import json
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
    author: str | list[str],
    rating: str = "4.21",
    rating_count: str = "12345",
    pages: str = "352",
    published: str = "2026-05-04",
    publisher: str = "Little, Brown and Company",
    isbn: str = "0316569801",
) -> str:
    if isinstance(author, list):
        author_payload = json.dumps([{"@type": "Person", "name": name} for name in author])
    else:
        author_payload = json.dumps({"@type": "Person", "name": author})
    return f"""
    <html>
      <head>
        <script type="application/ld+json">
        {{
          "@type": "Book",
          "name": "{title}",
          "author": {author_payload},
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

    def test_low_confidence_candidates_are_not_auto_matched(self):
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

        self.assertIn(updates["Goodreads Match Status"], {"review", "unmatched"})
        self.assertNotIn("Resolved Goodreads Book", updates)
        self.assertGreater(len(updates["Goodreads Candidates"]), 0)
        self.assertIn("Best Goodreads candidate scored", updates["Goodreads Match Reason"])

    def test_title_search_can_override_unconfirmed_isbn_candidate(self):
        scraper = StubGoodreadsScraper(
            {
                "search?q=9781837901319": search_page("/book/show/999-the-locked-door"),
                "Housemaid%27s+Secret": search_page("/book/show/91975625-the-housemaid-s-secret"),
                "/book/show/999-the-locked-door": book_page(
                    title="The Locked Door",
                    author="Freida McFadden",
                    rating="4.02",
                    rating_count="98765",
                    pages="320",
                    published="2021-11-01",
                    publisher="Hollywood Upstairs Press",
                    isbn="",
                ),
                "/book/show/91975625-the-housemaid-s-secret": book_page(
                    title="The Housemaid's Secret",
                    author="Freida McFadden",
                    rating="4.19",
                    rating_count="345678",
                    pages="352",
                    published="2023-02-20",
                    publisher="Bookouture",
                    isbn="9781837901319",
                ),
            }
        )

        updates = scraper.resolve_row(
            {
                "Title": "The Housemaid's Secret",
                "Author": "Freida McFadden",
                "ISBN-13": "9781837901319",
                "Publisher": "Bookouture",
                "Publication date": "2023",
                "Print Length": "352",
                "Part of series": "The Housemaid",
            }
        )

        self.assertEqual(updates["Goodreads Match Status"], "matched")
        self.assertIn("/book/show/91975625-the-housemaid-s-secret", updates["Resolved Goodreads Book"])
        self.assertNotIn("the-locked-door", updates["Resolved Goodreads Book"])
        self.assertEqual(updates["Goodreads rating"], "4.19")

    def test_unconfirmed_isbn_candidate_with_wrong_title_is_not_auto_matched(self):
        scraper = StubGoodreadsScraper(
            {
                "search?q=9781250301697": search_page("/book/show/40611328-the-last-time-i-lied"),
                "/book/show/40611328-the-last-time-i-lied": book_page(
                    title="The Last Time I Lied",
                    author="Riley Sager",
                    rating="4.09",
                    rating_count="100000",
                    pages="384",
                    published="2018-07-03",
                    publisher="Dutton",
                    isbn="",
                ),
            }
        )

        updates = scraper.resolve_row(
            {
                "Title": "The Silent Patient",
                "Author": "Alex Michaelides",
                "ISBN-13": "9781250301697",
                "Publisher": "Celadon Books",
                "Publication date": "2019",
                "Print Length": "336",
            }
        )

        self.assertIn(updates["Goodreads Match Status"], {"review", "unmatched"})
        self.assertNotIn("Resolved Goodreads Book", updates)
        self.assertIn("title match", updates["Goodreads Match Reason"])

    def test_coauthor_match_accepts_requested_author_when_not_first_on_goodreads(self):
        scraper = StubGoodreadsScraper(
            {
                "judge+stone+James+Patterson": search_page("/book/show/242482097-judge-stone"),
                "/book/show/242482097-judge-stone": book_page(
                    title="Judge Stone",
                    author=["Viola Davis", "James Patterson"],
                    rating="4.52",
                    rating_count="17410",
                    pages="425",
                    published="2026-03-09",
                    publisher="Little, Brown and Company",
                    isbn="0316579831",
                ),
            }
        )

        updates = scraper.resolve_row(
            {
                "Title": "Judge Stone: A Novel",
                "Author": "James Patterson",
                "Publisher": "Little, Brown and Company",
                "Publication date": "2026",
                "Print Length": "425",
            }
        )

        self.assertEqual(updates["Goodreads Match Status"], "matched")
        self.assertIn("/book/show/242482097-judge-stone", updates["Resolved Goodreads Book"])
        self.assertEqual(updates["Goodreads no of rating"], "17410")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from bs4 import BeautifulSoup

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from commissioning.services.amazon_http import (  # noqa: E402
    AmazonDetail,
    AmazonItem,
    AmazonScrapeError,
    clean_amazon_value,
    discover_amazon_items,
    discover_amazon_records,
    fetch_amazon_detail,
    parse_key_values,
    parse_media_matrix_links,
    to_record,
)


AUDIBLE_HTML = """
<html>
  <head><title>Amazon.com: Switch Book (Audible Audio Edition): Jane Writer, Voice Actor: Audible Books</title></head>
  <body>
    <span id="productTitle">Switch Book</span>
    <div id="bylineInfo">
      <span class="author"><a class="a-link-normal">Jane Writer</a><span class="contribution">(Author)</span></span>
      <span class="author"><a class="a-link-normal">Voice Actor</a><span class="contribution">(Narrator)</span></span>
    </div>
    <span id="acrPopover" title="4.4 out of 5 stars"><span class="a-icon-alt">4.4 out of 5 stars</span></span>
    <span id="acrCustomerReviewText">(1,234)</span>
    <div class="swatchElement selected" id="tmm-grid-swatch-AUDIO_DOWNLOAD">
      <span class="slot-title"><span>Audiobook</span></span>
      <a href="javascript:void(0)">Audiobook</a>
    </div>
    <div class="swatchElement" id="tmm-grid-swatch-KINDLE">
      <a href="/Switch-Book-Jane-Writer-ebook/dp/B0KINDLE01/ref=tmm_kin_swatch_0">
        <span class="slot-title"><span>Kindle</span></span>
      </a>
    </div>
    <div data-rpi-attribute-name="audiobook_details-listening_length">
      <div class="rpi-attribute-label"><span>Listening Length</span></div>
      <div class="rpi-attribute-value"><span>9 hours and 1 minute</span></div>
    </div>
    <div data-rpi-attribute-name="audiobook_details-program_type">
      <div class="rpi-attribute-label"><span>Program Type</span></div>
      <div class="rpi-attribute-value"><span>Audiobook</span></div>
    </div>
  </body>
</html>
"""


KINDLE_HTML = """
<html>
  <head><title>Amazon.com: Switch Book eBook : Writer, Jane: Kindle Store</title></head>
  <body>
    <span id="productTitle">Switch Book</span>
    <div id="bylineInfo">
      <span class="author"><a class="a-link-normal">Jane Writer</a><span class="contribution">(Author)</span></span>
    </div>
    <span id="acrPopover" title="4.5 out of 5 stars"><span class="a-icon-alt">4.5 out of 5 stars</span></span>
    <span id="acrCustomerReviewText">(1,250)</span>
    <div class="swatchElement selected" id="tmm-grid-swatch-KINDLE">
      <span class="slot-title"><span>Kindle INR 199.00 Available instantly</span></span>
      <a href="javascript:void(0)">Kindle</a>
    </div>
    <div data-rpi-attribute-name="book_details-fiona_pages">
      <div class="rpi-attribute-label"><span>Print length</span></div>
      <div class="rpi-attribute-value"><span>321 pages</span></div>
    </div>
    <div data-rpi-attribute-name="language">
      <div class="rpi-attribute-label"><span>Language</span></div>
      <div class="rpi-attribute-value"><span>English</span></div>
    </div>
    <div data-rpi-attribute-name="book_details-publication_date">
      <div class="rpi-attribute-label"><span>Publication date</span></div>
      <div class="rpi-attribute-value"><span>April 7, 2026</span></div>
    </div>
    <div data-rpi-attribute-name="book_details-publisher">
      <div class="rpi-attribute-label"><span>Publisher</span></div>
      <div class="rpi-attribute-value"><span>Knopf</span></div>
    </div>
    <div id="detailBullets_feature_div">
      <ul>
        <li><span class="a-list-item"><span class="a-text-bold">Best Sellers Rank:</span> #2 in Kindle Store (See Top 100) #1 in Suspense Thrillers</span></li>
        <li><span class="a-list-item"><span class="a-text-bold">Customer Reviews:</span> 4.5 out of 5 stars (1,250)</span></li>
      </ul>
    </div>
    <div id="bookDescription_feature_div"><div class="a-expander-content">A clean book description. Read more</div></div>
  </body>
</html>
"""


RAW_SEARCH_PAGE = """
<html>
  <body>
    <raw-html data-payload="&lt;span data-csa-c-item-id=&quot;amzn1.asin.B0RAW00001&quot;&gt;
      &lt;div data-cy=&quot;asin-faceout-container&quot;&gt;
        &lt;a href=&quot;/Raw-Book-ebook/dp/B0RAW00001/ref=lp_1_1&quot;&gt;
          &lt;h2 aria-label=&quot;Raw Payload Book&quot;&gt;&lt;span&gt;Raw Payload Book&lt;/span&gt;&lt;/h2&gt;
        &lt;/a&gt;
        &lt;div class=&quot;a-row a-size-base a-color-secondary&quot;&gt;by Jane Raw | Jan 1, 2026&lt;/div&gt;
      &lt;/div&gt;
    &lt;/span&gt;" executable="false"></raw-html>
  </body>
</html>
"""


NORMAL_SEARCH_PAGE = """
<html>
  <body>
    <div data-component-type="s-search-result" data-asin="B0NEXT0001">
      <a class="a-link-normal" href="/Wrong-Format/dp/B0IMAGE001/ref=image"><img alt="Wrong image link" /></a>
      <a class="a-link-normal" href="/Next-Book-ebook/dp/B0NEXT0001/ref=sr_1_2">
        <h2><span>Next Page Book</span></h2>
      </a>
      <div data-cy="title-recipe">
        <div class="a-row a-size-base a-color-secondary">by Jane Next | Feb 1, 2026</div>
      </div>
    </div>
  </body>
</html>
"""


class AmazonHttpParserTests(unittest.TestCase):
    def test_search_parser_reads_raw_html_cards_and_constructs_page_two(self):
        seen_urls = []

        def fake_fetch(url: str, *, retries: int = 2) -> str:
            seen_urls.append(url)
            if "page=2" in url:
                return NORMAL_SEARCH_PAGE
            return RAW_SEARCH_PAGE

        with patch("commissioning.services.amazon_http._fetch", side_effect=fake_fetch):
            items = discover_amazon_items("https://www.amazon.com/s?bbn=9069934011&rh=n%3A9089889011", max_results=2)

        self.assertEqual([item.asin for item in items], ["B0RAW00001", "B0NEXT0001"])
        self.assertEqual(items[0].title, "Raw Payload Book")
        self.assertEqual(items[0].author, "Jane Raw")
        self.assertIn("page=2", seen_urls[1])
        self.assertIn("B0NEXT0001", items[1].url)
        self.assertNotIn("B0IMAGE001", items[1].url)

    def test_rpi_and_hidden_unicode_values_are_cleaned(self):
        soup = BeautifulSoup(
            """
            <div data-rpi-attribute-name="book_details-publisher">
              <div class="rpi-attribute-label"><span>Publisher</span></div>
              <div class="rpi-attribute-value"><span>Knopf</span></div>
            </div>
            <div id="detailBullets_feature_div">
              <li><span class="a-list-item"><span class="a-text-bold">Publication date &rlm; : &lrm; </span>April 7, 2026</span></li>
            </div>
            """,
            "html.parser",
        )

        values = parse_key_values(soup)

        self.assertEqual(values["Publisher"], "Knopf")
        self.assertEqual(values["Publication date"], "April 7, 2026")
        self.assertEqual(clean_amazon_value("\u200f : \u200e Grand Central Publishing"), "Grand Central Publishing")

    def test_media_matrix_prefers_clean_format_labels(self):
        soup = BeautifulSoup(AUDIBLE_HTML, "html.parser")

        links = parse_media_matrix_links(soup, "https://www.amazon.com/dp/B0SOURCE01")

        self.assertEqual(links[0]["format"], "Audiobook")
        self.assertEqual(links[1]["format"], "Kindle")
        self.assertEqual(links[1]["url"], "https://www.amazon.com/Switch-Book-Jane-Writer-ebook/dp/B0KINDLE01/ref=tmm_kin_swatch_0")

    def test_audible_source_switches_to_kindle_metadata(self):
        def fake_fetch(url: str, *, retries: int = 2) -> str:
            if "B0KINDLE01" in url:
                return KINDLE_HTML
            return AUDIBLE_HTML

        with patch("commissioning.services.amazon_http._fetch", side_effect=fake_fetch):
            detail = fetch_amazon_detail("https://www.amazon.com/dp/B0SOURCE01")

        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertTrue(detail.used_format_switch)
        self.assertEqual(detail.source_asin, "B0SOURCE01")
        self.assertEqual(detail.detail_asin, "B0KINDLE01")
        self.assertEqual(detail.source_format, "Audiobook")
        self.assertEqual(detail.format, "Kindle")
        self.assertEqual(detail.publisher, "Knopf")
        self.assertEqual(detail.publication_date, "April 7, 2026")
        self.assertEqual(detail.best_sellers_rank_number, "2")
        self.assertEqual(detail.print_length, "321")
        self.assertEqual(detail.customer_reviews, "4.5 out of 5 stars; 1250 ratings")
        self.assertIn({"name": "Voice Actor", "role": "Narrator"}, detail.contributors)

        record = to_record(AmazonItem(asin="B0SOURCE01", title="B0SOURCE01", url="https://www.amazon.com/dp/B0SOURCE01"), detail)
        self.assertEqual(record["url"], "https://www.amazon.com/Switch-Book-Jane-Writer-ebook/dp/B0KINDLE01/ref=tmm_kin_swatch_0")
        self.assertEqual(record["best_sellers_rank"], "2")
        self.assertEqual(record["source_payload"]["source_asin"], "B0SOURCE01")
        self.assertEqual(record["source_payload"]["detail_asin"], "B0KINDLE01")

    def test_plain_dp_uses_alternate_detail_route(self):
        calls = []

        def fake_fetch(url: str, *, retries: int = 2) -> str:
            calls.append(url)
            if url in {"https://www.amazon.com/dp/B0SOURCE01", "https://www.amazon.com/Switch-Book-Jane-Writer-ebook/dp/B0KINDLE01/ref=tmm_kin_swatch_0"}:
                raise AmazonScrapeError("blocked")
            if url == "https://www.amazon.com/-/dp/B0SOURCE01":
                return AUDIBLE_HTML
            if url == "https://www.amazon.com/-/dp/B0KINDLE01":
                return KINDLE_HTML
            raise AssertionError(f"unexpected URL {url}")

        with patch("commissioning.services.amazon_http._fetch", side_effect=fake_fetch):
            detail = fetch_amazon_detail("https://www.amazon.com/dp/B0SOURCE01")

        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertEqual(calls[:4], [
            "https://www.amazon.com/dp/B0SOURCE01",
            "https://www.amazon.com/-/dp/B0SOURCE01",
            "https://www.amazon.com/Switch-Book-Jane-Writer-ebook/dp/B0KINDLE01/ref=tmm_kin_swatch_0",
            "https://www.amazon.com/-/dp/B0KINDLE01",
        ])
        self.assertEqual(detail.title, "Switch Book")
        self.assertEqual(detail.detail_asin, "B0KINDLE01")
        self.assertEqual(detail.publisher, "Knopf")

    def test_thin_plain_dp_falls_through_to_rich_route(self):
        calls = []

        def fake_fetch(url: str, *, retries: int = 2) -> str:
            calls.append(url)
            if url == "https://www.amazon.com/dp/B0782X86CP":
                return "<html><head><title>Amazon shell</title></head><body>continue</body></html>"
            if url == "https://www.amazon.com/-/dp/B0782X86CP":
                raise AmazonScrapeError("server error")
            if url == "https://www.amazon.com/gp/aw/d/B0782X86CP":
                return KINDLE_HTML
            raise AssertionError(f"unexpected URL {url}")

        with patch("commissioning.services.amazon_http._fetch", side_effect=fake_fetch):
            detail = fetch_amazon_detail("https://www.amazon.com/dp/B0782X86CP")

        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertEqual(
            calls[:3],
            [
                "https://www.amazon.com/dp/B0782X86CP",
                "https://www.amazon.com/-/dp/B0782X86CP",
                "https://www.amazon.com/gp/aw/d/B0782X86CP",
            ],
        )
        self.assertEqual(detail.title, "Switch Book")
        self.assertEqual(detail.publisher, "Knopf")

    def test_incomplete_asin_detail_is_retried_before_export(self):
        item = AmazonItem(
            asin="B0BAD00001",
            title="B0BAD00001",
            url="https://www.amazon.com/dp/B0BAD00001",
            raw={"source": "query_asins"},
        )
        sparse = AmazonDetail(asin="B0BAD00001", title="B0BAD00001", detail_url=item.url)
        sparse.amazon_quality_flags = []
        complete = AmazonDetail(
            asin="B0BAD00001",
            title="Real Book",
            author="Jane Writer",
            publisher="Knopf",
            publication_date="May 1, 2026",
            print_length="321",
            best_sellers_rank="#42 in Kindle Store",
            best_sellers_rank_number="42",
            synopsis="A complete enough synopsis for retry validation.",
            detail_url=item.url,
        )
        complete.amazon_quality_flags = []

        with (
            patch("commissioning.services.amazon_http.discover_amazon_items", return_value=[item]),
            patch("commissioning.services.amazon_http.AMAZON_DETAIL_WORKERS", 1),
            patch("commissioning.services.amazon_http.AMAZON_DETAIL_RETRY_ROUNDS", 1),
            patch("commissioning.services.amazon_http.AMAZON_DETAIL_RETRY_DELAY_SECONDS", 0),
            patch("commissioning.services.amazon_http.fetch_amazon_detail", side_effect=[sparse, complete]) as fetch_detail,
        ):
            records = list(discover_amazon_records("https://www.amazon.com/amz-books/seeMore/?asins=B0BAD00001"))

        self.assertEqual(fetch_detail.call_count, 2)
        self.assertEqual(records[0]["title"], "Real Book")
        self.assertEqual(records[0]["author"], "Jane Writer")
        self.assertEqual(records[0]["publisher"], "Knopf")
        self.assertEqual(records[0]["best_sellers_rank"], "42")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
import logging
import os
import sys
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from ..settings import BACKEND_DIR, WORKSPACE_ROOT
from .amazon_http import AmazonScrapeError, discover_amazon_records

if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _is_amazon_host(host: str) -> bool:
    normalized = (host or "").split(":", 1)[0].lower()
    return (
        normalized.startswith("amazon.")
        or ".amazon." in normalized
        or normalized == "amzn.com"
        or normalized.endswith(".amzn.com")
    )


def _run_async(coro):
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


def _playwright_amazon_fallback(url: str, max_results: int) -> list[dict]:
    """Browser-based fallback used only if HTTP fetching is blocked.

    Imported lazily so installations without Playwright or its browser binaries
    still work for the HTTP-first path.
    """
    if os.getenv("COMMISSIONING_DISABLE_PLAYWRIGHT_FALLBACK", "").strip().lower() in {"1", "true", "yes"}:
        logger.warning("Playwright fallback disabled; skipping browser scrape for %s", url)
        return []
    try:
        from scraper import AmazonScraper  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        logger.warning("Playwright fallback unavailable: %s", exc)
        return []
    try:
        scraper = AmazonScraper(headless=True)
        results = _run_async(scraper.scrape_bestseller_list(url, limit=max_results))
    except Exception as exc:
        logger.warning("Playwright fallback failed for %s: %s", url, exc)
        return []
    normalized: list[dict] = []
    for item in results or []:
        normalized.append(
            {
                "title": item.get("Book Title", "") or "",
                "author": item.get("Author Name", "") or "",
                "url": item.get("Amazon URL", "") or "",
                "amazon_url": item.get("Amazon URL", "") or "",
                "rating": item.get("Rating"),
                "rating_count": item.get("Number of Reviews"),
                "best_sellers_rank": item.get("Rank", ""),
                "publisher": item.get("Publisher", ""),
                "synopsis": item.get("Description", ""),
                "source_payload": item,
            }
        )
    return normalized


def discover_amazon_books(url: str, max_results: int, *, on_progress: callable | None = None) -> list[dict]:
    """Discover Amazon books from any supported listing URL.

    Strategy: prefer HTTP+BeautifulSoup (fast, no browser), fall back to
    Playwright only if the HTTP layer is challenged (captcha / robot check).
    """
    if not url or not url.strip():
        raise ValueError("Amazon source URL is empty.")
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not _is_amazon_host(parsed.netloc or ""):
        raise ValueError(f"Not an Amazon URL: {url!r}")

    try:
        records = list(discover_amazon_records(url, max_results, on_progress=on_progress))
        if records:
            return records
    except AmazonScrapeError as exc:
        logger.info("HTTP scraper hit anti-bot for %s (%s); trying Playwright fallback", url, exc)
        return _playwright_amazon_fallback(url, max_results)
    except requests.HTTPError as exc:
        raise RuntimeError(f"Amazon returned HTTP {exc.response.status_code} for {url}") from exc
    except requests.RequestException as exc:
        raise RuntimeError(f"Amazon request failed: {exc}") from exc

    # HTTP path returned zero results — try Playwright as a last attempt.
    return _playwright_amazon_fallback(url, max_results)


def discover_goodreads_books(url: str, max_results: int) -> list[dict]:
    """Delegated to the dedicated `goodreads_http` parser (handles list & shelf URLs)."""
    from .goodreads_http import GoodreadsScrapeError, discover_goodreads_records

    if not url or not url.strip():
        raise ValueError("Goodreads source URL is empty.")
    try:
        return list(discover_goodreads_records(url, max_results))
    except GoodreadsScrapeError as exc:
        raise RuntimeError(str(exc)) from exc
    except requests.HTTPError as exc:
        raise RuntimeError(f"Goodreads returned HTTP {exc.response.status_code} for {url}") from exc
    except requests.RequestException as exc:
        raise RuntimeError(f"Goodreads request failed: {exc}") from exc


def discover_books(source_type: str, url: str, max_results: int, *, on_progress: callable | None = None) -> list[dict]:
    if source_type == "amazon":
        return discover_amazon_books(url, max_results, on_progress=on_progress)
    if source_type == "goodreads":
        return discover_goodreads_books(url, max_results)
    raise ValueError(f"Unsupported source_type: {source_type}")

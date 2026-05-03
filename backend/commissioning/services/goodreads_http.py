"""HTTP-based Goodreads list/shelf discovery.

Supports the two URL families the UI exposes:
  * /list/show/<id>.<slug>     - Listopia table of books
  * /shelf/show/<slug>          - Genre shelf grid

Author / title / rating / rating-count are extracted with structured selectors
so the parser doesn't latch onto stray numbers (the original implementation
mistakenly filed the first numeric token in the row as `rating`).
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

REQUEST_TIMEOUT = 25
GOODREADS_ORIGIN = "https://www.goodreads.com"
PAGE_DELAY_SECONDS = 0.5
MAX_PAGES_PER_SOURCE = 100
DEFAULT_RESULT_CEILING = 5000

# Goodreads renders "4.35 avg rating" on /list/show pages and "avg rating 4.09" on shelves.
_RATING_RE = re.compile(r"(?:avg\s*rating[:\s]*)(\d+(?:\.\d+)?)|(\d+(?:\.\d+)?)\s*avg\s*rating", re.IGNORECASE)
_RATING_COUNT_RE = re.compile(r"([\d,]+)\s*ratings", re.IGNORECASE)


class GoodreadsScrapeError(RuntimeError):
    pass


@dataclass
class GoodreadsItem:
    title: str
    author: str = ""
    url: str = ""
    rating: float | None = None
    rating_count: int | None = None
    cover_image: str = ""
    raw: dict = field(default_factory=dict)


def _clean(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _parse_minirating(text: str) -> tuple[float | None, int | None]:
    if not text:
        return None, None
    rating = None
    count = None
    rmatch = _RATING_RE.search(text)
    if rmatch:
        token = rmatch.group(1) or rmatch.group(2)
        try:
            rating = float(token) if token else None
        except ValueError:
            rating = None
    cmatch = _RATING_COUNT_RE.search(text)
    if cmatch:
        try:
            count = int(cmatch.group(1).replace(",", ""))
        except ValueError:
            count = None
    return rating, count


def _fetch(url: str) -> str:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
    if response.status_code in (404, 410):
        raise GoodreadsScrapeError(f"Goodreads returned {response.status_code} for {url}")
    response.raise_for_status()
    return response.text


def _next_page_url(soup: BeautifulSoup, current_url: str) -> str | None:
    for anchor in soup.select("a.next_page, a[rel='next'], .pagination a"):
        href = anchor.get("href", "")
        if not href:
            continue
        classes = {str(item).lower() for item in anchor.get("class", [])}
        text = _clean(anchor.get_text(" ", strip=True)).lower()
        rel = {str(item).lower() for item in anchor.get("rel", [])}
        if "disabled" in classes:
            continue
        if "next" in classes or "next" in rel or "next" in text or text in {"›", "»"}:
            next_url = urljoin(current_url, href)
            return next_url if next_url != current_url else None
    return None


def _parse_list_rows(soup: BeautifulSoup, *, limit: int) -> list[GoodreadsItem]:
    out: list[GoodreadsItem] = []
    seen: set[str] = set()
    for row in soup.select('tr[itemtype="http://schema.org/Book"], tr.bookalike'):
        title_el = row.select_one("a.bookTitle")
        if not title_el:
            continue
        title = _clean(title_el.get_text(" ", strip=True))
        href = title_el.get("href", "")
        if not title or not href:
            continue
        full_url = urljoin(GOODREADS_ORIGIN, href)
        if full_url in seen:
            continue
        seen.add(full_url)

        author_el = row.select_one("a.authorName, a.authorName__container, span[itemprop='author'] a")
        author = _clean(author_el.get_text(" ", strip=True)) if author_el else ""

        mini_el = row.select_one(".minirating")
        rating, rating_count = _parse_minirating(mini_el.get_text(" ", strip=True)) if mini_el else (None, None)

        out.append(
            GoodreadsItem(
                title=title,
                author=author,
                url=full_url,
                rating=rating,
                rating_count=rating_count,
                raw={"title": title, "author": author, "url": full_url, "rating": rating, "rating_count": rating_count},
            )
        )
        if len(out) >= limit:
            break
    return out


def _parse_shelf_rows(soup: BeautifulSoup, *, limit: int) -> list[GoodreadsItem]:
    out: list[GoodreadsItem] = []
    seen: set[str] = set()
    for el in soup.select("div.elementList"):
        title_el = el.select_one("a.bookTitle")
        if not title_el:
            continue
        title = _clean(title_el.get_text(" ", strip=True))
        href = title_el.get("href", "")
        if not title or not href:
            continue
        # Strip "(Paperback)" / "(Hardcover)" tag if present
        title = re.sub(r"\s*\((Paperback|Hardcover|Kindle Edition|ebook|Mass Market Paperback|Audiobook)\)\s*$", "", title, flags=re.IGNORECASE)
        full_url = urljoin(GOODREADS_ORIGIN, href)
        if full_url in seen:
            continue
        seen.add(full_url)

        author_el = el.select_one("a.authorName span[itemprop='name'], a.authorName")
        author = _clean(author_el.get_text(" ", strip=True)) if author_el else ""

        # rating text on a shelf entry sits in `.greyText.smallText`
        rate_el = el.select_one(".greyText.smallText")
        rating, rating_count = _parse_minirating(rate_el.get_text(" ", strip=True)) if rate_el else (None, None)

        cover_el = el.select_one("a.leftAlignedImage img")
        cover = cover_el.get("src", "") if cover_el else ""

        out.append(
            GoodreadsItem(
                title=title,
                author=author,
                url=full_url,
                rating=rating,
                rating_count=rating_count,
                cover_image=cover,
                raw={"title": title, "author": author, "url": full_url, "rating": rating, "rating_count": rating_count},
            )
        )
        if len(out) >= limit:
            break
    return out


def _parse_generic(soup: BeautifulSoup, *, limit: int) -> list[GoodreadsItem]:
    """Fallback selector-set: harvest any /book/show/ links with surrounding context."""
    out: list[GoodreadsItem] = []
    seen: set[str] = set()
    for anchor in soup.select('a[href*="/book/show/"]'):
        href = anchor.get("href", "")
        title = _clean(anchor.get_text(" ", strip=True))
        if not title:
            img = anchor.select_one("img[alt]")
            if img:
                title = _clean(img.get("alt", ""))
        if not title or not href:
            continue
        full_url = urljoin(GOODREADS_ORIGIN, href)
        if full_url in seen:
            continue
        seen.add(full_url)
        parent = anchor.find_parent(["tr", "div", "article", "li"])
        author = ""
        rating = None
        rating_count = None
        if parent:
            author_el = parent.select_one("a.authorName, a[href*='/author/show/']")
            if author_el:
                author = _clean(author_el.get_text(" ", strip=True))
            mini_el = parent.select_one(".minirating, .greyText.smallText")
            if mini_el:
                rating, rating_count = _parse_minirating(mini_el.get_text(" ", strip=True))
        out.append(
            GoodreadsItem(
                title=title,
                author=author,
                url=full_url,
                rating=rating,
                rating_count=rating_count,
                raw={"title": title, "author": author, "url": full_url, "rating": rating, "rating_count": rating_count},
            )
        )
        if len(out) >= limit:
            break
    return out


def discover_goodreads_items(url: str, max_results: int) -> list[GoodreadsItem]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or "goodreads.com" not in (parsed.netloc or ""):
        raise ValueError(f"Not a Goodreads URL: {url!r}")

    target = max_results if max_results > 0 else DEFAULT_RESULT_CEILING
    current_url = url
    out: list[GoodreadsItem] = []
    seen: set[str] = set()

    path = parsed.path.lower()
    for _ in range(MAX_PAGES_PER_SOURCE):
        html = _fetch(current_url)
        soup = BeautifulSoup(html, "html.parser")
        remaining = target - len(out)
        if remaining <= 0:
            break

        if path.startswith("/list/show"):
            page_items = _parse_list_rows(soup, limit=remaining)
        elif path.startswith("/shelf/show") or path.startswith("/genres/"):
            page_items = _parse_shelf_rows(soup, limit=remaining)
        else:
            page_items = _parse_list_rows(soup, limit=remaining)
            if not page_items:
                page_items = _parse_shelf_rows(soup, limit=remaining)
            if not page_items:
                page_items = _parse_generic(soup, limit=remaining)

        added = 0
        for item in page_items:
            key = item.url or f"{item.title}|{item.author}"
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
            added += 1
            if len(out) >= target:
                break
        if len(out) >= target:
            break

        next_url = _next_page_url(soup, current_url)
        if not next_url or added == 0:
            break
        current_url = next_url
        time.sleep(PAGE_DELAY_SECONDS)
    return out


def to_record(item: GoodreadsItem) -> dict:
    return {
        "title": item.title,
        "author": item.author,
        "url": item.url,
        "goodread_link": item.url,
        "rating": item.rating,
        "rating_count": item.rating_count,
        "source_payload": item.raw,
    }


def discover_goodreads_records(url: str, max_results: int) -> list[dict]:
    return [to_record(item) for item in discover_goodreads_items(url, max_results)]

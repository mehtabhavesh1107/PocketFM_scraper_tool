"""Goodreads-focused discovery and extraction helpers for the KU Horror & CT sheet."""
from __future__ import annotations

import json
import html as html_lib
import os
import re
import threading
import time
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Iterable
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from ddgs import DDGS
except ImportError:  # pragma: no cover - optional fallback only
    DDGS = None

from .goodreads_config import (
    GENRE_HINTS,
    MAX_CANDIDATE_BOOKS,
    MAX_SEARCH_RESULTS,
    REQUEST_DELAY_SECONDS,
    REQUEST_TIMEOUT,
)

REQUEST_ATTEMPTS = int(os.getenv("GOODREADS_REQUEST_ATTEMPTS", "3"))
MIN_BOOK_MATCH_SCORE = 0.58
MIN_CONFIDENT_MATCH_SCORE = 0.72
MIN_REVIEW_SCORE = 0.35
MIN_TITLE_AUTO_MATCH_SCORE = 0.78
MIN_TITLE_REVIEW_MATCH_SCORE = 0.45
MIN_AUTHOR_AUTO_MATCH_SCORE = 0.55
HTML_CACHE_MAX_ENTRIES = max(0, int(os.getenv("GOODREADS_HTML_CACHE_MAX_ENTRIES", "0")))

GOODREADS_ROOT = "https://www.goodreads.com"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_GLOBAL_HTML_CACHE: dict[str, str] = {}
_GLOBAL_CACHE_LOCK = threading.RLock()


def _remember_html(cache: dict[str, str], key: str, html: str) -> None:
    if HTML_CACHE_MAX_ENTRIES <= 0:
        return
    cache[key] = html
    while len(cache) > HTML_CACHE_MAX_ENTRIES:
        cache.pop(next(iter(cache)), None)


def is_missing(value) -> bool:
    if value is None:
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "none", "n/a", "-", "#value!"}


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def normalize_title_for_match(title: str) -> str:
    text = html_lib.unescape(normalize_space(title)).lower()
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"\[[^\]]*\]", " ", text)
    text = re.sub(r"\bbook\s+\d+\b", " ", text)
    text = re.sub(r"\b\d+\s+of\s+\d+\b", " ", text)
    text = re.split(r"[:\-\u2013\u2014]", text)[0]
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return normalize_space(text)


def clean_author_name(row: dict) -> str:
    for field in ("Clean Author Names", "Author"):
        raw = normalize_space(str(row.get(field, "")))
        if raw and raw.lower() not in {"nan", "blocked_by_amazon"}:
            break
    else:
        return ""

    raw = re.sub(r"^(by)\s*", "", raw, flags=re.IGNORECASE)
    raw = raw.split("|")[0].strip()
    raw = raw.split(",")[0].strip()
    return raw


def extract_series_name(row: dict) -> str:
    cleaned = normalize_space(str(row.get("Cleaned Series Name", "")))
    if cleaned and cleaned.lower() != "nan":
        return cleaned

    part_of_series = normalize_space(str(row.get("Part of series", "")))
    if part_of_series:
        part_of_series = re.sub(r"\bbook\s+\d+\s+of\s+\d+.*$", "", part_of_series, flags=re.IGNORECASE)
        part_of_series = re.sub(r"\b\d+\s+of\s+\d+.*$", "", part_of_series, flags=re.IGNORECASE)
        return normalize_space(part_of_series.rstrip(":"))

    title = normalize_space(str(row.get("Title", "")))
    match = re.search(r"\(([^()]+?)\s+(?:Book|#)\s*\d+[^()]*\)", title, flags=re.IGNORECASE)
    if match:
        return normalize_space(match.group(1))
    return ""


def extract_book_number(row: dict) -> str:
    raw = normalize_space(str(row.get("Book number", "")))
    if re.fullmatch(r"\d+", raw):
        return raw

    title = normalize_space(str(row.get("Title", "")))
    match = re.search(r"(?:Book|#)\s*(\d+)", title, flags=re.IGNORECASE)
    return match.group(1) if match else ""


def similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def title_match_score(query_title: str, candidate_title: str) -> float:
    query = normalize_title_for_match(query_title)
    candidate = normalize_title_for_match(candidate_title)
    if not query or not candidate:
        return 0.0
    if query == candidate:
        return 1.0

    base = similarity(query, candidate)
    if query in candidate or candidate in query:
        query_tokens = query.split()
        candidate_tokens = candidate.split()
        shorter = min(len(query_tokens), len(candidate_tokens))
        longer = max(len(query_tokens), len(candidate_tokens), 1)
        coverage = shorter / longer
        collection_markers = (
            "book set",
            "books collection",
            "box set",
            "boxed set",
            "bundle",
            "collection",
            "complete",
            "omnibus",
            "series",
        )
        candidate_raw = html_lib.unescape(candidate_title or "").lower()
        if coverage >= 0.72:
            return max(base, 0.90)
        if any(marker in candidate_raw for marker in collection_markers):
            return min(max(base, 0.45), 0.55)
        return max(base, 0.75)
    return base


def author_match_score(query_author: str, candidate_author: str) -> float:
    query = normalize_title_for_match(query_author)
    candidate = normalize_title_for_match(candidate_author)
    if not query or not candidate:
        return 0.0
    if query == candidate:
        return 1.0
    base = similarity(query, candidate)
    if query in candidate or candidate in query:
        query_tokens = set(query.split())
        candidate_tokens = set(candidate.split())
        if query_tokens <= candidate_tokens or candidate_tokens <= query_tokens:
            return max(base, 0.90)
        return max(base, 0.72)
    return base


def normalize_url(url: str, keep_query: bool = False) -> str:
    if not url:
        return ""
    absolute = urljoin(GOODREADS_ROOT, url)
    parsed = urlparse(absolute)
    normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    if keep_query and parsed.query:
        normalized = f"{normalized}?{parsed.query}"
    return normalized


def parse_number(value) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        return str(int(value)) if float(value).is_integer() else str(value)
    text = normalize_space(str(value))
    match = re.search(r"[\d,]+(?:\.\d+)?", text)
    return match.group(0).replace(",", "") if match else ""


def normalize_isbn(value) -> str:
    text = re.sub(r"[^0-9Xx]+", "", str(value or "")).upper()
    return text if len(text) in {10, 13} else ""


def row_isbns(row: dict) -> list[str]:
    keys = (
        "ISBN-10",
        "ISBN-13",
        "ISBN 10",
        "ISBN 13",
        "isbn_10",
        "isbn_13",
        "Amazon ISBN-10",
        "Amazon ISBN-13",
    )
    out: list[str] = []
    seen: set[str] = set()
    for key in keys:
        isbn = normalize_isbn(row.get(key, ""))
        if isbn and isbn not in seen:
            seen.add(isbn)
            out.append(isbn)
    return out


def first_year(value) -> str:
    match = re.search(r"\b(19|20)\d{2}\b", str(value or ""))
    return match.group(0) if match else ""


def first_int(value) -> int | None:
    number = parse_number(value)
    if not number:
        return None
    try:
        return int(float(number))
    except ValueError:
        return None


def _json_walk(value) -> Iterable[dict]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _json_walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _json_walk(child)


@dataclass
class BookCandidate:
    url: str
    title: str = ""
    author: str = ""
    series_name: str = ""
    series_url: str = ""
    isbn_10: str = ""
    isbn_13: str = ""
    rating: str = ""
    rating_count: str = ""
    pages: str = ""
    published_year: str = ""
    publication: str = ""
    publisher: str = ""
    score: float = 0.0
    match_method: str = ""
    evidence: list[str] = field(default_factory=list)


@dataclass
class SeriesDetails:
    url: str = ""
    primary_book_count: str = ""
    total_pages: str = ""
    book1_url: str = ""
    book1_rating: str = ""
    book1_rating_count: str = ""
    book_ratings: list[str] = field(default_factory=list)
    book_rating_counts: list[str] = field(default_factory=list)


class GoodreadsScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._html_cache: dict[str, str] = {}
        self._book_cache: dict[str, BookCandidate] = {}
        self._series_cache: dict[str, SeriesDetails] = {}

    def _fetch_html(self, url: str) -> str:
        normalized = normalize_url(url, keep_query=True)
        if HTML_CACHE_MAX_ENTRIES > 0 and normalized in self._html_cache:
            return self._html_cache[normalized]
        if HTML_CACHE_MAX_ENTRIES > 0:
            with _GLOBAL_CACHE_LOCK:
                cached = _GLOBAL_HTML_CACHE.get(normalized)
            if cached is not None:
                _remember_html(self._html_cache, normalized, cached)
                return cached

        last_error = None
        for attempt in range(REQUEST_ATTEMPTS):
            try:
                response = self.session.get(normalized, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                response.encoding = response.encoding or "utf-8"
                html = response.text
                _remember_html(self._html_cache, normalized, html)
                if HTML_CACHE_MAX_ENTRIES > 0:
                    with _GLOBAL_CACHE_LOCK:
                        _remember_html(_GLOBAL_HTML_CACHE, normalized, html)
                time.sleep(REQUEST_DELAY_SECONDS)
                return html
            except requests.RequestException as exc:
                last_error = exc
                time.sleep(REQUEST_DELAY_SECONDS * (attempt + 1))

        raise last_error

    def _search_url(self, query: str) -> str:
        return f"{GOODREADS_ROOT}/search?q={quote_plus(query)}"

    def _extract_links(self, html: str, marker: str) -> list[str]:
        links = []
        seen = set()
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            if marker not in href:
                continue
            normalized = normalize_url(href)
            if normalized in seen:
                continue
            seen.add(normalized)
            links.append(normalized)

        if links:
            return links

        normalized_html = html.replace("\\u002F", "/").replace("\\/", "/")
        raw_marker = re.escape(marker.lstrip("/"))
        patterns = [
            rf'https?://www\.goodreads\.com/(?:[a-z]{{2}}/)?{raw_marker}[^"\'<>\s\\]+',
            rf'/(?:[a-z]{{2}}/)?{raw_marker}[^"\'<>\s\\]+',
        ]
        for pattern in patterns:
            for match in re.findall(pattern, normalized_html):
                normalized = normalize_url(match)
                if normalized in seen:
                    continue
                seen.add(normalized)
                links.append(normalized)
        return links

    def search_candidates(self, query: str) -> tuple[str, list[str], list[str]]:
        search_url = self._search_url(query)
        html = self._fetch_html(search_url)
        books = self._extract_links(html, "/book/show/")[: MAX_SEARCH_RESULTS * 2]
        series = self._extract_links(html, "/series/")[: MAX_SEARCH_RESULTS]
        return search_url, books, series

    def ddg_fallback_links(self, query: str, marker: str) -> list[str]:
        if DDGS is None:
            return []
        links = []
        seen = set()
        try:
            with DDGS(timeout=REQUEST_TIMEOUT) as ddgs:
                for result in ddgs.text(query, max_results=MAX_SEARCH_RESULTS):
                    href = normalize_url(result.get("href", ""))
                    if marker not in href or href in seen:
                        continue
                    seen.add(href)
                    links.append(href)
        except Exception:
            return []
        return links

    def fetch_book(self, url: str) -> BookCandidate:
        normalized = normalize_url(url)
        if normalized in self._book_cache:
            return self._book_cache[normalized]

        html = self._fetch_html(normalized)
        soup = BeautifulSoup(html, "html.parser")
        candidate = BookCandidate(url=normalized)

        json_ld = None
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = script.string or script.get_text(strip=True)
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            for item in _json_walk(data):
                item_type = item.get("@type")
                item_types = item_type if isinstance(item_type, list) else [item_type]
                if "Book" in item_types:
                    json_ld = item
                    break
            if json_ld:
                break

        if json_ld:
            candidate.title = normalize_space(json_ld.get("name", ""))
            authors = json_ld.get("author", [])
            if isinstance(authors, dict):
                authors = [authors]
            if authors:
                author_names = []
                for author in authors:
                    name = author.get("name", "") if isinstance(author, dict) else str(author)
                    name = normalize_space(name)
                    if name:
                        author_names.append(name)
                candidate.author = ", ".join(dict.fromkeys(author_names))
            rating = json_ld.get("aggregateRating", {})
            candidate.rating = parse_number(rating.get("ratingValue"))
            candidate.rating_count = parse_number(rating.get("ratingCount") or rating.get("reviewCount"))
            candidate.pages = parse_number(json_ld.get("numberOfPages"))
            publication = normalize_space(str(json_ld.get("datePublished", "")))
            candidate.publication = publication
            year_match = re.search(r"\b(19|20)\d{2}\b", publication)
            if year_match:
                candidate.published_year = year_match.group(0)
            publisher = json_ld.get("publisher", "")
            if isinstance(publisher, dict):
                publisher = publisher.get("name", "")
            candidate.publisher = normalize_space(str(publisher))
            candidate.isbn_10 = normalize_isbn(json_ld.get("isbn", ""))
            candidate.isbn_13 = normalize_isbn(json_ld.get("isbn13", ""))

        if not (candidate.rating and candidate.rating_count):
            page_text = normalize_space(soup.get_text(" ", strip=True))
            rating_match = re.search(r"(\d(?:\.\d+)?)\s+avg rating", page_text, flags=re.IGNORECASE)
            count_match = re.search(r"([\d,]+)\s+ratings", page_text, flags=re.IGNORECASE)
            if rating_match and not candidate.rating:
                candidate.rating = rating_match.group(1)
            if count_match and not candidate.rating_count:
                candidate.rating_count = count_match.group(1).replace(",", "")

        if not candidate.title:
            title_tag = soup.find("title")
            if title_tag:
                candidate.title = normalize_space(title_tag.get_text().split(" by ")[0])

        if not candidate.author:
            match = re.search(r"by ([^|]+)\| Goodreads", soup.get_text(" ", strip=True))
            if match:
                candidate.author = normalize_space(match.group(1))

        series_anchor = soup.find("a", href=re.compile(r"/series/"))
        if series_anchor:
            candidate.series_url = normalize_url(series_anchor.get("href", ""))
            candidate.series_name = normalize_space(series_anchor.get_text(" ", strip=True))
        else:
            match = re.search(r'href="([^"]*/series/[^"]+)"', html)
            if match:
                candidate.series_url = normalize_url(match.group(1))

        if not candidate.pages:
            text = soup.get_text(" ", strip=True)
            page_match = re.search(r"(\d{2,5})\s+pages", text, flags=re.IGNORECASE)
            if page_match:
                candidate.pages = page_match.group(1)
            if not candidate.isbn_10:
                isbn10_match = re.search(r"ISBN(?:-|\s)?10\s*[: ]\s*([0-9Xx -]{10,17})", text, flags=re.IGNORECASE)
                if isbn10_match:
                    candidate.isbn_10 = normalize_isbn(isbn10_match.group(1))
            if not candidate.isbn_13:
                isbn13_match = re.search(r"ISBN(?:-|\s)?13\s*[: ]\s*([0-9 -]{13,20})", text, flags=re.IGNORECASE)
                if isbn13_match:
                    candidate.isbn_13 = normalize_isbn(isbn13_match.group(1))
            if not candidate.published_year:
                year_match = re.search(r"\bpublished\s+(?:[A-Z][a-z]+\s+\d{1,2},\s+)?((?:19|20)\d{2})\b", text, flags=re.IGNORECASE)
                if year_match:
                    candidate.published_year = year_match.group(1)

        self._book_cache[normalized] = candidate
        return candidate

    def fetch_series(self, url: str) -> SeriesDetails:
        normalized = normalize_url(url)
        if normalized in self._series_cache:
            return self._series_cache[normalized]

        html = self._fetch_html(normalized)
        soup = BeautifulSoup(html, "html.parser")
        text = normalize_space(soup.get_text(" ", strip=True))
        details = SeriesDetails(url=normalized)

        count_match = re.search(r"(\d+)\s+primary works", text, flags=re.IGNORECASE)
        if count_match:
            details.primary_book_count = count_match.group(1)
        else:
            count_match = re.search(r"bookCount[^\d]*(\d+)", html, flags=re.IGNORECASE)
            if count_match:
                details.primary_book_count = count_match.group(1)

        rows = []
        seen_urls = set()
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            if "/book/show/" not in href:
                continue
            book_url = normalize_url(href)
            if book_url in seen_urls:
                continue
            seen_urls.add(book_url)

            parent = anchor
            chosen_text = ""
            for _ in range(6):
                parent = getattr(parent, "parent", None)
                if parent is None or not hasattr(parent, "get_text"):
                    break
                parent_text = normalize_space(parent.get_text(" ", strip=True))
                lowered_parent_text = parent_text.lower()
                if 40 <= len(parent_text) <= 1200 and (
                    "page" in lowered_parent_text
                    or "rating" in lowered_parent_text
                    or "published" in lowered_parent_text
                    or "edition" in lowered_parent_text
                ):
                    chosen_text = parent_text
                    break

            if not chosen_text:
                chosen_text = normalize_space(anchor.get_text(" ", strip=True))

            pages_match = re.search(r"(\d{2,5})\s+pages", chosen_text, flags=re.IGNORECASE)
            rating_match = re.search(r"(\d(?:\.\d+)?)\s+avg rating", chosen_text, flags=re.IGNORECASE)
            if not rating_match:
                rating_match = re.search(r"\b(\d(?:\.\d+)?)\s*[·•]\s*[\d,]+\s+ratings", chosen_text, flags=re.IGNORECASE)
            count_match = re.search(r"([\d,]+)\s+ratings", chosen_text, flags=re.IGNORECASE)
            rows.append(
                {
                    "url": book_url,
                    "pages": pages_match.group(1) if pages_match else "",
                    "rating": rating_match.group(1) if rating_match else "",
                    "rating_count": count_match.group(1).replace(",", "") if count_match else "",
                    "text": chosen_text,
                }
            )

        if rows:
            details.book_ratings = [row["rating"] for row in rows]
            details.book_rating_counts = [row["rating_count"] for row in rows]
            details.book1_url = rows[0]["url"]
            details.book1_rating = rows[0]["rating"]
            details.book1_rating_count = rows[0]["rating_count"]

            total_pages = 0
            page_hits = 0
            target = int(details.primary_book_count) if details.primary_book_count.isdigit() else len(rows)
            for row in rows[:target]:
                if row["pages"].isdigit():
                    total_pages += int(row["pages"])
                    page_hits += 1
            if page_hits:
                details.total_pages = str(total_pages)

        if details.book1_url and (not details.book1_rating or not details.book1_rating_count):
            book1 = self.fetch_book(details.book1_url)
            details.book1_rating = details.book1_rating or book1.rating
            details.book1_rating_count = details.book1_rating_count or book1.rating_count
            if not details.book_ratings:
                details.book_ratings = [details.book1_rating]
            elif not details.book_ratings[0]:
                details.book_ratings[0] = details.book1_rating
            if not details.book_rating_counts:
                details.book_rating_counts = [details.book1_rating_count]
            elif not details.book_rating_counts[0]:
                details.book_rating_counts[0] = details.book1_rating_count

        self._series_cache[normalized] = details
        return details

    def _build_queries(self, row: dict) -> list[str]:
        title = normalize_space(str(row.get("Title", "")))
        author = clean_author_name(row)
        series = extract_series_name(row)
        genre = normalize_space(str(row.get("Genre", "")))
        core_title = normalize_title_for_match(title)

        queries = []
        if title and author:
            if core_title:
                queries.append(f"{core_title} {author}")
                queries.append(f"{author} {core_title}")
            queries.append(f"{title} {author}")
            queries.append(f"{author} {title}")
        if title:
            queries.append(core_title or title)
            queries.append(title)
        if title and series:
            queries.append(f"{core_title or title} {series}")
            queries.append(f"{title} {series}")
        if series and author:
            queries.append(f"{series} {author}")
            queries.append(f"{author} {series}")
        for hint in GENRE_HINTS.get(genre, []):
            if author and title:
                queries.append(f"{author} {normalize_title_for_match(title)} {hint}")

        deduped = []
        seen = set()
        for query in queries:
            normalized = normalize_space(query)
            if normalized and normalized not in seen:
                seen.add(normalized)
                deduped.append(normalized)
        return deduped

    def _match_quality(self, row: dict, candidate: BookCandidate) -> tuple[float, float, bool]:
        title_score = title_match_score(str(row.get("Title", "")), candidate.title)
        author_score = author_match_score(clean_author_name(row), candidate.author)
        query_isbns = set(row_isbns(row))
        candidate_isbns = {isbn for isbn in (candidate.isbn_10, candidate.isbn_13) if isbn}
        return title_score, author_score, bool(query_isbns and query_isbns & candidate_isbns)

    def _is_confident_match(self, row: dict, candidate: BookCandidate) -> bool:
        title_score, author_score, exact_isbn = self._match_quality(row, candidate)
        query_author = normalize_title_for_match(clean_author_name(row))
        candidate_author = normalize_title_for_match(candidate.author)
        if title_score < MIN_TITLE_AUTO_MATCH_SCORE:
            return False
        if query_author and candidate_author and author_score < MIN_AUTHOR_AUTO_MATCH_SCORE:
            return False
        if candidate.score >= MIN_CONFIDENT_MATCH_SCORE:
            return True
        return exact_isbn and candidate.score >= MIN_BOOK_MATCH_SCORE

    def _confidence_failure_reason(self, row: dict, candidate: BookCandidate, status: str) -> str:
        title_score, author_score, exact_isbn = self._match_quality(row, candidate)
        reason_bits = [f"Best Goodreads candidate scored {candidate.score:.2f}"]
        if title_score < MIN_TITLE_AUTO_MATCH_SCORE:
            reason_bits.append(f"title match {title_score:.2f} below auto threshold")
        query_author = normalize_title_for_match(clean_author_name(row))
        candidate_author = normalize_title_for_match(candidate.author)
        if query_author and candidate_author and author_score < MIN_AUTHOR_AUTO_MATCH_SCORE:
            reason_bits.append(f"author match {author_score:.2f} below auto threshold")
        if candidate.match_method == "isbn_search" and not exact_isbn:
            reason_bits.append("ISBN search did not expose a matching ISBN on the candidate page")
        suffix = "manual review is recommended" if status == "review" else "no confident Goodreads match was found"
        return "; ".join(reason_bits) + f"; {suffix}."

    def _prioritized_candidate_urls(self, candidate_books: dict[str, set[str]], *, expanded: bool = False) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        per_method_limit = max(MAX_CANDIDATE_BOOKS, MAX_SEARCH_RESULTS + 4) if expanded else MAX_CANDIDATE_BOOKS
        for method in ("existing_book_link", "goodreads_search", "ddg_book_fallback", "isbn_search"):
            added_for_method = 0
            for url, methods in candidate_books.items():
                if method not in methods or url in seen:
                    continue
                urls.append(url)
                seen.add(url)
                added_for_method += 1
                if added_for_method >= per_method_limit:
                    break
        return urls[: max(MAX_CANDIDATE_BOOKS, per_method_limit * 3)]

    def _score_book(self, row: dict, candidate: BookCandidate) -> float:
        candidate.evidence = []
        query_title_raw = str(row.get("Title", ""))
        query_title = normalize_title_for_match(query_title_raw)
        query_author = normalize_title_for_match(clean_author_name(row))
        query_series = normalize_title_for_match(extract_series_name(row))
        candidate_title = normalize_title_for_match(candidate.title)
        query_year = first_year(row.get("Publication date") or row.get("Published Year") or row.get("Publication"))
        candidate_year = candidate.published_year
        query_pages = first_int(row.get("Print Length") or row.get("Num Pages"))
        candidate_pages = first_int(candidate.pages)
        query_publisher = normalize_title_for_match(str(row.get("Publisher", "") or row.get("Publisher name", "")))
        candidate_publisher = normalize_title_for_match(candidate.publisher)
        query_isbns = set(row_isbns(row))
        candidate_isbns = {isbn for isbn in (candidate.isbn_10, candidate.isbn_13) if isbn}
        title_score = title_match_score(query_title_raw, candidate.title)
        author_score = author_match_score(clean_author_name(row), candidate.author)

        score = title_score * 0.65
        score += author_score * 0.25
        if query_series:
            score += similarity(query_series, normalize_title_for_match(candidate.series_name)) * 0.10
        if query_isbns and query_isbns & candidate_isbns:
            score += 0.45
            candidate.evidence.append("ISBN matched")
        elif candidate.match_method == "isbn_search":
            score += 0.04
            candidate.evidence.append("Found from ISBN search without page ISBN confirmation")

        if query_title and candidate_title:
            if title_score >= 0.98:
                score += 0.20
                candidate.evidence.append("Exact title match")
            elif title_score >= MIN_TITLE_AUTO_MATCH_SCORE:
                score += 0.10
                candidate.evidence.append("Close title match")
            elif title_score < MIN_TITLE_REVIEW_MATCH_SCORE:
                score -= 0.35
                candidate.evidence.append("Title mismatch")
            else:
                score -= 0.12
                candidate.evidence.append("Weak title match")
        if query_author and candidate.author:
            if author_score >= 0.82:
                score += 0.08
                candidate.evidence.append("Author matched")
            elif author_score < 0.35:
                score -= 0.25
                candidate.evidence.append("Author mismatch")
        if query_year and candidate_year:
            if query_year == candidate_year:
                score += 0.06
                candidate.evidence.append("Publication year matched")
            elif abs(int(query_year) - int(candidate_year)) > 2:
                score -= 0.05
        if query_publisher and candidate_publisher:
            publisher_score = similarity(query_publisher, candidate_publisher)
            if publisher_score >= 0.70:
                score += 0.05
                candidate.evidence.append("Publisher matched")
        if query_pages and candidate_pages:
            tolerance = max(25, int(query_pages * 0.18))
            if abs(query_pages - candidate_pages) <= tolerance:
                score += 0.04
                candidate.evidence.append("Page count close")

        bad_title_markers = ("study guide", "summary of", "analysis of", "workbook", "conversation starters")
        if any(marker in candidate_title for marker in bad_title_markers) and not any(marker in query_title for marker in bad_title_markers):
            score -= 0.45
        if candidate.series_url and query_series:
            score += 0.05
            candidate.evidence.append("Series present")
        return max(0.0, min(score, 1.0))

    def _candidate_review(self, candidate: BookCandidate) -> dict:
        return {
            "url": candidate.url,
            "title": candidate.title,
            "author": candidate.author,
            "series_name": candidate.series_name,
            "series_url": candidate.series_url,
            "rating": candidate.rating,
            "rating_count": candidate.rating_count,
            "pages": candidate.pages,
            "published_year": candidate.published_year,
            "publication": candidate.publication,
            "publisher": candidate.publisher,
            "isbn_10": candidate.isbn_10,
            "isbn_13": candidate.isbn_13,
            "score": round(candidate.score, 3),
            "match_method": candidate.match_method,
            "evidence": list(dict.fromkeys(candidate.evidence)),
        }

    def _updates_from_match(
        self,
        row: dict,
        *,
        best: BookCandidate,
        search_url: str,
        resolved_series_url: str = "",
        candidate_reviews: list[dict] | None = None,
        status: str = "matched",
    ) -> dict:
        series = SeriesDetails()
        if resolved_series_url:
            try:
                series = self.fetch_series(resolved_series_url)
            except requests.RequestException:
                series = SeriesDetails()

        goodreads_rating = best.rating
        goodreads_rating_count = best.rating_count
        primary_books = series.primary_book_count or ("1" if best.url else "")
        total_pages = series.total_pages or best.pages
        book1_url = series.book1_url or best.url
        book1_rating = series.book1_rating or best.rating
        book_ratings = list(series.book_ratings)
        book_rating_counts = list(series.book_rating_counts)
        if not book_ratings:
            book_ratings = [book1_rating]
        if not book_rating_counts:
            book_rating_counts = [goodreads_rating_count]

        extra = {}
        primary_book_count = first_int(primary_books) or 0
        series_column_count = max(10, len(book_ratings), len(book_rating_counts), primary_book_count)
        for index in range(1, series_column_count + 1):
            rating = book_ratings[index - 1] if len(book_ratings) >= index else ""
            rating_count = book_rating_counts[index - 1] if len(book_rating_counts) >= index else ""
            legacy_rating_key = "GR Book 1O Rating" if index == 10 else f"GR Book {index} Rating"
            extra[legacy_rating_key] = rating
            if index == 10:
                extra["GR Book 10 Rating"] = rating
            extra[f"Book {index} Ratings"] = rating
            extra[f"Book{index} No Of Rating"] = rating_count
            extra[f"Book {index} No Of Rating"] = rating_count
        extra["Goodreads Series Ratings"] = book_ratings
        extra["Goodreads Series Rating Counts"] = book_rating_counts

        reason_bits = list(dict.fromkeys(best.evidence))
        if not reason_bits and best.match_method:
            reason_bits.append(best.match_method.replace("_", " "))
        reason = "; ".join(reason_bits) or "Highest-scoring Goodreads candidate."

        return {
            "Goodread Link": search_url,
            "Resolved Goodreads Book": best.url,
            "Series Book 1": book1_url,
            "Series Link": series.url or resolved_series_url,
            "# of primary book": primary_books,
            "# of total pages in series": total_pages,
            "GR Book 1 Rating": book1_rating,
            "Goodreads rating": goodreads_rating,
            "Goodreads no of rating": goodreads_rating_count,
            "Published Year": best.published_year,
            "Publication": best.publication or best.published_year,
            "Publisher name": best.publisher,
            "Goodreads Match Status": status,
            "Goodreads Match Confidence": round(best.score, 3),
            "Goodreads Match Reason": reason,
            "Goodreads Match Method": best.match_method,
            "Goodreads Candidates": candidate_reviews or [self._candidate_review(best)],
            "Goodreads ISBNs Used": row_isbns(row),
            **extra,
        }

    def resolve_row(self, row: dict) -> dict:
        search_url = str(row.get("Goodread Link", "")).strip()
        book_url = str(row.get("Series Book 1", "")).strip()
        series_url = str(row.get("Series Link", "")).strip()

        candidate_books: dict[str, set[str]] = {}
        candidate_series: dict[str, set[str]] = {}
        attempts: list[dict] = []

        def add_book(url: str, method: str) -> None:
            normalized = normalize_url(url)
            if normalized:
                candidate_books.setdefault(normalized, set()).add(method)

        def add_series(url: str, method: str) -> None:
            normalized = normalize_url(url)
            if normalized:
                candidate_series.setdefault(normalized, set()).add(method)

        if book_url and not is_missing(book_url):
            add_book(book_url, "existing_book_link")

        if series_url and not is_missing(series_url):
            add_series(series_url, "existing_series_link")

        queries = self._build_queries(row)
        if not search_url or is_missing(search_url):
            search_url = self._search_url(queries[0] if queries else str(row.get("Title", "")))

        for isbn in row_isbns(row):
            try:
                isbn_search_url, books, series = self.search_candidates(isbn)
            except requests.RequestException as exc:
                attempts.append({"method": "isbn_search", "query": isbn, "error": str(exc)})
                continue
            search_url = search_url or isbn_search_url
            attempts.append({"method": "isbn_search", "query": isbn, "books": len(books), "series": len(series)})
            for url in books:
                add_book(url, "isbn_search")
            for url in series:
                add_series(url, "isbn_search")
            if books:
                break

        for query in queries:
            try:
                _, books, series = self.search_candidates(query)
            except requests.RequestException as exc:
                attempts.append({"method": "goodreads_search", "query": query, "error": str(exc)})
                continue
            attempts.append({"method": "goodreads_search", "query": query, "books": len(books), "series": len(series)})
            for url in books:
                add_book(url, "goodreads_search")
            for url in series:
                add_series(url, "goodreads_search")
            if books and any("isbn_search" in methods for methods in candidate_books.values()):
                break

        if not candidate_books:
            fallback_query = f'site:goodreads.com/book/show "{clean_author_name(row)}" "{row.get("Title", "")}"'
            links = self.ddg_fallback_links(fallback_query, "/book/show/")
            attempts.append({"method": "ddg_book_fallback", "query": fallback_query, "books": len(links)})
            for url in links:
                add_book(url, "ddg_book_fallback")

        if not candidate_series and extract_series_name(row):
            fallback_query = (
                f'site:goodreads.com/series "{clean_author_name(row)}" "{extract_series_name(row)}"'
            )
            links = self.ddg_fallback_links(fallback_query, "/series/")
            attempts.append({"method": "ddg_series_fallback", "query": fallback_query, "series": len(links)})
            for url in links:
                add_series(url, "ddg_series_fallback")

        def score_candidates(*, expanded: bool = False) -> tuple[BookCandidate | None, list[dict]]:
            best_candidate = None
            reviewed_candidates: list[BookCandidate] = []
            for candidate_url in self._prioritized_candidate_urls(candidate_books, expanded=expanded):
                try:
                    candidate = self.fetch_book(candidate_url)
                except requests.RequestException:
                    continue
                methods = sorted(candidate_books.get(candidate_url, {"goodreads_search"}))
                for preferred in ("existing_book_link", "goodreads_search", "ddg_book_fallback", "isbn_search"):
                    if preferred in methods:
                        candidate.match_method = preferred
                        break
                if not candidate.match_method:
                    candidate.match_method = methods[0]
                candidate.score = self._score_book(row, candidate)
                reviewed_candidates.append(candidate)
                if best_candidate is None or candidate.score > best_candidate.score:
                    best_candidate = candidate

            review_payloads = [
                self._candidate_review(candidate)
                for candidate in sorted(reviewed_candidates, key=lambda item: item.score, reverse=True)[:5]
            ]
            return best_candidate, review_payloads

        best, reviews = score_candidates()
        if best is not None and not self._is_confident_match(row, best):
            fallback_title = normalize_space(str(row.get("Title", "")))
            fallback_author = clean_author_name(row)
            fallback_query = f'site:goodreads.com/book/show "{fallback_title}" "{fallback_author}"'
            links = self.ddg_fallback_links(fallback_query, "/book/show/")
            attempts.append({"method": "ddg_book_fallback", "query": fallback_query, "books": len(links)})
            for url in links:
                add_book(url, "ddg_book_fallback")
            best, reviews = score_candidates(expanded=True)

        if best is None:
            return {
                "Goodread Link": search_url,
                "Goodreads Match Status": "unmatched",
                "Goodreads Match Confidence": 0,
                "Goodreads Match Reason": "No Goodreads book candidates were found.",
                "Goodreads Candidates": [],
                "Goodreads Search Attempts": attempts,
                "Goodreads ISBNs Used": row_isbns(row),
            }

        resolved_series_url = normalize_url(series_url) if series_url and not is_missing(series_url) else ""
        if not resolved_series_url:
            resolved_series_url = best.series_url
        if not resolved_series_url and candidate_series:
            resolved_series_url = normalize_url(next(iter(candidate_series)))

        confident = self._is_confident_match(row, best)
        if not confident:
            title_score, _, _ = self._match_quality(row, best)
            status = "review" if best.score >= MIN_REVIEW_SCORE and title_score >= MIN_TITLE_REVIEW_MATCH_SCORE else "unmatched"
            reason = self._confidence_failure_reason(row, best, status)
            return {
                "Goodread Link": search_url,
                "Goodreads Match Status": status,
                "Goodreads Match Confidence": round(best.score, 3),
                "Goodreads Match Reason": reason,
                "Goodreads Match Method": best.match_method,
                "Goodreads Candidates": reviews,
                "Goodreads Search Attempts": attempts,
                "Goodreads ISBNs Used": row_isbns(row),
            }

        updates = self._updates_from_match(
            row,
            best=best,
            search_url=search_url,
            resolved_series_url=resolved_series_url,
            candidate_reviews=reviews,
        )
        updates["Goodreads Search Attempts"] = attempts
        return updates

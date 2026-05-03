"""Goodreads-focused discovery and extraction helpers for the KU Horror & CT sheet."""
from __future__ import annotations

import json
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
MIN_BOOK_MATCH_SCORE = 0.50
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
    text = normalize_space(title).lower()
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


@dataclass
class BookCandidate:
    url: str
    title: str = ""
    author: str = ""
    series_name: str = ""
    series_url: str = ""
    rating: str = ""
    rating_count: str = ""
    pages: str = ""
    published_year: str = ""
    publication: str = ""
    publisher: str = ""
    score: float = 0.0


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
            if isinstance(data, list):
                data = next((item for item in data if item.get("@type") == "Book"), data[0])
            if isinstance(data, dict) and data.get("@type") == "Book":
                json_ld = data
                break

        if json_ld:
            candidate.title = normalize_space(json_ld.get("name", ""))
            authors = json_ld.get("author", [])
            if isinstance(authors, dict):
                authors = [authors]
            if authors:
                candidate.author = normalize_space(authors[0].get("name", ""))
            rating = json_ld.get("aggregateRating", {})
            candidate.rating = parse_number(rating.get("ratingValue"))
            candidate.rating_count = parse_number(rating.get("ratingCount"))
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
            details.book_ratings = [row["rating"] for row in rows[:10]]
            details.book_rating_counts = [row["rating_count"] for row in rows[:10]]
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

        queries = []
        if series and author:
            queries.append(f"{series} {author}")
        if author and series:
            queries.append(f"{author} {series}")
        if author and title:
            queries.append(f"{author} {title}")
        if title and series:
            queries.append(f"{title} {series}")
        if author and title:
            queries.append(f"{author} {normalize_title_for_match(title)}")
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

    def _score_book(self, row: dict, candidate: BookCandidate) -> float:
        query_title = normalize_title_for_match(str(row.get("Title", "")))
        query_author = normalize_title_for_match(clean_author_name(row))
        query_series = normalize_title_for_match(extract_series_name(row))
        candidate_title = normalize_title_for_match(candidate.title)

        score = similarity(query_title, candidate_title) * 0.65
        score += similarity(query_author, normalize_title_for_match(candidate.author)) * 0.25
        if query_series:
            score += similarity(query_series, normalize_title_for_match(candidate.series_name)) * 0.10

        if query_title and candidate_title:
            if query_title == candidate_title:
                score += 0.20
            elif query_title in candidate_title or candidate_title in query_title:
                score += 0.10

        bad_title_markers = ("study guide", "summary of", "analysis of", "workbook", "conversation starters")
        if any(marker in candidate_title for marker in bad_title_markers) and not any(marker in query_title for marker in bad_title_markers):
            score -= 0.45

        if query_author and candidate.author:
            if similarity(query_author, normalize_title_for_match(candidate.author)) < 0.35:
                score -= 0.25
        if candidate.series_url and query_series:
            score += 0.05
        return score

    def resolve_row(self, row: dict) -> dict:
        search_url = str(row.get("Goodread Link", "")).strip()
        book_url = str(row.get("Series Book 1", "")).strip()
        series_url = str(row.get("Series Link", "")).strip()

        candidate_books = []
        candidate_series = []

        if book_url and not is_missing(book_url):
            candidate_books.append(normalize_url(book_url))

        if series_url and not is_missing(series_url):
            candidate_series.append(normalize_url(series_url))

        queries = self._build_queries(row)
        if not search_url or is_missing(search_url):
            search_url = self._search_url(queries[0] if queries else str(row.get("Title", "")))

        for query in queries:
            _, books, series = self.search_candidates(query)
            candidate_books.extend(books)
            candidate_series.extend(series)
            if candidate_books:
                break

        if not candidate_books:
            fallback_query = f'site:goodreads.com/book/show "{clean_author_name(row)}" "{row.get("Title", "")}"'
            candidate_books.extend(self.ddg_fallback_links(fallback_query, "/book/show/"))

        if not candidate_series and extract_series_name(row):
            fallback_query = (
                f'site:goodreads.com/series "{clean_author_name(row)}" "{extract_series_name(row)}"'
            )
            candidate_series.extend(self.ddg_fallback_links(fallback_query, "/series/"))

        seen = set()
        unique_books = []
        for url in candidate_books:
            normalized = normalize_url(url)
            if normalized not in seen:
                seen.add(normalized)
                unique_books.append(normalized)

        best = None
        for candidate_url in unique_books[:MAX_CANDIDATE_BOOKS]:
            try:
                candidate = self.fetch_book(candidate_url)
            except requests.RequestException:
                continue
            candidate.score = self._score_book(row, candidate)
            if best is None or candidate.score > best.score:
                best = candidate

        if best is None or best.score < MIN_BOOK_MATCH_SCORE:
            return {
                "Goodread Link": search_url,
            }

        resolved_series_url = normalize_url(series_url) if series_url and not is_missing(series_url) else ""
        if not resolved_series_url:
            resolved_series_url = best.series_url
        if not resolved_series_url and candidate_series:
            resolved_series_url = normalize_url(candidate_series[0])

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
        book_ratings = list(series.book_ratings[:10])
        book_rating_counts = list(series.book_rating_counts[:10])
        if not book_ratings:
            book_ratings = [book1_rating]
        if not book_rating_counts:
            book_rating_counts = [goodreads_rating_count]

        extra = {}
        for index in range(1, 11):
            rating = book_ratings[index - 1] if len(book_ratings) >= index else ""
            rating_count = book_rating_counts[index - 1] if len(book_rating_counts) >= index else ""
            legacy_rating_key = "GR Book 1O Rating" if index == 10 else f"GR Book {index} Rating"
            extra[legacy_rating_key] = rating
            extra[f"Book {index} Ratings"] = rating
            extra[f"Book{index} No Of Rating"] = rating_count
            extra[f"Book {index} No Of Rating"] = rating_count

        return {
            "Goodread Link": search_url,
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
            **extra,
        }

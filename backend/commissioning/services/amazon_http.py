"""HTTP-based Amazon discovery + product-detail scraper.

Listing parser handles bestseller, zgbs, category, and search-result URLs and
walks every available page until exhausted. Detail parser fetches a product
page (`/dp/<ASIN>`) and pulls the columns the commissioning sheet expects:
publisher, publication date, language, print length, format, genre, series,
book number, synopsis.

Both layers fall through layered selectors so Amazon's regular DOM tweaks
don't break us silently.
"""
from __future__ import annotations

import logging
import html as html_lib
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Iterable, Iterator
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

REQUEST_TIMEOUT = 25
PAGE_DELAY_SECONDS = 0.6
MAX_PAGES_PER_SOURCE = 100  # safety ceiling — Amazon search caps at ~7 pages anyway
DEFAULT_RESULT_CEILING = 5000
DETAIL_THIN_HTML_BYTES = 50_000


def _env_int(name: str, default: int, *, minimum: int = 1, maximum: int = 24) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(value, maximum))


def _env_float(name: str, default: float, *, minimum: float = 0.0, maximum: float = 60.0) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(value, maximum))


AMAZON_DETAIL_WORKERS = _env_int("AMAZON_DETAIL_WORKERS", 4, maximum=12)
AMAZON_DETAIL_RETRY_ROUNDS = _env_int("AMAZON_DETAIL_RETRY_ROUNDS", 1, minimum=0, maximum=5)
AMAZON_DETAIL_RETRY_DELAY_SECONDS = _env_float("AMAZON_DETAIL_RETRY_DELAY_SECONDS", 1.5, maximum=15.0)


class AmazonScrapeError(RuntimeError):
    """Raised when Amazon returns a captcha / robot-check / non-2xx page."""


@dataclass
class AmazonItem:
    asin: str
    title: str
    author: str = ""
    url: str = ""
    rating: float | None = None
    rating_count: int | None = None
    rank: str = ""
    price: str = ""
    raw: dict = field(default_factory=dict)


@dataclass
class AmazonDetail:
    asin: str = ""
    source_asin: str = ""
    detail_asin: str = ""
    source_url: str = ""
    detail_url: str = ""
    title: str = ""
    author: str = ""
    contributors: list[dict[str, str]] = field(default_factory=list)
    publisher: str = ""
    publication_date: str = ""
    language: str = ""
    print_length: str = ""
    listening_length: str = ""
    format: str = ""
    source_format: str = ""
    detail_format: str = ""
    genre: str = ""
    synopsis: str = ""
    series_name: str = ""
    book_number: str = ""
    series_total: str = ""
    rating: float | None = None
    rating_count: int | None = None
    best_sellers_rank: str = ""
    best_sellers_rank_number: str = ""
    customer_reviews: str = ""
    used_format_switch: bool = False
    amazon_quality_flags: list[str] = field(default_factory=list)
    raw_values: dict = field(default_factory=dict)


# ---------- shared utilities -----------------------------------------------------


def _origin_for(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return "https://www.amazon.com"


def _detect_block(html: str) -> str | None:
    lowered = html.lower()
    markers = (
        "enter the characters you see below",
        "to discuss automated access to amazon",
        "type the characters you see in this image",
        "api-services-support@amazon.com",
    )
    for marker in markers:
        if marker in lowered:
            return marker
    if "<title>robot check" in lowered:
        return "robot check"
    return None


_thread_local = threading.local()


def _session() -> requests.Session:
    sess = getattr(_thread_local, "amazon_session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update(DEFAULT_HEADERS)
        _thread_local.amazon_session = sess
    return sess


def _fetch(url: str, *, retries: int = 2) -> str:
    sess = _session()
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = sess.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            response.raise_for_status()
            block = _detect_block(response.text)
            if block:
                raise AmazonScrapeError(f"Amazon anti-bot page returned ({block})")
            return response.text
        except (requests.RequestException, AmazonScrapeError) as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(0.8 * (attempt + 1))
            else:
                raise
    raise last_err  # pragma: no cover - unreachable


_RATING_RE = re.compile(r"(\d+(?:\.\d+)?)\s*out of\s*5", re.IGNORECASE)
_INT_RE = re.compile(r"[\d,]+")
_ASIN_RE = re.compile(r"^[A-Z0-9]{10}$", re.IGNORECASE)


def _parse_rating(text: str) -> float | None:
    if not text:
        return None
    match = _RATING_RE.search(text)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    bare = re.match(r"^\s*(\d(?:\.\d+)?)\s*$", text)
    if bare:
        try:
            value = float(bare.group(1))
            if 0 <= value <= 5:
                return value
        except ValueError:
            return None
    return None


def _parse_int(text: str) -> int | None:
    if not text:
        return None
    match = _INT_RE.search(text.replace(",", ""))
    if not match:
        return None
    digits = match.group(0).replace(",", "")
    try:
        return int(digits)
    except ValueError:
        return None


def _clean(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _looks_like_search(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    return path.startswith("/s") or path == "/s"


def _asins_from_query(url: str) -> list[str]:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    raw_values = params.get("asins", []) + params.get("asin", [])
    asins: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        for token in re.split(r"[\s,]+", raw_value):
            asin = token.strip().upper()
            if not _ASIN_RE.fullmatch(asin) or asin in seen:
                continue
            seen.add(asin)
            asins.append(asin)
    return asins


# ---------- bestseller / zgbs / category parser ----------------------------------


def _parse_bestseller_card(card: Tag, *, base_url: str, fallback_rank: int | None) -> AmazonItem | None:
    asin = (card.get("data-asin") or "").strip()
    if not asin:
        anchor = card.select_one('a[href*="/dp/"]')
        if anchor:
            href = anchor.get("href", "")
            match = re.search(r"/dp/([A-Z0-9]{10})", href)
            if match:
                asin = match.group(1)
    if not asin:
        return None

    title_selectors = [
        "._cDEzb_p13n-sc-css-line-clamp-1_1Fn1y",
        ".p13n-sc-css-line-clamp-1",
        ".p13n-sc-css-line-clamp-2",
        ".p13n-sc-css-line-clamp-3",
        ".p13n-sc-css-line-clamp-4",
        '[class*="line-clamp-"]',
        ".p13n-sc-truncate-desktop-type2",
        ".p13n-sc-truncated",
    ]
    title = ""
    for sel in title_selectors:
        el = card.select_one(sel)
        if el:
            title = _clean(el.get_text(" ", strip=True))
            if title:
                break
    if not title:
        img = card.select_one("img[alt]")
        if img:
            title = _clean(img.get("alt", ""))
    if not title:
        return None

    link_el = card.select_one('a.a-link-normal[href*="/dp/"], a[href*="/dp/"]')
    href = link_el.get("href") if link_el else ""
    full_url = urljoin(base_url, href) if href else f"{base_url}/dp/{asin}"

    author = ""
    for sel in [
        ".a-size-small.a-link-child",
        "a.a-size-small.a-link-normal",
        ".a-row.a-size-small > .a-link-normal",
        ".a-row.a-size-small",
    ]:
        el = card.select_one(sel)
        if el:
            text = _clean(el.get_text(" ", strip=True))
            if text and "out of" not in text.lower() and "stars" not in text.lower():
                author = text
                break

    rating_el = card.select_one(".a-icon-alt, [class*='a-icon-alt']")
    rating = _parse_rating(rating_el.get_text(" ", strip=True)) if rating_el else None

    rc_el = card.select_one("a.a-size-small.a-link-normal, .a-size-small.a-link-normal")
    rating_count = _parse_int(rc_el.get_text(" ", strip=True)) if rc_el else None

    rank_el = card.select_one(".zg-bdg-text, .p13n-sc-badge-label-size-base, span.zg-badge-text")
    rank = _clean(rank_el.get_text(" ", strip=True)) if rank_el else ""
    rank = rank.lstrip("#") if rank else (str(fallback_rank) if fallback_rank else "")

    price_el = card.select_one(".p13n-sc-price, ._cDEzb_p13n-sc-price_3mJ9Z, .a-color-price")
    price = _clean(price_el.get_text(" ", strip=True)) if price_el else ""

    return AmazonItem(
        asin=asin,
        title=title,
        author=author,
        url=full_url,
        rating=rating,
        rating_count=rating_count,
        rank=rank,
        price=price,
        raw={"asin": asin, "title": title, "url": full_url, "author": author, "rank": rank},
    )


def _parse_bestseller_html(html: str, *, base_url: str, page_offset: int = 0) -> tuple[list[AmazonItem], str | None]:
    soup = BeautifulSoup(html, "html.parser")
    cards: list[Tag] = []
    seen_ids: set[str] = set()
    candidate_selectors = [
        'div[id^="gridItemRoot"]',
        ".zg-grid-general-faceout",
        ".p13n-sc-uncoverable-faceout",
        "[data-asin]",
    ]
    for sel in candidate_selectors:
        for el in soup.select(sel):
            asin = (el.get("data-asin") or "").strip() or el.get("id", "").strip()
            if not asin or asin in seen_ids:
                continue
            seen_ids.add(asin)
            cards.append(el)

    items: list[AmazonItem] = []
    seen_asin: set[str] = set()
    for index, card in enumerate(cards, start=1):
        item = _parse_bestseller_card(card, base_url=base_url, fallback_rank=page_offset + index)
        if not item or item.asin in seen_asin:
            continue
        seen_asin.add(item.asin)
        items.append(item)

    next_link = None
    next_anchor = soup.select_one(
        'li.a-last:not(.a-disabled) a, a.zg-pagination-next:not(.zg-pagination-next-disabled), '
        '.s-pagination-next:not(.s-pagination-disabled)'
    )
    if next_anchor:
        href = next_anchor.get("href", "")
        if href:
            next_link = urljoin(base_url, href)
    return items, next_link


# ---------- search-results parser ------------------------------------------------


def _asin_from_card(card: Tag) -> str:
    asin = (card.get("data-asin") or "").strip().upper()
    if _ASIN_RE.fullmatch(asin):
        return asin

    csa_id = (card.get("data-csa-c-item-id") or "").strip()
    match = re.search(r"amzn1\.asin\.([A-Z0-9]{10})", csa_id, flags=re.IGNORECASE)
    if match:
        return match.group(1).upper()

    for anchor in card.select('a[href*="/dp/"], a[href*="/gp/product/"]'):
        href = anchor.get("href", "")
        match = re.search(r"/(?:dp|gp/product)/([A-Z0-9]{10})", href, flags=re.IGNORECASE)
        if match:
            return match.group(1).upper()
    return ""


def _asin_from_href(href: str) -> str:
    match = re.search(r"/(?:dp|gp/product)/([A-Z0-9]{10})", href or "", flags=re.IGNORECASE)
    return match.group(1).upper() if match else ""


def _product_link_for_card(card: Tag, asin: str) -> Tag | None:
    anchors = list(card.select('a[href*="/dp/"], a[href*="/gp/product/"]'))
    if not anchors:
        return None

    title_anchors = [anchor for anchor in anchors if anchor.select_one("h2")]
    for anchor in title_anchors:
        if _asin_from_href(anchor.get("href", "")) == asin:
            return anchor
    for anchor in anchors:
        if _asin_from_href(anchor.get("href", "")) == asin:
            return anchor
    return title_anchors[0] if title_anchors else anchors[0]


def _decoded_raw_html_soup(soup: BeautifulSoup) -> BeautifulSoup | None:
    payloads = [
        html_lib.unescape(raw.get("data-payload") or "")
        for raw in soup.select("raw-html[data-payload]")
        if raw.get("data-payload")
    ]
    if not payloads:
        return None
    return BeautifulSoup("\n".join(payloads), "html.parser")


def _search_cards_from_soup(soup: BeautifulSoup) -> list[Tag]:
    cards: list[Tag] = []
    seen: set[str] = set()
    selectors = [
        '[data-component-type="s-search-result"]',
        '[data-csa-c-item-id^="amzn1.asin."]',
        '[data-cy="asin-faceout-container"]',
    ]
    for selector in selectors:
        for card in soup.select(selector):
            asin = _asin_from_card(card)
            if not asin or asin in seen:
                continue
            seen.add(asin)
            cards.append(card)
    return cards


def _parse_search_card(card: Tag, *, base_url: str) -> AmazonItem | None:
    asin = _asin_from_card(card)
    if not asin:
        return None

    title_el = card.select_one("h2 span, h2 a span, h2")
    title = _clean(title_el.get_text(" ", strip=True)) if title_el else ""
    if not title and title_el:
        title = _clean(title_el.get("aria-label", ""))
    if not title:
        img = card.select_one("img[alt]")
        if img:
            title = _clean(img.get("alt", ""))
    if not title:
        return None

    link_el = _product_link_for_card(card, asin)
    href = link_el.get("href") if link_el else ""
    if href:
        href = href.split("?")[0]
    full_url = urljoin(base_url, href) if href else f"{base_url}/dp/{asin}"

    title_recipe = card.select_one('[data-cy="title-recipe"]') or card

    author = ""
    author_link = title_recipe.select_one('a[href*="/e/"], a[href*="/author/"], a[href*="field-author="]')
    if author_link:
        author = _clean(author_link.get_text(" ", strip=True))
    if not author:
        secondary = title_recipe.select_one(".a-row.a-size-base.a-color-secondary, .a-color-secondary")
        if secondary:
            text = _clean(secondary.get_text(" ", strip=True))
            match = re.search(r"\bby\s+(.+?)(?:\s*\|\s*|\s+\d{1,2}\s+\w+\s+\d{4}|$)", text, re.IGNORECASE)
            if match:
                author = match.group(1).strip(" |")

    rating_el = card.select_one("span.a-icon-alt")
    rating = _parse_rating(rating_el.get_text(" ", strip=True)) if rating_el else None

    rating_count = None
    for el in card.select('a[aria-label$="ratings"], a[aria-label$="rating"]'):
        aria = el.get("aria-label", "")
        if not re.search(r"^\s*[\d,]+\s+rating", aria):
            continue
        rating_count = _parse_int(aria)
        if rating_count is not None:
            break
    if rating_count is None:
        rc_el = card.select_one("[data-cy='reviews-block'] span.a-size-base.s-underline-text, span.a-size-base.s-underline-text")
        if rc_el:
            text = _clean(rc_el.get_text(" ", strip=True)).strip("()")
            multiplier = 1
            if text.endswith("K"):
                multiplier, text = 1000, text[:-1]
            elif text.endswith("M"):
                multiplier, text = 1_000_000, text[:-1]
            try:
                rating_count = int(float(text.replace(",", "")) * multiplier)
            except ValueError:
                rating_count = None

    price_el = card.select_one(".a-price .a-offscreen")
    price = _clean(price_el.get_text(" ", strip=True)) if price_el else ""

    return AmazonItem(
        asin=asin,
        title=title,
        author=author,
        url=full_url,
        rating=rating,
        rating_count=rating_count,
        price=price,
        raw={"asin": asin, "title": title, "url": full_url, "author": author},
    )


def _parse_search_html(html: str, *, base_url: str) -> tuple[list[AmazonItem], str | None]:
    soup = BeautifulSoup(html, "html.parser")
    cards = _search_cards_from_soup(soup)

    raw_soup = _decoded_raw_html_soup(soup)
    if raw_soup is not None:
        seen_card_asins = {_asin_from_card(card) for card in cards}
        for card in _search_cards_from_soup(raw_soup):
            asin = _asin_from_card(card)
            if asin and asin not in seen_card_asins:
                seen_card_asins.add(asin)
                cards.append(card)

    items: list[AmazonItem] = []
    seen_asin: set[str] = set()
    for card in cards:
        item = _parse_search_card(card, base_url=base_url)
        if not item or item.asin in seen_asin:
            continue
        seen_asin.add(item.asin)
        items.append(item)

    next_link = None
    next_anchor = soup.select_one('a.s-pagination-next:not(.s-pagination-disabled)')
    if next_anchor is None and raw_soup is not None:
        next_anchor = raw_soup.select_one('a.s-pagination-next:not(.s-pagination-disabled)')
    if next_anchor:
        href = next_anchor.get("href", "")
        if href:
            next_link = urljoin(base_url, href)
    return items, next_link


def _fallback_search_page_url(current_url: str, next_page_number: int) -> str:
    parsed = urlparse(current_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["page"] = [str(next_page_number)]
    params["ref"] = [f"sr_pg_{next_page_number - 1}"]
    query = urlencode(params, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, parsed.fragment))


# ---------- multi-page driver ----------------------------------------------------


def iter_amazon_listing(url: str, *, max_results: int = 0) -> Iterator[AmazonItem]:
    """Yield every book the listing exposes, walking pagination.

    `max_results=0` means "all available" (capped by `MAX_PAGES_PER_SOURCE`
    pages and `DEFAULT_RESULT_CEILING` items).
    """
    if not url:
        return
    base = _origin_for(url)
    target = max_results if max_results > 0 else DEFAULT_RESULT_CEILING

    query_asins = _asins_from_query(url)
    if query_asins:
        for asin in query_asins[:target]:
            yield AmazonItem(
                asin=asin,
                title=asin,
                url=f"{base}/dp/{asin}",
                raw={"asin": asin, "source": "query_asins", "source_url": url},
            )
        return

    seen: set[str] = set()
    yielded = 0
    current = url
    is_search = _looks_like_search(url)

    for page_index in range(MAX_PAGES_PER_SOURCE):
        try:
            html = _fetch(current)
        except (requests.RequestException, AmazonScrapeError) as exc:
            logger.warning("Amazon listing fetch failed for %s: %s", current, exc)
            return

        if is_search:
            items, next_link = _parse_search_html(html, base_url=base)
        else:
            items, next_link = _parse_bestseller_html(html, base_url=base, page_offset=yielded)

        if not items:
            # First page returned nothing — try the alternate parser before giving up.
            if page_index == 0:
                fallback = _parse_search_html if not is_search else _parse_bestseller_html
                if fallback is _parse_bestseller_html:
                    alt_items, alt_next = fallback(html, base_url=base, page_offset=yielded)
                else:
                    alt_items, alt_next = fallback(html, base_url=base)
                if alt_items:
                    items, next_link = alt_items, alt_next
            if not items:
                return

        page_new_count = 0
        for item in items:
            if item.asin in seen:
                continue
            seen.add(item.asin)
            page_new_count += 1
            yielded += 1
            yield item
            if yielded >= target:
                return

        if items and page_new_count == 0:
            return
        if is_search and items and not next_link:
            next_link = _fallback_search_page_url(current, page_index + 2)
        if not next_link or next_link == current:
            return
        current = next_link
        time.sleep(PAGE_DELAY_SECONDS)


# ---------- product detail page --------------------------------------------------


FORMAT_PRIORITY = {
    "Kindle": 0,
    "Paperback": 1,
    "Hardcover": 2,
    "Mass Market Paperback": 3,
    "Audiobook": 4,
    "Audio CD": 5,
}


def clean_amazon_value(value: str | None) -> str:
    text = value or ""
    text = text.replace("\u200e", " ").replace("\u200f", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^[\s:;\-]+", "", text).strip()
    text = re.sub(r"\s+Read more\s*$", "", text, flags=re.IGNORECASE).strip()
    return text


def _extract_asin(url: str) -> str:
    match = re.search(r"/(?:dp|gp/product|gp/aw/d)/([A-Z0-9]{10})", url or "", flags=re.IGNORECASE)
    return match.group(1).upper() if match else ""


def _detail_url_candidates(url: str) -> list[str]:
    asin = _extract_asin(url)
    if not asin:
        return [url] if url else []
    origin = _origin_for(url)
    candidates = [
        url,
        f"{origin}/-/dp/{asin}",
        f"{origin}/gp/aw/d/{asin}",
    ]
    seen: set[str] = set()
    unique = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)
    return unique


def _fetch_detail_html(url: str) -> tuple[str, str]:
    last_err: Exception | None = None
    candidates = _detail_url_candidates(url)
    for index, candidate in enumerate(candidates):
        try:
            html = _fetch(candidate)
            if index < len(candidates) - 1 and _looks_like_thin_detail_html(html):
                logger.info("Amazon detail route returned thin shell page for %s; trying alternate route", candidate)
                continue
            return html, candidate
        except (requests.RequestException, AmazonScrapeError) as exc:
            last_err = exc
            logger.info("Amazon detail route failed for %s: %s", candidate, exc)
    if last_err:
        raise last_err
    raise ValueError("Amazon detail URL is empty.")


def _looks_like_thin_detail_html(html: str) -> bool:
    """Detect Amazon's lightweight /dp shell pages that parse as incomplete books.

    These pages often return HTTP 200 with only a few KB of markup, so treating
    them as successful detail pages causes repeated slow retries. Alternate
    routes such as /-/dp/<ASIN> or /gp/aw/d/<ASIN> usually contain the real
    product metadata.
    """
    if not html:
        return True
    if len(html) >= DETAIL_THIN_HTML_BYTES:
        return False
    lowered = html.lower()
    useful_markers = (
        'id="producttitle"',
        'id="ebooksproducttitle"',
        "data-rpi-attribute-name",
        "detailbullets_feature_div",
        "productdetails_detailbullets_sections1",
        "bookdescription_feature_div",
    )
    return not any(marker in lowered for marker in useful_markers)


def _put_value(values: dict[str, str], label: str, value: str) -> None:
    label = clean_amazon_value(label).rstrip(":")
    value = clean_amazon_value(value)
    if label and value and not values.get(label):
        values[label] = value


def _split_label_value(text: str) -> tuple[str, str] | None:
    text = clean_amazon_value(text)
    if ":" not in text:
        return None
    label, value = text.split(":", 1)
    label = clean_amazon_value(label).rstrip(":")
    value = clean_amazon_value(value)
    if not label or not value:
        return None
    return label, value


def parse_rpi_attributes(soup: BeautifulSoup) -> dict[str, str]:
    values: dict[str, str] = {}
    for div in soup.select("[data-rpi-attribute-name]"):
        label_el = div.select_one(".rpi-attribute-label")
        value_el = div.select_one(".rpi-attribute-value")
        label = clean_amazon_value(label_el.get_text(" ", strip=True) if label_el else "")
        value = clean_amazon_value(value_el.get_text(" ", strip=True) if value_el else "")
        _put_value(values, label, value)
    return values


def parse_detail_bullets(soup: BeautifulSoup) -> dict[str, str]:
    values: dict[str, str] = {}
    for item in soup.select("#detailBullets_feature_div li, #detailBulletsWrapper_feature_div li"):
        split = _split_label_value(item.get_text(" ", strip=True))
        if split:
            _put_value(values, split[0], split[1])
    return values


def parse_key_values(soup: BeautifulSoup) -> dict[str, str]:
    values: dict[str, str] = {}
    for source in (parse_rpi_attributes(soup), parse_detail_bullets(soup)):
        for label, value in source.items():
            _put_value(values, label, value)

    for row in soup.select(
        "#productDetails_detailBullets_sections1 tr, #productDetailsTable tr, "
        "#productDetails_techSpec_section_1 tr, #detailBullets_feature_div tr"
    ):
        cells = row.find_all(["th", "td"], recursive=False)
        if len(cells) >= 2:
            _put_value(values, cells[0].get_text(" ", strip=True), cells[1].get_text(" ", strip=True))

    for row in soup.select("#productOverview_feature_div tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
        if len(cells) >= 2:
            _put_value(values, cells[0], cells[1])
    return values


def _clean_title(value: str) -> str:
    title = clean_amazon_value(value)
    title = re.sub(r"^\s*Amazon\.com:\s*", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*:\s*Amazon\.com.*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s+Audible Audiobook\s*[-\u2013\u2014]\s*Unabridged$", "", title, flags=re.IGNORECASE)
    title = re.sub(
        r"\s+(Kindle Edition|Audible Audiobook|Paperback|Hardcover|Mass Market Paperback|Audio CD)\b.*$",
        "",
        title,
        flags=re.IGNORECASE,
    )
    return title.strip()


def _normalize_format_label(value: str) -> str:
    text = clean_amazon_value(value)
    text = re.sub(r"\s+Format:\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+INR\s+.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+\$[\d,.]+.*$", "", text)
    lowered = text.lower()
    if "kindle" in lowered or "ebook" in lowered:
        return "Kindle"
    if "mass market" in lowered:
        return "Mass Market Paperback"
    if "paperback" in lowered:
        return "Paperback"
    if "hardcover" in lowered:
        return "Hardcover"
    if "audio cd" in lowered:
        return "Audio CD"
    if "audible" in lowered or "audiobook" in lowered:
        return "Audiobook"
    return text


def _selected_format(soup: BeautifulSoup, values: dict[str, str]) -> str:
    for selector in (
        ".swatchElement.selected .slot-title",
        "#tmmSwatches .swatchElement.selected .slot-title",
        "[id^='tmm-grid-swatch'].selected .slot-title",
        ".tmm-format-name",
    ):
        el = soup.select_one(selector)
        if el:
            text = _normalize_format_label(el.get_text(" ", strip=True))
            if text:
                return text

    byline = clean_amazon_value(soup.select_one("#bylineInfo").get_text(" ", strip=True)) if soup.select_one("#bylineInfo") else ""
    match = re.search(r"Format:\s*([A-Za-z ]+)", byline)
    if match:
        return _normalize_format_label(match.group(1))
    if "audiobook" in values.get("Program Type", "").lower() or values.get("Listening Length"):
        return "Audiobook"
    if values.get("File size") or values.get("Page Flip"):
        return "Kindle"
    return ""


def _parse_contributors(soup: BeautifulSoup, values: dict[str, str]) -> list[dict[str, str]]:
    contributors: list[dict[str, str]] = []
    for span in soup.select("#bylineInfo .author"):
        name_el = span.select_one("a.a-link-normal")
        name = clean_amazon_value(name_el.get_text(" ", strip=True) if name_el else "")
        role_el = span.select_one(".contribution")
        role = clean_amazon_value(role_el.get_text(" ", strip=True) if role_el else "").strip("()")
        if name:
            contributors.append({"name": name, "role": role})
    if not contributors and values.get("Author"):
        for name in re.split(r",|\band\b", values["Author"]):
            cleaned = clean_amazon_value(name)
            if cleaned:
                contributors.append({"name": cleaned, "role": "Author"})
    return contributors


def _author_from_contributors(contributors: list[dict[str, str]], values: dict[str, str]) -> str:
    author_names = [
        item["name"]
        for item in contributors
        if "author" in item.get("role", "").lower() or (not item.get("role") and item.get("name"))
    ]
    if author_names:
        return ", ".join(dict.fromkeys(author_names))
    if values.get("Author"):
        return values["Author"]
    return ""


def parse_rating_and_reviews(soup: BeautifulSoup, values: dict[str, str]) -> tuple[float | None, int | None, str]:
    rating = None
    for candidate in (
        soup.select_one("#acrPopover").get("title") if soup.select_one("#acrPopover") else "",
        soup.select_one("#acrPopover").get_text(" ", strip=True) if soup.select_one("#acrPopover") else "",
        values.get("Customer Reviews", ""),
        soup.select_one("span.a-icon-alt").get_text(" ", strip=True) if soup.select_one("span.a-icon-alt") else "",
    ):
        rating = _parse_rating(candidate or "")
        if rating is not None:
            break

    rating_count = None
    count_candidates = [
        soup.select_one("#acrCustomerReviewText").get_text(" ", strip=True) if soup.select_one("#acrCustomerReviewText") else "",
        values.get("Customer Reviews", ""),
    ]
    for candidate in count_candidates:
        text = clean_amazon_value(candidate)
        match = re.search(r"\(([\d,]+)\)", text) or re.search(r"([\d,]+)\s+ratings?", text, re.IGNORECASE)
        if match:
            rating_count = _parse_int(match.group(1))
        elif text and not re.search(r"out of\s+5", text, re.IGNORECASE):
            rating_count = _parse_int(text)
        if rating_count is not None:
            break

    review_text = ""
    if rating is not None:
        review_text = f"{rating:g} out of 5 stars"
        if rating_count is not None:
            review_text += f"; {rating_count} ratings"
    return rating, rating_count, review_text


def _rank_number(rank_text: str) -> str:
    match = re.search(r"#\s*([\d,]+)", rank_text or "")
    return match.group(1).replace(",", "") if match else ""


def _genre_from_rank(rank_text: str) -> str:
    matches = re.findall(r"#\s*[\d,]+\s+in\s+([^#()]+)", rank_text or "")
    cleaned = [clean_amazon_value(match) for match in matches if clean_amazon_value(match)]
    if cleaned:
        return cleaned[-1]
    return ""


def _parse_print_length(values: dict[str, str]) -> str:
    raw = values.get("Print length") or values.get("Paperback") or values.get("Hardcover") or ""
    match = re.search(r"\d+", raw)
    return match.group(0) if match else ""


def _parse_series(values: dict[str, str], title: str) -> tuple[str, str, str]:
    for label, value in values.items():
        match = re.match(r"Book\s+(\d+)\s+of\s+(\d+)", label, flags=re.IGNORECASE)
        if match:
            return value, match.group(1), match.group(2)
    combined = f"{title} {values.get('Part of series', '')} {values.get('Series', '')}"
    match = re.search(r"\(([^()]+?)\s+(?:Book|#)\s*(\d+)[^()]*\)", combined, flags=re.IGNORECASE)
    if match:
        return clean_amazon_value(match.group(1)), match.group(2), ""
    return "", "", ""


def _parse_synopsis(soup: BeautifulSoup) -> str:
    for selector in (
        "#bookDescription_feature_div .a-expander-content",
        "#bookDescription_feature_div noscript",
        "#bookDescription_feature_div",
        "#productDescription",
        '[data-a-expander-name="book_description_expander"]',
    ):
        el = soup.select_one(selector)
        if el:
            text = clean_amazon_value(el.get_text(" ", strip=True))
            if len(text) > 20:
                return text
    return ""


def parse_media_matrix_links(soup: BeautifulSoup, current_url: str) -> list[dict[str, str | bool]]:
    links: list[dict[str, str | bool]] = []
    seen: set[tuple[str, str]] = set()
    for element in soup.select(".swatchElement, [id^='tmm-grid-swatch']"):
        label_el = element.select_one(".slot-title")
        label = _normalize_format_label(label_el.get_text(" ", strip=True) if label_el else element.get_text(" ", strip=True))
        selected = "selected" in {str(item) for item in element.get("class", [])}
        href_el = element.find("a", href=True)
        href = current_url if selected else href_el.get("href", "") if href_el else ""
        if not href or href.lower().startswith("javascript:"):
            continue
        full_url = urljoin(current_url, href)
        key = (label, full_url)
        if key in seen:
            continue
        seen.add(key)
        links.append({"format": label, "url": full_url, "selected": selected})
    return links


def _page_core_score(detail: AmazonDetail) -> int:
    return sum(
        1
        for value in (
            detail.title,
            detail.author,
            detail.publisher,
            detail.publication_date,
            detail.best_sellers_rank_number or detail.best_sellers_rank,
            detail.print_length,
            detail.synopsis,
        )
        if value
    )


def _quality_flags(detail: AmazonDetail) -> list[str]:
    flags = []
    for name, value in (
        ("missing_title", detail.title),
        ("missing_author", detail.author),
        ("missing_publisher", detail.publisher),
        ("missing_publication_date", detail.publication_date),
        ("missing_best_sellers_rank", detail.best_sellers_rank_number or detail.best_sellers_rank),
        ("missing_print_length", detail.print_length),
        ("missing_synopsis", detail.synopsis),
        ("missing_rating", detail.rating),
    ):
        if value in (None, ""):
            flags.append(name)
    return flags


def _meaningful_title(value: str, asin: str = "") -> bool:
    title = _clean_title(value or "")
    if not title:
        return False
    if asin and title.upper() == asin.upper():
        return False
    return not bool(_ASIN_RE.fullmatch(title))


def _detail_missing_retryable_core(detail: AmazonDetail) -> bool:
    flags = set(detail.amazon_quality_flags or _quality_flags(detail))
    return bool(
        flags
        & {
            "missing_title",
            "missing_author",
            "missing_publisher",
            "missing_publication_date",
            "missing_print_length",
        }
    )


def _detail_needs_retry(item: AmazonItem, detail: AmazonDetail | None) -> bool:
    if detail is None:
        return True
    asin = item.asin or detail.source_asin or detail.asin
    has_title = _meaningful_title(detail.title, asin) or _meaningful_title(item.title, asin)
    has_author = bool(detail.author or item.author or detail.contributors)
    if not has_title or not has_author:
        return True
    if item.raw.get("source") == "query_asins":
        if _detail_missing_retryable_core(detail):
            return True
        return _page_core_score(detail) < 5
    return _page_core_score(detail) < 3


def _finalize_detail_quality(item: AmazonItem, detail: AmazonDetail | None) -> AmazonDetail | None:
    if detail is None:
        return None
    flags = set(detail.amazon_quality_flags or _quality_flags(detail))
    asin = item.asin or detail.source_asin or detail.asin
    if not _meaningful_title(detail.title, asin):
        flags.add("placeholder_title")
    if _detail_needs_retry(item, detail):
        flags.add("detail_fetch_incomplete")
    detail.amazon_quality_flags = sorted(flags)
    return detail


def _fetch_amazon_detail_with_retries(item: AmazonItem) -> AmazonDetail | None:
    attempts = AMAZON_DETAIL_RETRY_ROUNDS + 1
    last_detail: AmazonDetail | None = None
    for attempt in range(attempts):
        if attempt and AMAZON_DETAIL_RETRY_DELAY_SECONDS:
            time.sleep(AMAZON_DETAIL_RETRY_DELAY_SECONDS * attempt)
        last_detail = fetch_amazon_detail(item.url)
        if not _detail_needs_retry(item, last_detail):
            return _finalize_detail_quality(item, last_detail)
        if attempt < attempts - 1:
            logger.info(
                "Amazon detail incomplete for %s; retrying %s/%s",
                item.asin or item.url,
                attempt + 1,
                AMAZON_DETAIL_RETRY_ROUNDS,
            )
    return _finalize_detail_quality(item, last_detail)


def _parse_amazon_detail_page(html: str, url: str) -> tuple[AmazonDetail, BeautifulSoup]:
    soup = BeautifulSoup(html, "html.parser")
    values = parse_key_values(soup)
    detail = AmazonDetail(asin=_extract_asin(url), detail_url=url, raw_values=values)
    detail.detail_asin = detail.asin
    title_el = soup.select_one("#productTitle, #ebooksProductTitle")
    if title_el:
        detail.title = _clean_title(title_el.get_text(" ", strip=True))
    elif soup.title:
        detail.title = _clean_title(soup.title.get_text(" ", strip=True))

    detail.contributors = _parse_contributors(soup, values)
    detail.author = _author_from_contributors(detail.contributors, values)
    detail.publisher = values.get("Publisher", "")
    detail.publication_date = values.get("Publication date", "") or values.get("Audible.com Release Date", "")
    detail.language = values.get("Language", "")
    detail.print_length = _parse_print_length(values)
    detail.listening_length = values.get("Listening Length", "")
    detail.format = _selected_format(soup, values)
    detail.synopsis = _parse_synopsis(soup)
    detail.series_name, detail.book_number, detail.series_total = _parse_series(values, detail.title)
    detail.best_sellers_rank = values.get("Best Sellers Rank", "")
    detail.best_sellers_rank_number = _rank_number(detail.best_sellers_rank)
    detail.genre = _genre_from_rank(detail.best_sellers_rank)
    if not detail.genre:
        crumb = soup.select_one("#wayfinding-breadcrumbs_feature_div .a-link-normal")
        if crumb:
            detail.genre = clean_amazon_value(crumb.get_text(" ", strip=True))
    detail.rating, detail.rating_count, detail.customer_reviews = parse_rating_and_reviews(soup, values)
    return detail, soup


def _merge_detail(primary: AmazonDetail, fallback: AmazonDetail) -> AmazonDetail:
    merged = AmazonDetail()
    for field_name in (
        "asin",
        "detail_asin",
        "detail_url",
        "title",
        "author",
        "publisher",
        "publication_date",
        "language",
        "print_length",
        "listening_length",
        "format",
        "detail_format",
        "genre",
        "synopsis",
        "series_name",
        "book_number",
        "series_total",
        "best_sellers_rank",
        "best_sellers_rank_number",
        "customer_reviews",
    ):
        setattr(merged, field_name, getattr(primary, field_name) or getattr(fallback, field_name))
    merged.rating = primary.rating if primary.rating is not None else fallback.rating
    merged.rating_count = primary.rating_count if primary.rating_count is not None else fallback.rating_count
    contributor_map: dict[tuple[str, str], dict[str, str]] = {}
    for contributor in [*primary.contributors, *fallback.contributors]:
        key = (contributor.get("name", ""), contributor.get("role", ""))
        if key[0]:
            contributor_map.setdefault(key, contributor)
    merged.contributors = list(contributor_map.values())
    merged.raw_values = {"detail": primary.raw_values, "source": fallback.raw_values}
    merged.source_asin = fallback.source_asin or fallback.asin
    merged.source_url = fallback.source_url or fallback.detail_url
    merged.source_format = fallback.source_format or fallback.format
    merged.used_format_switch = primary.detail_url != fallback.detail_url
    merged.amazon_quality_flags = _quality_flags(merged)
    return merged


def fetch_amazon_detail(url: str) -> AmazonDetail | None:
    if not url:
        return None
    try:
        html, effective_url = _fetch_detail_html(url)
    except (requests.RequestException, AmazonScrapeError) as exc:
        logger.info("Amazon detail fetch failed for %s: %s", url, exc)
        return None

    source_detail, source_soup = _parse_amazon_detail_page(html, effective_url)
    source_detail.source_asin = _extract_asin(url) or source_detail.asin
    source_detail.source_url = url
    source_detail.source_format = source_detail.format
    source_detail.detail_format = source_detail.format
    source_detail.amazon_quality_flags = _quality_flags(source_detail)

    should_try_format_switch = source_detail.format == "Audiobook" or any(
        flag in source_detail.amazon_quality_flags
        for flag in ("missing_publisher", "missing_publication_date", "missing_best_sellers_rank", "missing_print_length")
    )
    if not should_try_format_switch:
        return source_detail

    candidates = [
        link
        for link in parse_media_matrix_links(source_soup, effective_url)
        if not link.get("selected") and str(link.get("url", "")) != url
    ]
    candidates.sort(key=lambda item: FORMAT_PRIORITY.get(str(item.get("format", "")), 99))
    source_score = _page_core_score(source_detail)
    for candidate in candidates:
        candidate_url = str(candidate.get("url", ""))
        candidate_format = str(candidate.get("format", ""))
        if candidate_format == "Audiobook":
            continue
        try:
            candidate_html, effective_candidate_url = _fetch_detail_html(candidate_url)
            candidate_detail, _ = _parse_amazon_detail_page(candidate_html, effective_candidate_url)
        except (requests.RequestException, AmazonScrapeError) as exc:
            logger.info("Amazon format-switch fetch failed for %s: %s", candidate_url, exc)
            continue
        candidate_detail.detail_format = candidate_detail.format or candidate_format
        if candidate_detail.title and _page_core_score(candidate_detail) >= source_score:
            return _merge_detail(candidate_detail, source_detail)
    return source_detail


# ---------- public entrypoints ---------------------------------------------------


def discover_amazon_items(url: str, max_results: int = 0) -> list[AmazonItem]:
    return list(iter_amazon_listing(url, max_results=max_results))


def to_record(item: AmazonItem, detail: AmazonDetail | None = None) -> dict:
    """Normalize an AmazonItem (+ optional detail) into the discovery dict shape."""
    rating = detail.rating if detail and detail.rating is not None else item.rating
    rating_count = detail.rating_count if detail and detail.rating_count is not None else item.rating_count
    source_asin = item.asin
    detail_asin = detail.detail_asin if detail and detail.detail_asin else (detail.asin if detail else item.asin)
    detail_url = detail.detail_url if detail and detail.detail_url else item.url
    display_url = detail_url if detail and detail.used_format_switch else item.url
    best_rank = ""
    best_rank_text = ""
    if detail:
        best_rank = detail.best_sellers_rank_number or detail.best_sellers_rank
        best_rank_text = detail.best_sellers_rank
    if not best_rank:
        best_rank = item.rank
    quality_flags = list(detail.amazon_quality_flags) if detail else ["detail_fetch_failed"]
    if not detail and not _meaningful_title(item.title, item.asin):
        quality_flags.append("placeholder_title")
    record: dict = {
        "title": (detail.title if detail and detail.title else item.title),
        "author": (detail.author if detail and detail.author else item.author),
        "url": display_url,
        "amazon_url": display_url,
        "rating": rating,
        "rating_count": rating_count,
        "customer_reviews": detail.customer_reviews if detail else "",
        "best_sellers_rank": best_rank,
        "publisher": detail.publisher if detail else "",
        "publication_date": detail.publication_date if detail else "",
        "language": detail.language if detail else "",
        "print_length": detail.print_length if detail else "",
        "format": detail.format if detail else "",
        "genre": detail.genre if detail else "",
        "synopsis": detail.synopsis if detail else "",
        "part_of_series": detail.series_name if detail else "",
        "cleaned_series_name": detail.series_name if detail else "",
        "book_number": detail.book_number if detail else "",
        "series_flag": "Y" if detail and detail.series_name else ("N" if detail else ""),
        "source_asin": source_asin,
        "detail_asin": detail_asin,
        "detail_url": detail_url,
        "source_format": detail.source_format if detail else "",
        "detail_format": detail.detail_format if detail else "",
        "best_sellers_rank_number": detail.best_sellers_rank_number if detail else "",
        "best_sellers_rank_text": best_rank_text,
        "contributors": detail.contributors if detail else [],
        "amazon_quality_flags": quality_flags,
        "source_payload": {
            "asin": source_asin,
            "source_asin": source_asin,
            "detail_asin": detail_asin,
            "source_url": item.url,
            "detail_url": detail_url,
            "used_format_switch": bool(detail and detail.used_format_switch),
            "source_format": detail.source_format if detail else "",
            "detail_format": detail.detail_format if detail else "",
            "rank": item.rank,
            "best_sellers_rank_number": detail.best_sellers_rank_number if detail else "",
            "best_sellers_rank_text": best_rank_text,
            "price": item.price,
            "detail_fetched": bool(detail),
            "contributors": detail.contributors if detail else [],
            "customer_reviews": detail.customer_reviews if detail else "",
            "amazon_quality_flags": quality_flags,
            "raw_values": detail.raw_values if detail else {},
            "listing_payload": item.raw,
        },
    }
    return record


def discover_amazon_records(
    url: str,
    max_results: int = 0,
    *,
    fetch_details: bool = True,
    on_progress: callable | None = None,
) -> Iterable[dict]:
    """Yield enriched discovery records, optionally fetching each product detail page.

    `on_progress(index, total_known, item)` is called after each record so callers
    can stream progress to the UI without blocking on the full crawl.
    """
    items = discover_amazon_items(url, max_results=max_results)
    total = len(items)
    if on_progress:
        try:
            on_progress(0, total, None)
        except Exception:  # pragma: no cover - progress is best-effort
            pass

    if fetch_details and total > 1 and AMAZON_DETAIL_WORKERS > 1:
        worker_count = min(AMAZON_DETAIL_WORKERS, total)
        records: list[dict | None] = [None] * total
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="amazon-detail") as executor:
            futures = {
                executor.submit(_fetch_amazon_detail_with_retries, item): (index, item)
                for index, item in enumerate(items)
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                index, item = futures[future]
                try:
                    detail = future.result()
                except Exception as exc:  # pragma: no cover - fetch_amazon_detail catches expected errors
                    logger.info("Amazon detail worker failed for %s: %s", item.url, exc)
                    detail = None
                record = to_record(item, detail)
                records[index] = record
                if on_progress:
                    try:
                        on_progress(completed, total, record)
                    except Exception:  # pragma: no cover - progress is best-effort
                        pass
        for record in records:
            if record is not None:
                yield record
        return

    for index, item in enumerate(items, start=1):
        detail = _fetch_amazon_detail_with_retries(item) if fetch_details else None
        record = to_record(item, detail)
        if on_progress:
            try:
                on_progress(index, total, record)
            except Exception:  # pragma: no cover - progress is best-effort
                pass
        yield record
        if fetch_details:
            time.sleep(PAGE_DELAY_SECONDS)

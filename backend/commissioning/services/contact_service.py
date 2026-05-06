from __future__ import annotations

import logging
import re
import sys
from dataclasses import dataclass, field
from time import sleep
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from ..settings import WORKSPACE_ROOT

if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

logger = logging.getLogger(__name__)
_CONTACT_IMPORT_ERROR: Exception | None = None

try:
    from contact_info_pipeline import clean_publisher_details_field, infer_email_types_field, research_author  # type: ignore
except Exception as exc:  # pragma: no cover - depends on optional local helper outside this repo
    clean_publisher_details_field = None
    infer_email_types_field = None
    research_author = None
    _CONTACT_IMPORT_ERROR = exc

try:
    from ddgs import DDGS
except Exception:  # pragma: no cover - optional network search helper
    DDGS = None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
SKIP_DOMAINS = {
    "amazon.com",
    "audible.com",
    "barnesandnoble.com",
    "bookbub.com",
    "bookshop.org",
    "goodreads.com",
    "google.com",
    "instagram.com",
    "linkedin.com",
    "openlibrary.org",
    "reddit.com",
    "twitter.com",
    "x.com",
}
AUTHOR_HINTS = ("author", "books", "fiction", "novel", "official", "website", "contact")
AGENCY_HINTS = ("agent", "agency", "literary", "representation", "rights")
PUBLISHER_HINTS = ("publisher", "publishing", "press", "publicity", "media", "editorial")


@dataclass
class BuiltInContactResult:
    email_id: str = ""
    email_source_note: str = ""
    email_type: str = ""
    contact_forms: str = ""
    facebook_link: str = ""
    publisher_details: str = ""
    website: str = ""
    author_email: str = ""
    agent_email: str = ""
    sources: list[str] = field(default_factory=list)


def _compact(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _split_values(value: object) -> list[str]:
    return [_normalize_space(item) for item in re.split(r";|\n|\|", str(value or "")) if _normalize_space(item)]


def _merge_value(current: str, value: str, *, max_items: int = 3) -> str:
    seen = {item.lower(): item for item in _split_values(current)}
    for item in _split_values(value):
        seen.setdefault(item.lower(), item)
        if len(seen) >= max_items:
            break
    return "; ".join(seen.values())


def _normalize_url(value: str) -> str:
    text = _normalize_space(value)
    if not text:
        return ""
    parsed = urlparse(text if re.match(r"https?://", text, flags=re.I) else f"https://{text}")
    if not parsed.netloc:
        return ""
    path = re.sub(r"/+$", "", parsed.path or "")
    return urlunparse(parsed._replace(scheme=parsed.scheme.lower(), netloc=parsed.netloc.lower(), path=path, query="", fragment=""))


def _root_domain(value: str) -> str:
    host = urlparse(_normalize_url(value)).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def _author_tokens(author: str) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9]+", (author or "").lower()) if len(token) > 1]


def _mentions_author(text: str, author: str) -> bool:
    tokens = _author_tokens(author)
    compact = _compact(text)
    if not tokens:
        return False
    if _compact(author) and _compact(author) in compact:
        return True
    return len([token for token in tokens if token in compact]) >= min(2, len(tokens))


def _domain_matches_author(url: str, author: str) -> bool:
    domain = _compact(_root_domain(url).split(".", 1)[0])
    tokens = _author_tokens(author)
    if not domain or not tokens:
        return False
    if _compact(author) in domain:
        return True
    return tokens[-1] in domain and (len(tokens) == 1 or tokens[0] in domain)


def _is_skippable(url: str) -> bool:
    domain = _root_domain(url)
    return any(domain == item or domain.endswith(f".{item}") for item in SKIP_DOMAINS)


def _has_any(text: str, hints: tuple[str, ...]) -> bool:
    lowered = (text or "").lower()
    return any(hint in lowered for hint in hints)


def _extract_emails(text: str) -> list[str]:
    emails: list[str] = []
    seen: set[str] = set()
    for match in EMAIL_RE.findall(text or ""):
        email = match.strip(".,;:()[]{}<>").lower()
        if email.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg")):
            continue
        if email not in seen:
            seen.add(email)
            emails.append(email)
    return emails[:3]


def _classify_email(email: str, context: str, source_url: str, author: str) -> str:
    email_domain = email.split("@", 1)[1].lower() if "@" in email else ""
    haystack = f"{email_domain} {context} {source_url}".lower()
    if _domain_matches_author(f"https://{email_domain}", author) or _mentions_author(email, author):
        return "Author email"
    if _has_any(haystack, AGENCY_HINTS):
        return "Agent email"
    if _has_any(haystack, PUBLISHER_HINTS):
        return "Publisher email"
    if _mentions_author(context, author):
        return "Author email"
    return "Contact email"


def _fetch_text(url: str) -> tuple[str, BeautifulSoup | None]:
    if _is_skippable(url):
        return "", None
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code >= 400 or not response.text:
            return "", None
        content_type = response.headers.get("content-type", "")
        if "text/html" not in content_type and "application/xhtml" not in content_type:
            return response.text[:50000], None
        soup = BeautifulSoup(response.text, "html.parser")
        return soup.get_text(" ", strip=True)[:50000], soup
    except requests.RequestException:
        return "", None


def _search(query: str, *, max_results: int, pause: float) -> list[dict]:
    if DDGS is None:
        return []
    try:
        if pause:
            sleep(pause)
        with DDGS() as ddgs:
            return list(ddgs.text(query, max_results=max_results))
    except Exception as exc:  # pragma: no cover - network search varies by runtime
        logger.warning("Contact search failed for %r: %s", query, exc)
        return []


def _candidate_urls_from_soup(soup: BeautifulSoup | None, base_url: str, author: str) -> tuple[list[str], list[str]]:
    if soup is None:
        return [], []
    forms: list[str] = []
    facebook: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = _normalize_url(urljoin(base_url, anchor.get("href", "")))
        if not href:
            continue
        label = _normalize_space(anchor.get_text(" ", strip=True)).lower()
        path = urlparse(href).path.lower()
        if "facebook.com" in _root_domain(href) and (_mentions_author(href, author) or _mentions_author(label, author)):
            facebook.append(href)
        if "contact" in path or "contact" in label or "rights" in label:
            forms.append(href)
    return forms[:3], facebook[:2]


def _add_email(result: BuiltInContactResult, email: str, label: str) -> None:
    result.email_id = _merge_value(result.email_id, email, max_items=3)
    result.email_source_note = _merge_value(result.email_source_note, label, max_items=3)
    result.email_type = _merge_value(result.email_type, label, max_items=3)
    if label == "Author email":
        result.author_email = _merge_value(result.author_email, email, max_items=2)
    if label == "Agent email":
        result.agent_email = _merge_value(result.agent_email, email, max_items=2)


def _best_website(sources: list[str], author: str) -> str:
    for source in sources:
        url = _normalize_url(source)
        if url and not _is_skippable(url) and _domain_matches_author(url, author):
            return url
    for source in sources:
        url = _normalize_url(source)
        if url and not _is_skippable(url):
            return url
    return ""


def _derive_direct_email_fields(email_id: str, email_type: str) -> tuple[str, str]:
    emails = _split_values(email_id)
    types = _split_values(email_type)
    author_emails: list[str] = []
    agent_emails: list[str] = []
    for index, email in enumerate(emails):
        label = types[index] if index < len(types) else "; ".join(types)
        if "author" in label.lower():
            author_emails.append(email)
        if "agent" in label.lower():
            agent_emails.append(email)
    return "; ".join(author_emails), "; ".join(agent_emails)


def _research_author_builtin(author: str, publisher: str = "", *, max_results: int = 8, pause: float = 0.0) -> BuiltInContactResult:
    result = BuiltInContactResult()
    queries = [
        f"{author} author official website contact email",
        f"{author} author contact",
        f"{author} literary agent contact",
        f"{author} author Facebook",
    ]
    if publisher:
        queries.append(f"{author} {publisher} rights contact")

    seen_urls: set[str] = set()
    for query in queries:
        if result.email_id and result.contact_forms and result.publisher_details:
            break
        for item in _search(query, max_results=max_results, pause=pause):
            raw_url = str(item.get("href") or item.get("url") or "")
            url = _normalize_url(raw_url)
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            title = _normalize_space(item.get("title", ""))
            body = _normalize_space(item.get("body", ""))
            snippet_context = f"{title} {body} {url}"
            if _is_skippable(url) and "facebook.com" not in _root_domain(url):
                continue
            if not (_mentions_author(snippet_context, author) or _domain_matches_author(url, author)):
                continue
            result.sources.append(url)
            if not result.website and _domain_matches_author(url, author):
                result.website = url
            for email in _extract_emails(snippet_context):
                _add_email(result, email, _classify_email(email, snippet_context, url, author))
            if _has_any(snippet_context, AGENCY_HINTS + PUBLISHER_HINTS):
                detail = f"{title or _root_domain(url)} - {url}"
                result.publisher_details = _merge_value(result.publisher_details, detail, max_items=2)
            page_text, soup = _fetch_text(url)
            page_context = f"{snippet_context} {page_text[:12000]}"
            for email in _extract_emails(page_context):
                _add_email(result, email, _classify_email(email, page_context, url, author))
            forms, facebook = _candidate_urls_from_soup(soup, url, author)
            result.contact_forms = _merge_value(result.contact_forms, "; ".join(forms), max_items=3)
            result.facebook_link = _merge_value(result.facebook_link, "; ".join(facebook), max_items=2)
            if _has_any(page_context, AGENCY_HINTS + PUBLISHER_HINTS):
                detail = f"{title or _root_domain(url)} - {url}"
                result.publisher_details = _merge_value(result.publisher_details, detail, max_items=2)

    result.website = result.website or _best_website(result.sources, author)
    return result


def enrich_book_contacts(book) -> dict:
    author = (book.clean_author_names or book.author or "").strip()
    if not author:
        return {}
    if research_author is not None:
        try:
            result = research_author(author=author, max_results=8, pause=0.0)
            email_type = infer_email_types_field(result.email, result.email_note, author) if infer_email_types_field else ""
            author_email, agent_email = _derive_direct_email_fields(result.email, email_type)
            publisher_details = clean_publisher_details_field(result.publisher_details, author) if clean_publisher_details_field else result.publisher_details
            sources = list(getattr(result, "sources", []) or [])
            return {
                "email_id": result.email,
                "email_source_note": result.email_note,
                "email_type": email_type,
                "contact_forms": result.contact_forms,
                "facebook_link": result.facebook,
                "publisher_details": publisher_details,
                "website": _best_website(sources, author),
                "author_email": author_email,
                "agent_email": agent_email,
            }
        except Exception as exc:
            logger.warning("Optional contact pipeline failed for %s; trying built-in discovery: %s", author, exc)
    logger.info("Using built-in contact discovery because optional contact pipeline is unavailable: %s", _CONTACT_IMPORT_ERROR)
    result = _research_author_builtin(author, book.publisher or "", max_results=8, pause=0.0)
    return {
        "email_id": result.email_id,
        "email_source_note": result.email_source_note,
        "email_type": result.email_type,
        "contact_forms": result.contact_forms,
        "facebook_link": result.facebook_link,
        "publisher_details": result.publisher_details,
        "website": result.website,
        "author_email": result.author_email,
        "agent_email": result.agent_email,
    }

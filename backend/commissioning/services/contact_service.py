from __future__ import annotations

import logging
import sys

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


def enrich_book_contacts(book) -> dict:
    author = (book.clean_author_names or book.author or "").strip()
    if not author or research_author is None:
        if research_author is None:
            logger.warning("Contact enrichment skipped; optional contact pipeline is unavailable: %s", _CONTACT_IMPORT_ERROR)
        return {}
    result = research_author(author=author, max_results=8, pause=0.0)
    return {
        "email_id": result.email,
        "email_source_note": result.email_note,
        "email_type": infer_email_types_field(result.email, result.email_note, author) if infer_email_types_field else "",
        "contact_forms": result.contact_forms,
        "facebook_link": result.facebook,
        "publisher_details": clean_publisher_details_field(result.publisher_details, author) if clean_publisher_details_field else result.publisher_details,
    }

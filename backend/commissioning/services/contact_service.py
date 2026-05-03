from __future__ import annotations

import sys

from ..settings import WORKSPACE_ROOT

if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from contact_info_pipeline import clean_publisher_details_field, infer_email_types_field, research_author  # type: ignore


def enrich_book_contacts(book) -> dict:
    author = (book.clean_author_names or book.author or "").strip()
    if not author:
        return {}
    result = research_author(author=author, max_results=8, pause=0.0)
    return {
        "email_id": result.email,
        "email_source_note": result.email_note,
        "email_type": infer_email_types_field(result.email, result.email_note, author),
        "contact_forms": result.contact_forms,
        "facebook_link": result.facebook,
        "publisher_details": clean_publisher_details_field(result.publisher_details, author),
    }

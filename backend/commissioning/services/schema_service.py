from __future__ import annotations

import csv
import io
import json

from sqlalchemy.orm import Session

from ..models import StoredSchema


def guess_type(label: str) -> str:
    lower = label.lower()
    if any(token in lower for token in ("date", "year", "published")):
        return "date"
    if any(token in lower for token in ("count", "number", "pages", "position", "rank")):
        return "integer"
    if any(token in lower for token in ("rating", "score", "price")):
        return "number"
    if any(token in lower for token in ("url", "link", "href")):
        return "url"
    if "email" in lower:
        return "email"
    if any(token in lower for token in ("synopsis", "description", "summary", "bio", "details")):
        return "text"
    if any(token in lower for token in ("genre", "tags", "list")):
        return "list"
    return "string"


def likely_required(label: str) -> bool:
    return label.strip().lower() in {"title", "author", "rating", "asin", "isbn", "id"}


def parse_schema_content(content: str, file_name: str) -> list[dict]:
    ext = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else "csv"
    if ext == "csv":
        reader = csv.reader(io.StringIO(content))
        for row in reader:
            headers = [item.strip() for item in row if item.strip()]
            if headers:
                return [
                    {
                        "name": header.lower().replace(" ", "_"),
                        "label": header,
                        "type": guess_type(header),
                        "required": likely_required(header),
                        "on": True,
                    }
                    for header in headers
                ]
        return []

    payload = json.loads(content)
    fields = payload if isinstance(payload, list) else payload.get("fields") or payload.get("columns") or []
    parsed = []
    for field in fields:
        name = field.get("name") or field.get("column") or field.get("key") or "field"
        label = field.get("label") or field.get("display_name") or name
        parsed.append(
            {
                "name": name,
                "label": label,
                "type": field.get("type") or field.get("data_type") or guess_type(label),
                "required": bool(field.get("required") or field.get("mandatory")),
                "on": field.get("enabled", True),
            }
        )
    return parsed


def create_schema(
    db: Session,
    *,
    source_type: str,
    file_name: str,
    content: str,
    batch_id: int | None = None,
    name: str = "",
) -> StoredSchema:
    fields = parse_schema_content(content, file_name)
    selected = [field["name"] for field in fields if field.get("required") or field.get("on", True)]
    schema = StoredSchema(
        batch_id=batch_id,
        source_type=source_type,
        name=name or file_name,
        file_name=file_name,
        file_format=file_name.rsplit(".", 1)[-1].lower() if "." in file_name else "",
        fields_json=fields,
        selected_fields_json=selected,
        raw_content=content,
    )
    db.add(schema)
    db.commit()
    db.refresh(schema)
    return schema


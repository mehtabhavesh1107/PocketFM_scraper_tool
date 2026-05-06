from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..models import Batch, ExportRecord
from ..settings import GCS_EXPORT_BUCKET, GCS_EXPORT_PREFIX


MEDIA_TYPES = {
    "csv": "text/csv; charset=utf-8",
    "json": "application/json",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "pdf": "application/pdf",
}


@dataclass(frozen=True)
class ExportDownload:
    filename: str
    media_type: str
    content: bytes | None = None
    local_path: Path | None = None


def gcs_exports_enabled() -> bool:
    return bool(GCS_EXPORT_BUCKET)


def is_gcs_uri(value: str) -> bool:
    return value.startswith("gs://")


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not is_gcs_uri(uri):
        raise ValueError("Not a Google Cloud Storage URI.")
    bucket_and_name = uri[5:]
    bucket, _, name = bucket_and_name.partition("/")
    if not bucket or not name:
        raise ValueError("Invalid Google Cloud Storage URI.")
    return bucket, name


def _storage_client():
    from google.cloud import storage

    return storage.Client()


def upload_export_file(local_path: Path, batch: Batch, export: ExportRecord) -> dict:
    if not gcs_exports_enabled():
        return {}
    prefix = GCS_EXPORT_PREFIX.strip("/")
    object_name = "/".join(
        part
        for part in [
            prefix,
            batch.workspace_id or "public",
            f"batch_{batch.id}",
            local_path.name,
        ]
        if part
    )
    client = _storage_client()
    bucket = client.bucket(GCS_EXPORT_BUCKET)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))
    return {
        "storage_backend": "gcs",
        "gcs_bucket": GCS_EXPORT_BUCKET,
        "gcs_object": object_name,
        "gcs_uri": f"gs://{GCS_EXPORT_BUCKET}/{object_name}",
        "filename": local_path.name,
        "local_staging_path": str(local_path),
    }


def export_download(export: ExportRecord) -> ExportDownload:
    filename = str((export.metadata_json or {}).get("filename") or "")
    media_type = MEDIA_TYPES.get((export.export_format or "").lower(), "application/octet-stream")
    if is_gcs_uri(export.file_path):
        bucket_name, object_name = _parse_gcs_uri(export.file_path)
        client = _storage_client()
        blob = client.bucket(bucket_name).blob(object_name)
        content = blob.download_as_bytes()
        return ExportDownload(filename=filename or Path(object_name).name, media_type=media_type, content=content)
    path = Path(export.file_path)
    return ExportDownload(filename=filename or path.name, media_type=media_type, local_path=path)

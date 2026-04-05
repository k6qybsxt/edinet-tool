from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable
import zipfile

from edinet_monitor.services.collector.document_download_service import download_document_zip
from edinet_monitor.services.storage.path_service import build_zip_save_path


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_manifest_row_for_download(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    doc_id = str(normalized.get("doc_id") or "").strip()
    submit_date = str(normalized.get("submit_date") or "").strip()
    zip_path = str(normalized.get("zip_path") or "").strip()

    if not zip_path and doc_id:
        zip_path = str(build_zip_save_path(submit_date, doc_id))

    normalized["doc_id"] = doc_id
    normalized["submit_date"] = submit_date
    normalized["zip_path"] = zip_path
    normalized["download_status"] = str(normalized.get("download_status") or "pending").strip() or "pending"
    normalized["download_attempts"] = int(normalized.get("download_attempts") or 0)

    return normalized


def is_valid_zip_path(zip_path: Path) -> bool:
    return zip_path.exists() and zipfile.is_zipfile(zip_path)


def should_process_manifest_row(
    row: dict[str, Any],
    *,
    retry_errors: bool = False,
) -> bool:
    doc_id = str(row.get("doc_id") or "").strip()
    if not doc_id:
        return False

    status = str(row.get("download_status") or "pending").strip() or "pending"

    if status in {"pending", ""}:
        return True

    if retry_errors and status == "error":
        return True

    return False


def select_manifest_row_indexes(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    retry_errors: bool = False,
) -> list[int]:
    indexes: list[int] = []

    for idx, row in enumerate(rows):
        rows[idx] = normalize_manifest_row_for_download(row)

        if not should_process_manifest_row(rows[idx], retry_errors=retry_errors):
            continue

        indexes.append(idx)
        if len(indexes) >= limit:
            break

    return indexes


def mark_manifest_download_success(row: dict[str, Any], saved_path: Path, *, existing_file: bool = False) -> None:
    row["zip_path"] = str(saved_path)
    row["download_status"] = "downloaded"
    row["downloaded_at"] = now_text()
    row["download_error"] = ""
    row["download_note"] = "existing_file" if existing_file else "downloaded"


def mark_manifest_download_error(row: dict[str, Any], error: Exception) -> None:
    row["download_status"] = "error"
    row["download_error"] = repr(error)
    row["downloaded_at"] = ""
    row["download_note"] = ""


def process_manifest_download_row(
    row: dict[str, Any],
    *,
    api_key: str,
    downloader: Callable[..., Path] = download_document_zip,
) -> dict[str, Any]:
    normalized = normalize_manifest_row_for_download(row)
    normalized["download_attempts"] = int(normalized.get("download_attempts") or 0) + 1

    doc_id = normalized["doc_id"]
    output_path = Path(str(normalized.get("zip_path") or ""))

    if output_path and is_valid_zip_path(output_path):
        mark_manifest_download_success(normalized, output_path, existing_file=True)
        row.clear()
        row.update(normalized)
        return {
            "result": "existing",
            "doc_id": doc_id,
            "path": str(output_path),
        }

    if output_path.exists():
        output_path.unlink(missing_ok=True)

    saved_path = downloader(
        doc_id=doc_id,
        api_key=api_key,
        output_path=output_path,
        timeout_sec=30,
    )
    mark_manifest_download_success(normalized, saved_path, existing_file=False)
    row.clear()
    row.update(normalized)
    return {
        "result": "downloaded",
        "doc_id": doc_id,
        "path": str(saved_path),
    }

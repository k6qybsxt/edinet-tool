from __future__ import annotations

from datetime import datetime
from pathlib import Path
from time import perf_counter
from time import sleep
from typing import Any, Callable
import zipfile

from edinet_monitor.config.settings import (
    DOWNLOAD_CONNECT_TIMEOUT_SEC,
    DOWNLOAD_MAX_RETRIES,
    DOWNLOAD_READ_TIMEOUT_SEC,
    DOWNLOAD_RETRY_WAIT_SEC,
)
from edinet_monitor.services.collector.document_download_service import (
    DownloadDocumentZipError,
    classify_download_exception,
    download_document_zip,
)
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
    normalized["download_error"] = str(normalized.get("download_error") or "")
    normalized["download_error_type"] = str(normalized.get("download_error_type") or "")
    normalized["download_error_retryable"] = int(normalized.get("download_error_retryable") or 0)
    normalized["download_http_status"] = (
        "" if normalized.get("download_http_status") in (None, "") else int(normalized["download_http_status"])
    )
    normalized["downloaded_at"] = str(normalized.get("downloaded_at") or "")
    normalized["download_last_attempt_at"] = str(normalized.get("download_last_attempt_at") or "")
    normalized["download_note"] = str(normalized.get("download_note") or "")
    return normalized


def is_valid_zip_path(zip_path: Path) -> bool:
    return zip_path.exists() and zipfile.is_zipfile(zip_path)


def resolve_manifest_row_target_date(row: dict[str, Any]) -> str:
    submit_date = str(row.get("submit_date") or "").strip()
    if len(submit_date) >= 10:
        return submit_date[:10]

    source_date = str(row.get("source_date") or "").strip()
    if len(source_date) >= 10:
        return source_date[:10]

    return ""


def resolve_manifest_row_target_time(row: dict[str, Any]) -> str:
    submit_date = str(row.get("submit_date") or "").strip()
    if len(submit_date) >= 16:
        return submit_date[11:16]

    return ""


def matches_manifest_row_submit_filter(
    row: dict[str, Any],
    *,
    target_date_text: str = "",
    date_from_text: str = "",
    date_to_text: str = "",
    time_from_text: str = "",
    time_to_text: str = "",
) -> bool:
    target_date = resolve_manifest_row_target_date(row)
    if not target_date:
        return not (
            target_date_text
            or date_from_text
            or date_to_text
            or time_from_text
            or time_to_text
        )

    if target_date_text and target_date != target_date_text:
        return False

    if date_from_text and target_date < date_from_text:
        return False

    if date_to_text and target_date > date_to_text:
        return False

    if not (time_from_text or time_to_text):
        return True

    target_time = resolve_manifest_row_target_time(row)
    if not target_time:
        return False

    if time_from_text and target_time < time_from_text:
        return False

    if time_to_text and target_time > time_to_text:
        return False

    return True


def matches_manifest_row_date_filter(
    row: dict[str, Any],
    *,
    target_date_text: str = "",
    date_from_text: str = "",
    date_to_text: str = "",
) -> bool:
    return matches_manifest_row_submit_filter(
        row,
        target_date_text=target_date_text,
        date_from_text=date_from_text,
        date_to_text=date_to_text,
    )


def should_process_manifest_row(
    row: dict[str, Any],
    *,
    retry_errors: bool = False,
    target_date_text: str = "",
    date_from_text: str = "",
    date_to_text: str = "",
    time_from_text: str = "",
    time_to_text: str = "",
) -> bool:
    doc_id = str(row.get("doc_id") or "").strip()
    if not doc_id:
        return False

    if not matches_manifest_row_submit_filter(
        row,
        target_date_text=target_date_text,
        date_from_text=date_from_text,
        date_to_text=date_to_text,
        time_from_text=time_from_text,
        time_to_text=time_to_text,
    ):
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
    target_date_text: str = "",
    date_from_text: str = "",
    date_to_text: str = "",
    time_from_text: str = "",
    time_to_text: str = "",
) -> list[int]:
    indexes: list[int] = []

    for idx, row in enumerate(rows):
        rows[idx] = normalize_manifest_row_for_download(row)

        if not should_process_manifest_row(
            rows[idx],
            retry_errors=retry_errors,
            target_date_text=target_date_text,
            date_from_text=date_from_text,
            date_to_text=date_to_text,
            time_from_text=time_from_text,
            time_to_text=time_to_text,
        ):
            continue

        indexes.append(idx)
        if len(indexes) >= limit:
            break

    return indexes


def mark_manifest_download_success(
    row: dict[str, Any],
    saved_path: Path,
    *,
    existing_file: bool = False,
    replaced_invalid_existing: bool = False,
) -> None:
    row["zip_path"] = str(saved_path)
    row["download_status"] = "downloaded"
    row["downloaded_at"] = now_text()
    row["download_error"] = ""
    row["download_error_type"] = ""
    row["download_error_retryable"] = 0
    row["download_http_status"] = ""
    if existing_file:
        row["download_note"] = "existing_file"
    elif replaced_invalid_existing:
        row["download_note"] = "replaced_invalid_existing"
    else:
        row["download_note"] = "downloaded"


def mark_manifest_download_error(
    row: dict[str, Any],
    error: Exception,
    *,
    preserved_note: str = "",
) -> None:
    classified = classify_download_exception(error)
    row["download_status"] = "error"
    row["download_error"] = repr(error)
    row["download_error_type"] = classified.error_type
    row["download_error_retryable"] = 1 if classified.retryable else 0
    row["download_http_status"] = classified.status_code or ""
    row["downloaded_at"] = ""
    row["download_note"] = preserved_note


def should_trigger_cooldown(*, error_type: str, retryable: bool, status_code: int | str | None) -> bool:
    if retryable:
        return True

    if status_code in (429, 500, 502, 503, 504):
        return True

    return error_type in {"timeout", "request_error"}


def process_manifest_download_row(
    row: dict[str, Any],
    *,
    api_key: str,
    downloader: Callable[..., Path] = download_document_zip,
    connect_timeout_sec: int = DOWNLOAD_CONNECT_TIMEOUT_SEC,
    read_timeout_sec: int = DOWNLOAD_READ_TIMEOUT_SEC,
    max_retries: int = DOWNLOAD_MAX_RETRIES,
    retry_wait_sec: float = DOWNLOAD_RETRY_WAIT_SEC,
    sleep_func: Callable[[float], None] = sleep,
    timer_func: Callable[[], float] = perf_counter,
) -> dict[str, Any]:
    normalized = normalize_manifest_row_for_download(row)
    base_attempts = int(normalized.get("download_attempts") or 0)
    doc_id = normalized["doc_id"]
    output_path = Path(str(normalized.get("zip_path") or ""))
    replaced_invalid_existing = False
    download_elapsed_seconds = 0.0
    retry_wait_elapsed_seconds = 0.0

    if output_path and output_path.exists() and not is_valid_zip_path(output_path):
        output_path.unlink(missing_ok=True)
        replaced_invalid_existing = True

    if output_path and is_valid_zip_path(output_path):
        normalized["download_attempts"] = base_attempts + 1
        normalized["download_last_attempt_at"] = now_text()
        mark_manifest_download_success(normalized, output_path, existing_file=True)
        row.clear()
        row.update(normalized)
        return {
            "result": "existing",
            "doc_id": doc_id,
            "path": str(output_path),
            "attempts_used": 1,
            "download_elapsed_seconds": 0.0,
            "retry_wait_elapsed_seconds": 0.0,
            "error_type": "",
            "retryable": False,
            "status_code": "",
            "cooldown_eligible": False,
        }

    final_error: DownloadDocumentZipError | None = None

    for attempt_index in range(max_retries + 1):
        normalized["download_attempts"] = base_attempts + attempt_index + 1
        normalized["download_last_attempt_at"] = now_text()

        try:
            attempt_started = timer_func()
            saved_path = downloader(
                doc_id=doc_id,
                api_key=api_key,
                output_path=output_path,
                connect_timeout_sec=connect_timeout_sec,
                read_timeout_sec=read_timeout_sec,
            )
            download_elapsed_seconds += max(timer_func() - attempt_started, 0.0)
            mark_manifest_download_success(
                normalized,
                saved_path,
                existing_file=False,
                replaced_invalid_existing=replaced_invalid_existing,
            )
            row.clear()
            row.update(normalized)
            return {
                "result": "downloaded",
                "doc_id": doc_id,
                "path": str(saved_path),
                "attempts_used": attempt_index + 1,
                "download_elapsed_seconds": round(download_elapsed_seconds, 3),
                "retry_wait_elapsed_seconds": round(retry_wait_elapsed_seconds, 3),
                "error_type": "",
                "retryable": False,
                "status_code": "",
                "cooldown_eligible": False,
            }
        except Exception as error:
            download_elapsed_seconds += max(timer_func() - attempt_started, 0.0)
            classified = classify_download_exception(error)
            final_error = classified
            mark_manifest_download_error(
                normalized,
                classified,
                preserved_note="replaced_invalid_existing" if replaced_invalid_existing else "",
            )

            if classified.retryable and attempt_index < max_retries:
                if retry_wait_sec > 0:
                    retry_wait_started = timer_func()
                    sleep_func(retry_wait_sec)
                    retry_wait_elapsed_seconds += max(timer_func() - retry_wait_started, 0.0)
                continue

            row.clear()
            row.update(normalized)
            return {
                "result": "error",
                "doc_id": doc_id,
                "path": str(output_path),
                "attempts_used": attempt_index + 1,
                "download_elapsed_seconds": round(download_elapsed_seconds, 3),
                "retry_wait_elapsed_seconds": round(retry_wait_elapsed_seconds, 3),
                "error_type": normalized["download_error_type"],
                "retryable": bool(normalized["download_error_retryable"]),
                "status_code": normalized["download_http_status"],
                "cooldown_eligible": should_trigger_cooldown(
                    error_type=normalized["download_error_type"],
                    retryable=bool(normalized["download_error_retryable"]),
                    status_code=normalized["download_http_status"],
                ),
            }

    row.clear()
    row.update(normalized)
    return {
        "result": "error",
        "doc_id": doc_id,
        "path": str(output_path),
        "attempts_used": max_retries + 1,
        "download_elapsed_seconds": round(download_elapsed_seconds, 3),
        "retry_wait_elapsed_seconds": round(retry_wait_elapsed_seconds, 3),
        "error_type": final_error.error_type if final_error else "unexpected_error",
        "retryable": final_error.retryable if final_error else False,
        "status_code": final_error.status_code if final_error else "",
        "cooldown_eligible": should_trigger_cooldown(
            error_type=final_error.error_type if final_error else "unexpected_error",
            retryable=final_error.retryable if final_error else False,
            status_code=final_error.status_code if final_error else "",
        ),
    }

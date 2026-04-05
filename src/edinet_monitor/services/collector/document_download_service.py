from __future__ import annotations

import zipfile
from dataclasses import dataclass
from pathlib import Path

import requests
from requests import HTTPError, RequestException, Timeout

from edinet_monitor.config.settings import (
    DOCUMENT_TYPE_ZIP,
    DOWNLOAD_CONNECT_TIMEOUT_SEC,
    DOWNLOAD_READ_TIMEOUT_SEC,
    EDINET_API_BASE_URL,
)


@dataclass(frozen=True)
class DownloadDocumentZipError(RuntimeError):
    error_type: str
    retryable: bool
    status_code: int | None = None
    detail: str = ""

    def __str__(self) -> str:
        suffix = []
        if self.status_code is not None:
            suffix.append(f"status_code={self.status_code}")
        suffix.append(f"retryable={self.retryable}")
        if self.detail:
            suffix.append(self.detail)
        suffix_text = " ".join(suffix)
        return f"{self.error_type} {suffix_text}".strip()


def is_retryable_http_status(status_code: int) -> bool:
    return status_code in {408, 409, 425, 429} or status_code >= 500


def classify_download_exception(error: Exception) -> DownloadDocumentZipError:
    if isinstance(error, DownloadDocumentZipError):
        return error

    if isinstance(error, Timeout):
        return DownloadDocumentZipError(
            error_type="timeout",
            retryable=True,
            detail=str(error),
        )

    if isinstance(error, HTTPError):
        status_code = None
        if error.response is not None:
            status_code = int(error.response.status_code)
        return DownloadDocumentZipError(
            error_type="http_error",
            retryable=is_retryable_http_status(status_code or 0),
            status_code=status_code,
            detail=str(error),
        )

    if isinstance(error, RequestException):
        return DownloadDocumentZipError(
            error_type="request_error",
            retryable=True,
            detail=str(error),
        )

    return DownloadDocumentZipError(
        error_type="unexpected_error",
        retryable=False,
        detail=repr(error),
    )


def build_document_url(doc_id: str) -> str:
    return f"{EDINET_API_BASE_URL}/documents/{doc_id}"


def download_document_zip(
    doc_id: str,
    api_key: str,
    output_path: Path,
    *,
    connect_timeout_sec: int = DOWNLOAD_CONNECT_TIMEOUT_SEC,
    read_timeout_sec: int = DOWNLOAD_READ_TIMEOUT_SEC,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    url = build_document_url(doc_id)

    params = {
        "type": DOCUMENT_TYPE_ZIP,
        "Subscription-Key": api_key,
    }

    print(f"[DEBUG] request_start doc_id={doc_id} url={url} output_path={output_path}")

    try:
        response = requests.get(
            url,
            params=params,
            timeout=(connect_timeout_sec, read_timeout_sec),
            stream=False,
        )
    except Exception as error:
        raise classify_download_exception(error) from error

    print(f"[DEBUG] response_status doc_id={doc_id} status_code={response.status_code}")
    print(f"[DEBUG] content_type doc_id={doc_id} content_type={response.headers.get('Content-Type')}")
    print(f"[DEBUG] content_length doc_id={doc_id} content_length={len(response.content)}")

    try:
        response.raise_for_status()
    except Exception as error:
        raise classify_download_exception(error) from error

    if len(response.content) < 4:
        raise DownloadDocumentZipError(
            error_type="response_too_small",
            retryable=True,
            detail=f"bytes={len(response.content)}",
        )

    if response.content[:2] != b"PK":
        preview = response.text[:300]
        raise DownloadDocumentZipError(
            error_type="not_a_zip_response",
            retryable=False,
            detail=f"content_type={response.headers.get('Content-Type')} preview={preview!r}",
        )

    output_path.write_bytes(response.content)

    if not zipfile.is_zipfile(output_path):
        output_path.unlink(missing_ok=True)
        raise DownloadDocumentZipError(
            error_type="saved_zip_invalid",
            retryable=True,
            detail=f"path={output_path}",
        )

    print(f"[DEBUG] write_complete doc_id={doc_id} bytes={len(response.content)}")

    return output_path

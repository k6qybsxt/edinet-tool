from __future__ import annotations

from pathlib import Path

import requests

from edinet_monitor.config.settings import DOCUMENT_TYPE_ZIP, EDINET_API_BASE_URL


def build_document_url(doc_id: str) -> str:
    return f"{EDINET_API_BASE_URL}/documents/{doc_id}"


def download_document_zip(
    doc_id: str,
    api_key: str,
    output_path: Path,
    *,
    timeout_sec: int = 30,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    url = build_document_url(doc_id)

    params = {
        "type": DOCUMENT_TYPE_ZIP,
        "Subscription-Key": api_key,
    }

    print(f"[DEBUG] request_start doc_id={doc_id} url={url} output_path={output_path}")

    response = requests.get(
        url,
        params=params,
        timeout=(10, timeout_sec),
        stream=False,
    )

    print(f"[DEBUG] response_status doc_id={doc_id} status_code={response.status_code}")
    print(f"[DEBUG] content_type doc_id={doc_id} content_type={response.headers.get('Content-Type')}")
    print(f"[DEBUG] content_length doc_id={doc_id} content_length={len(response.content)}")

    response.raise_for_status()

    if len(response.content) < 4:
        raise RuntimeError(f"response too small: doc_id={doc_id} bytes={len(response.content)}")

    if response.content[:2] != b'PK':
        preview = response.text[:300]
        raise RuntimeError(
            f"not a zip response: doc_id={doc_id} content_type={response.headers.get('Content-Type')} preview={preview!r}"
        )

    output_path.write_bytes(response.content)
    print(f"[DEBUG] write_complete doc_id={doc_id} bytes={len(response.content)}")

    return output_path
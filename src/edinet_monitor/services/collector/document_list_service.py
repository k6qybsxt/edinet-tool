from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import requests

from edinet_monitor.config.settings import EDINET_API_BASE_URL


@dataclass
class DocumentListResult:
    metadata: dict[str, Any]
    results: list[dict[str, Any]]


def fetch_document_list(
    target_date: date,
    api_key: str,
    *,
    list_type: int = 2,
    timeout_sec: int = 30,
) -> DocumentListResult:
    url = f"{EDINET_API_BASE_URL}/documents.json"

    params = {
        "date": target_date.isoformat(),
        "type": list_type,
        "Subscription-Key": api_key,
    }

    response = requests.get(url, params=params, timeout=timeout_sec)
    response.raise_for_status()

    payload = response.json()

    metadata = {
        "date": payload.get("metadata", {}).get("date"),
        "process_date_time": payload.get("metadata", {}).get("processDateTime"),
        "status": payload.get("metadata", {}).get("status"),
        "message": payload.get("metadata", {}).get("message"),
    }

    results = payload.get("results", []) or []

    return DocumentListResult(
        metadata=metadata,
        results=results,
    )
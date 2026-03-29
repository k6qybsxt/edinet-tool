from __future__ import annotations

from datetime import datetime
from typing import Any


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_filing_record(row: dict[str, Any]) -> dict[str, Any]:
    timestamp = now_text()

    return {
        "doc_id": str(row.get("docID") or ""),
        "edinet_code": str(row.get("edinetCode") or ""),
        "security_code": str(row.get("secCode") or ""),
        "form_type": str(row.get("formCode") or ""),
        "period_end": str(row.get("periodEnd") or ""),
        "submit_date": str(row.get("submitDateTime") or ""),
        "amendment_flag": 1 if str(row.get("docInfoEditStatus") or "") == "1" else 0,
        "zip_path": "",
        "xbrl_path": "",
        "download_status": "pending",
        "parse_status": "pending",
        "created_at": timestamp,
        "updated_at": timestamp,
    }
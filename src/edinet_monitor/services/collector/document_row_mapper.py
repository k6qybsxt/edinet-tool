from __future__ import annotations

from datetime import datetime
from typing import Any


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_security_code(sec_code: str) -> str:
    text = str(sec_code or "").strip()
    if len(text) == 5 and text.isdigit():
        return text
    return ""


def to_filing_record(row: dict[str, Any]) -> dict[str, Any]:
    timestamp = now_text()
    doc_info_edit_status = str(row.get("docInfoEditStatus") or "")
    legal_status = str(row.get("legalStatus") or "")

    return {
        "doc_id": str(row.get("docID") or ""),
        "edinet_code": str(row.get("edinetCode") or ""),
        "security_code": normalize_security_code(row.get("secCode")),
        "form_type": str(row.get("formCode") or ""),
        "period_end": str(row.get("periodEnd") or ""),
        "submit_date": str(row.get("submitDateTime") or ""),
        "amendment_flag": 1 if doc_info_edit_status == "1" else 0,
        "doc_info_edit_status": doc_info_edit_status,
        "legal_status": legal_status,
        "accounting_standard": "",
        "document_display_unit": "",
        "zip_path": "",
        "xbrl_path": "",
        "download_status": "pending",
        "parse_status": "pending",
        "created_at": timestamp,
        "updated_at": timestamp,
    }

def to_issuer_record(row: dict[str, Any]) -> dict[str, Any]:
    timestamp = now_text()

    return {
        "edinet_code": str(row.get("edinetCode") or ""),
        "security_code": normalize_security_code(row.get("secCode")),
        "company_name": str(row.get("filerName") or ""),
        "market": "",
        "industry": "",
        "is_listed": 1,
        "exchange": "",
        "listing_source": "edinet_document_list",
        "updated_at": timestamp,
    }

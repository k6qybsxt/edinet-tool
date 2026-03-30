from __future__ import annotations

from typing import Any

from edinet_monitor.config.settings import TARGET_FORM_CODES


def _is_numeric_5digit_security_code(sec_code: str) -> bool:
    text = str(sec_code or "").strip()
    return len(text) == 5 and text.isdigit()


def is_target_filing(row: dict[str, Any]) -> bool:
    doc_type_code = str(row.get("docTypeCode") or "")
    ordinance_code = str(row.get("ordinanceCode") or "")
    form_code = str(row.get("formCode") or "")
    edinet_code = str(row.get("edinetCode") or "")
    sec_code = str(row.get("secCode") or "")
    legal_status = str(row.get("legalStatus") or "")

    if not edinet_code:
        return False

    if not _is_numeric_5digit_security_code(sec_code):
        return False

    if ordinance_code != "010":
        return False

    if form_code not in TARGET_FORM_CODES:
        return False

    if doc_type_code != "120":
        return False

    if legal_status not in ("1", ""):
        return False

    return True


def filter_target_filings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if is_target_filing(row)]
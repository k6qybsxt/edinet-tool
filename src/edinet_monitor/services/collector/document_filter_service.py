from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from edinet_monitor.config.settings import TARGET_FORM_CODES


FORM_CODE_ALIASES = {
    # The EDINET API uses 043A00 for 半期報告書. Keep 043000 as a friendly
    # operator alias because it is easy to confuse with the XBRL jpcrp040300.
    "043000": "043A00",
}

HALF_REPORT_FORM_CODES = {"043A00"}
DOC_TYPE_CODES_BY_FORM_CODE = {
    "030000": {"120"},  # 有価証券報告書
    "043A00": {"160"},  # 半期報告書
}


def normalize_form_code(form_code: str | None) -> str:
    text = str(form_code or "").strip().upper()
    return FORM_CODE_ALIASES.get(text, text)


def normalize_form_codes(form_codes: str | Iterable[str] | None = None) -> tuple[str, ...]:
    if form_codes is None:
        raw_items: Iterable[str] = TARGET_FORM_CODES
    elif isinstance(form_codes, str):
        raw_items = form_codes.split(",")
    else:
        raw_items = form_codes

    out: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        text = normalize_form_code(str(item or "").strip())
        if not text or text in seen:
            continue
        out.append(text)
        seen.add(text)
    return tuple(out)


def is_half_form_type(form_type: str | None) -> bool:
    return normalize_form_code(form_type) in HALF_REPORT_FORM_CODES


def _is_numeric_5digit_security_code(sec_code: str) -> bool:
    text = str(sec_code or "").strip()
    return len(text) == 5 and text.isdigit()


def is_target_filing(row: dict[str, Any], *, form_codes: str | Iterable[str] | None = None) -> bool:
    doc_type_code = str(row.get("docTypeCode") or "")
    ordinance_code = str(row.get("ordinanceCode") or "")
    form_code = normalize_form_code(row.get("formCode"))
    edinet_code = str(row.get("edinetCode") or "")
    sec_code = str(row.get("secCode") or "")
    legal_status = str(row.get("legalStatus") or "")
    target_form_codes = set(normalize_form_codes(form_codes))

    if not edinet_code:
        return False

    if not _is_numeric_5digit_security_code(sec_code):
        return False

    if ordinance_code != "010":
        return False

    if form_code not in target_form_codes:
        return False

    expected_doc_type_codes = DOC_TYPE_CODES_BY_FORM_CODE.get(form_code)
    if expected_doc_type_codes:
        if doc_type_code not in expected_doc_type_codes:
            return False
    elif doc_type_code != "120":
        return False

    if legal_status not in ("1", "2", ""):
        return False

    return True


def filter_target_filings(
    rows: list[dict[str, Any]],
    *,
    form_codes: str | Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    return [row for row in rows if is_target_filing(row, form_codes=form_codes)]

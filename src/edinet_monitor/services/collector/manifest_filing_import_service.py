from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable

from edinet_monitor.services.collector.document_row_mapper import normalize_security_code, now_text
from edinet_monitor.services.storage.manifest_service import build_manifest_path, read_manifest_rows
from edinet_monitor.services.storage.path_service import build_xbrl_save_path


DOWNLOAD_STATUS_PRIORITY = {
    "pending": 0,
    "error": 1,
    "downloaded": 2,
}

PRESERVE_PARSE_STATUSES = {
    "raw_facts_saved",
    "normalized_metrics_saved",
    "derived_metrics_saved",
    "raw_facts_error",
    "normalized_metrics_error",
    "derived_metrics_error",
}


def resolve_manifest_paths(
    *,
    manifest_root: Path,
    manifest_name: str = "",
    manifest_path: str | Path = "",
) -> list[Path]:
    if manifest_path:
        return [Path(manifest_path)]

    if manifest_name:
        return [build_manifest_path(manifest_name)]

    return sorted(path for path in Path(manifest_root).glob("*.jsonl") if path.is_file())


def _normalize_download_status(status: str | None) -> str:
    text = str(status or "").strip()
    if text in DOWNLOAD_STATUS_PRIORITY:
        return text
    return "pending"


def _manifest_row_priority(row: dict[str, Any]) -> tuple[int, str, str]:
    return (
        DOWNLOAD_STATUS_PRIORITY[_normalize_download_status(row.get("download_status"))],
        str(row.get("submit_date") or ""),
        str(row.get("source_date") or ""),
    )


def merge_manifest_rows_for_filing_sync(
    manifest_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_doc_id: dict[str, dict[str, Any]] = {}

    for row in manifest_rows:
        doc_id = str(row.get("doc_id") or "").strip()
        if not doc_id:
            continue

        existing = by_doc_id.get(doc_id)
        if existing is None or _manifest_row_priority(row) >= _manifest_row_priority(existing):
            by_doc_id[doc_id] = dict(row)

    return sorted(
        by_doc_id.values(),
        key=lambda row: (
            str(row.get("submit_date") or ""),
            str(row.get("doc_id") or ""),
        ),
    )


def load_manifest_rows_for_filing_sync(
    manifest_paths: Iterable[Path],
) -> list[dict[str, Any]]:
    merged_input_rows: list[dict[str, Any]] = []

    for manifest_path in manifest_paths:
        merged_input_rows.extend(read_manifest_rows(Path(manifest_path)))

    return merge_manifest_rows_for_filing_sync(merged_input_rows)


def build_filing_record_from_manifest_row(
    row: dict[str, Any],
    *,
    timestamp_text: str | None = None,
) -> dict[str, Any]:
    timestamp = timestamp_text or now_text()
    doc_id = str(row.get("doc_id") or "").strip()
    submit_date = str(row.get("submit_date") or "").strip()
    zip_path_text = str(row.get("zip_path") or "").strip()
    zip_path = Path(zip_path_text) if zip_path_text else None
    zip_exists = bool(zip_path and zip_path.exists())

    xbrl_candidate = build_xbrl_save_path(submit_date, doc_id)
    xbrl_exists = xbrl_candidate.exists()

    if xbrl_exists:
        download_status = "downloaded"
        parse_status = "xbrl_ready"
        xbrl_path_text = str(xbrl_candidate)
    elif zip_exists:
        download_status = "downloaded"
        parse_status = "pending"
        xbrl_path_text = ""
    else:
        download_status = _normalize_download_status(row.get("download_status"))
        parse_status = "pending"
        xbrl_path_text = ""

    return {
        "doc_id": doc_id,
        "edinet_code": str(row.get("edinet_code") or "").strip(),
        "security_code": normalize_security_code(row.get("security_code")),
        "form_type": str(row.get("form_code") or "").strip(),
        "period_end": str(row.get("period_end") or "").strip(),
        "submit_date": submit_date,
        "amendment_flag": int(row.get("amendment_flag") or 0),
        "doc_info_edit_status": str(row.get("doc_info_edit_status") or "").strip(),
        "legal_status": str(row.get("legal_status") or "").strip(),
        "accounting_standard": "",
        "document_display_unit": "",
        "zip_path": str(zip_path) if zip_exists and zip_path else "",
        "xbrl_path": xbrl_path_text,
        "download_status": download_status,
        "parse_status": parse_status,
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def _merge_download_status(existing_status: str, incoming_status: str) -> str:
    existing = _normalize_download_status(existing_status)
    incoming = _normalize_download_status(incoming_status)

    if DOWNLOAD_STATUS_PRIORITY[incoming] >= DOWNLOAD_STATUS_PRIORITY[existing]:
        return incoming
    return existing


def _merge_parse_status(existing_status: str, incoming_status: str) -> str:
    existing = str(existing_status or "").strip()
    incoming = str(incoming_status or "").strip()

    if existing in PRESERVE_PARSE_STATUSES:
        return existing

    if incoming == "xbrl_ready" or existing == "xbrl_ready":
        return "xbrl_ready"

    if existing == "xbrl_extract_error" and incoming in ("", "pending"):
        return "xbrl_extract_error"

    if incoming:
        return incoming

    if existing:
        return existing

    return "pending"


def merge_filing_record(
    existing: dict[str, Any] | None,
    incoming: dict[str, Any],
) -> dict[str, Any]:
    if not existing:
        return dict(incoming)

    return {
        "doc_id": incoming["doc_id"],
        "edinet_code": incoming["edinet_code"] or existing.get("edinet_code", ""),
        "security_code": incoming["security_code"] or existing.get("security_code", ""),
        "form_type": incoming["form_type"] or existing.get("form_type", ""),
        "period_end": incoming["period_end"] or existing.get("period_end", ""),
        "submit_date": incoming["submit_date"] or existing.get("submit_date", ""),
        "amendment_flag": incoming["amendment_flag"],
        "doc_info_edit_status": incoming["doc_info_edit_status"] or existing.get("doc_info_edit_status", ""),
        "legal_status": incoming["legal_status"] or existing.get("legal_status", ""),
        "accounting_standard": existing.get("accounting_standard", "") or incoming.get("accounting_standard", ""),
        "document_display_unit": existing.get("document_display_unit", "") or incoming.get("document_display_unit", ""),
        "zip_path": incoming["zip_path"] or existing.get("zip_path", ""),
        "xbrl_path": incoming["xbrl_path"] or existing.get("xbrl_path", ""),
        "download_status": _merge_download_status(
            str(existing.get("download_status") or ""),
            str(incoming.get("download_status") or ""),
        ),
        "parse_status": _merge_parse_status(
            str(existing.get("parse_status") or ""),
            str(incoming.get("parse_status") or ""),
        ),
        "created_at": str(existing.get("created_at") or incoming.get("created_at") or ""),
        "updated_at": incoming["updated_at"],
    }


def _fetch_existing_filings(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
            doc_id,
            edinet_code,
            security_code,
            form_type,
            period_end,
            submit_date,
            amendment_flag,
            doc_info_edit_status,
            legal_status,
            accounting_standard,
            document_display_unit,
            zip_path,
            xbrl_path,
            download_status,
            parse_status,
            created_at,
            updated_at
        FROM filings
        """
    ).fetchall()
    return {str(row["doc_id"]): dict(row) for row in rows}


def _upsert_full_filing(conn: sqlite3.Connection, filing: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO filings (
            doc_id,
            edinet_code,
            security_code,
            form_type,
            period_end,
            submit_date,
            amendment_flag,
            doc_info_edit_status,
            legal_status,
            accounting_standard,
            document_display_unit,
            zip_path,
            xbrl_path,
            download_status,
            parse_status,
            created_at,
            updated_at
        )
        VALUES (
            :doc_id,
            :edinet_code,
            :security_code,
            :form_type,
            :period_end,
            :submit_date,
            :amendment_flag,
            :doc_info_edit_status,
            :legal_status,
            :accounting_standard,
            :document_display_unit,
            :zip_path,
            :xbrl_path,
            :download_status,
            :parse_status,
            :created_at,
            :updated_at
        )
        ON CONFLICT(doc_id) DO UPDATE SET
            edinet_code = excluded.edinet_code,
            security_code = excluded.security_code,
            form_type = excluded.form_type,
            period_end = excluded.period_end,
            submit_date = excluded.submit_date,
            amendment_flag = excluded.amendment_flag,
            doc_info_edit_status = excluded.doc_info_edit_status,
            legal_status = excluded.legal_status,
            accounting_standard = excluded.accounting_standard,
            document_display_unit = excluded.document_display_unit,
            zip_path = excluded.zip_path,
            xbrl_path = excluded.xbrl_path,
            download_status = excluded.download_status,
            parse_status = excluded.parse_status,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
        """,
        filing,
    )


def upsert_manifest_filing_records(
    conn: sqlite3.Connection,
    filing_records: Iterable[dict[str, Any]],
) -> dict[str, int]:
    existing_by_doc_id = _fetch_existing_filings(conn)

    total_rows = 0
    inserted_rows = 0
    updated_rows = 0
    downloaded_rows = 0
    xbrl_ready_rows = 0

    for incoming in filing_records:
        doc_id = str(incoming.get("doc_id") or "").strip()
        if not doc_id:
            continue

        total_rows += 1
        existing = existing_by_doc_id.get(doc_id)
        merged = merge_filing_record(existing, incoming)
        _upsert_full_filing(conn, merged)
        existing_by_doc_id[doc_id] = merged

        if existing is None:
            inserted_rows += 1
        else:
            updated_rows += 1

        if merged["download_status"] == "downloaded":
            downloaded_rows += 1

        if merged["parse_status"] == "xbrl_ready":
            xbrl_ready_rows += 1

    conn.commit()

    return {
        "total_rows": total_rows,
        "inserted_rows": inserted_rows,
        "updated_rows": updated_rows,
        "downloaded_rows": downloaded_rows,
        "xbrl_ready_rows": xbrl_ready_rows,
    }


from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

from edinet_monitor.config.settings import XBRL_RETENTION_ENABLED, XBRL_RETENTION_MONTHS
from edinet_monitor.services.storage.raw_retention_service import month_start, shift_month


def parse_submit_month(submit_date_text: str) -> date | None:
    text = str(submit_date_text or "").strip()
    if len(text) < 7:
        return None

    try:
        year = int(text[:4])
        month = int(text[5:7])
    except ValueError:
        return None

    if month < 1 or month > 12:
        return None

    return month_start(year, month)


def detect_latest_filing_month(conn: sqlite3.Connection) -> date | None:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT submit_date
        FROM filings
        WHERE submit_date IS NOT NULL
          AND submit_date <> ''
        ORDER BY submit_date DESC, doc_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return parse_submit_month(str(row["submit_date"] or ""))


def resolve_xbrl_keep_from_month(*, latest_month: date, keep_months: int = XBRL_RETENTION_MONTHS) -> date:
    if keep_months <= 0:
        raise ValueError("keep_months must be greater than 0.")
    return shift_month(latest_month, -(keep_months - 1))


def fetch_xbrl_cleanup_candidates(
    conn: sqlite3.Connection,
    *,
    keep_from_month: date,
) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
            doc_id,
            submit_date,
            xbrl_path
        FROM filings
        WHERE xbrl_path IS NOT NULL
          AND xbrl_path <> ''
          AND zip_path IS NOT NULL
          AND zip_path <> ''
          AND parse_status = 'derived_metrics_saved'
          AND accounting_standard IS NOT NULL
          AND accounting_standard <> ''
          AND document_display_unit IS NOT NULL
          AND document_display_unit <> ''
          AND submit_date IS NOT NULL
          AND submit_date <> ''
        ORDER BY submit_date ASC, doc_id ASC
        """
    ).fetchall()

    candidates: list[dict[str, Any]] = []
    for row in rows:
        submit_month = parse_submit_month(str(row["submit_date"] or ""))
        if submit_month is None or submit_month >= keep_from_month:
            continue
        candidates.append(dict(row))

    return candidates


def cleanup_old_xbrl_storage(
    conn: sqlite3.Connection,
    *,
    enabled: bool = XBRL_RETENTION_ENABLED,
    keep_months: int = XBRL_RETENTION_MONTHS,
    latest_month: date | None = None,
) -> dict[str, Any]:
    if not enabled:
        return {
            "status": "skipped",
            "reason": "disabled",
            "reference_month": "",
            "keep_from_month": "",
            "target_total": 0,
            "deleted_total": 0,
            "missing_file_total": 0,
            "error_total": 0,
            "sample_deleted_paths": [],
            "error": "",
        }

    reference_month = latest_month or detect_latest_filing_month(conn)
    if reference_month is None:
        return {
            "status": "skipped",
            "reason": "no_reference_month",
            "reference_month": "",
            "keep_from_month": "",
            "target_total": 0,
            "deleted_total": 0,
            "missing_file_total": 0,
            "error_total": 0,
            "sample_deleted_paths": [],
            "error": "",
        }

    keep_from_month = resolve_xbrl_keep_from_month(latest_month=reference_month, keep_months=keep_months)
    candidates = fetch_xbrl_cleanup_candidates(conn, keep_from_month=keep_from_month)

    deleted_total = 0
    missing_file_total = 0
    error_total = 0
    sample_deleted_paths: list[str] = []
    last_error = ""

    for row in candidates:
        doc_id = str(row.get("doc_id") or "")
        xbrl_path = Path(str(row.get("xbrl_path") or ""))

        try:
            if xbrl_path.exists():
                xbrl_path.unlink()
                deleted_total += 1
                if len(sample_deleted_paths) < 10:
                    sample_deleted_paths.append(str(xbrl_path))
            else:
                missing_file_total += 1

            conn.execute(
                """
                UPDATE filings
                SET xbrl_path = ''
                WHERE doc_id = ?
                """,
                (doc_id,),
            )
        except Exception as exc:
            error_total += 1
            last_error = repr(exc)

    conn.commit()

    return {
        "status": "completed" if error_total == 0 else "completed_with_errors",
        "reason": "",
        "reference_month": reference_month.strftime("%Y-%m"),
        "keep_from_month": keep_from_month.strftime("%Y-%m"),
        "target_total": len(candidates),
        "deleted_total": deleted_total,
        "missing_file_total": missing_file_total,
        "error_total": error_total,
        "sample_deleted_paths": sample_deleted_paths,
        "error": last_error,
    }

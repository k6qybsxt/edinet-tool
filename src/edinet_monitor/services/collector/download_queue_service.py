from __future__ import annotations

import sqlite3

from edinet_monitor.services.collector.document_filter_service import normalize_form_codes


def _form_code_filter(form_codes: tuple[str, ...] | list[str] | str | None) -> tuple[str, list[str]]:
    codes = normalize_form_codes(form_codes)
    if not codes:
        return "", []
    placeholders = ",".join("?" for _ in codes)
    return f" AND f.form_type IN ({placeholders})", list(codes)


def fetch_pending_filings(
    conn: sqlite3.Connection,
    limit: int = 10,
    *,
    form_codes: tuple[str, ...] | list[str] | str | None = None,
) -> list[sqlite3.Row]:
    cur = conn.cursor()
    form_filter, form_params = _form_code_filter(form_codes)
    cur.execute(
        f"""
        SELECT
            f.doc_id,
            f.edinet_code,
            f.security_code,
            f.form_type,
            f.submit_date
        FROM filings f
        INNER JOIN issuer_master im
            ON f.edinet_code = im.edinet_code
        WHERE f.download_status = 'pending'
          AND im.is_listed = 1
          AND im.exchange = 'TSE'
          {form_filter}
        ORDER BY f.submit_date ASC, f.doc_id ASC
        LIMIT ?
        """,
        (*form_params, limit),
    )
    return cur.fetchall()

def fetch_downloaded_filings_without_xbrl(
    conn: sqlite3.Connection,
    limit: int = 10,
    *,
    form_codes: tuple[str, ...] | list[str] | str | None = None,
) -> list[sqlite3.Row]:
    cur = conn.cursor()
    form_filter, form_params = _form_code_filter(form_codes)
    cur.execute(
        f"""
        SELECT
            f.doc_id,
            f.form_type,
            f.submit_date,
            f.zip_path,
            f.xbrl_path
        FROM filings f
        INNER JOIN issuer_master im
            ON f.edinet_code = im.edinet_code
        WHERE f.download_status = 'downloaded'
          AND (f.xbrl_path IS NULL OR f.xbrl_path = '')
          AND f.parse_status IN ('pending', 'xbrl_extract_error')
          AND im.is_listed = 1
          AND im.exchange = 'TSE'
          {form_filter}
        ORDER BY f.submit_date ASC, f.doc_id ASC
        LIMIT ?
        """,
        (*form_params, limit),
    )
    return cur.fetchall()

def mark_download_success(conn: sqlite3.Connection, doc_id: str, zip_path: str) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            zip_path = ?,
            download_status = 'downloaded'
        WHERE doc_id = ?
        """,
        (zip_path, doc_id),
    )
    conn.commit()


def mark_download_error(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            download_status = 'error'
        WHERE doc_id = ?
        """,
        (doc_id,),
    )
    conn.commit()


def reset_download_to_pending(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            zip_path = '',
            download_status = 'pending',
            parse_status = 'pending'
        WHERE doc_id = ?
        """,
        (doc_id,),
    )
    conn.commit()


def _has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row[1]) == column_name for row in rows)


def mark_xbrl_extract_success(
    conn: sqlite3.Connection,
    doc_id: str,
    xbrl_path: str,
    xbrl_member_name: str | None = None,
    period_end: str | None = None,
) -> None:
    period_end_text = str(period_end or "").strip()
    if _has_column(conn, "filings", "xbrl_member_name"):
        conn.execute(
            """
            UPDATE filings
            SET
                xbrl_path = ?,
                xbrl_member_name = ?,
                period_end = CASE WHEN ? <> '' THEN ? ELSE period_end END,
                parse_status = 'xbrl_ready'
            WHERE doc_id = ?
            """,
            (xbrl_path, xbrl_member_name or "", period_end_text, period_end_text, doc_id),
        )
        conn.commit()
        return

    conn.execute(
        """
        UPDATE filings
        SET
            xbrl_path = ?,
            period_end = CASE WHEN ? <> '' THEN ? ELSE period_end END,
            parse_status = 'xbrl_ready'
        WHERE doc_id = ?
        """,
        (xbrl_path, period_end_text, period_end_text, doc_id),
    )
    conn.commit()


def mark_xbrl_extract_error(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            parse_status = 'xbrl_extract_error'
        WHERE doc_id = ?
        """,
        (doc_id,),
    )
    conn.commit()

def fetch_xbrl_ready_filings(
    conn: sqlite3.Connection,
    limit: int = 10,
    *,
    form_codes: tuple[str, ...] | list[str] | str | None = None,
) -> list[sqlite3.Row]:
    cur = conn.cursor()
    member_column = "f.xbrl_member_name" if _has_column(conn, "filings", "xbrl_member_name") else "''"
    form_filter, form_params = _form_code_filter(form_codes)
    cur.execute(
        f"""
        SELECT
            f.doc_id,
            f.form_type,
            f.submit_date,
            f.xbrl_path,
            f.zip_path,
            {member_column} AS xbrl_member_name
        FROM filings f
        INNER JOIN issuer_master im
            ON f.edinet_code = im.edinet_code
        WHERE f.parse_status = 'xbrl_ready'
          AND f.xbrl_path IS NOT NULL
          AND f.xbrl_path <> ''
          AND im.is_listed = 1
          AND im.exchange = 'TSE'
          {form_filter}
        ORDER BY f.submit_date ASC, f.doc_id ASC
        LIMIT ?
        """,
        (*form_params, limit),
    )
    return cur.fetchall()

def mark_raw_facts_saved(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            parse_status = 'raw_facts_saved'
        WHERE doc_id = ?
        """,
        (doc_id,),
    )
    conn.commit()


def update_filing_parse_metadata(
    conn: sqlite3.Connection,
    doc_id: str,
    *,
    accounting_standard: str | None,
    document_display_unit: str | None,
) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            accounting_standard = ?,
            document_display_unit = ?
        WHERE doc_id = ?
        """,
        (
            str(accounting_standard or ""),
            str(document_display_unit or ""),
            doc_id,
        ),
    )
    conn.commit()


def mark_raw_facts_error(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            parse_status = 'raw_facts_error'
        WHERE doc_id = ?
        """,
        (doc_id,),
    )
    conn.commit()

def fetch_raw_facts_saved_filings(
    conn: sqlite3.Connection,
    limit: int = 10,
    *,
    form_codes: tuple[str, ...] | list[str] | str | None = None,
) -> list[sqlite3.Row]:
    cur = conn.cursor()
    form_filter, form_params = _form_code_filter(form_codes)
    cur.execute(
        f"""
        SELECT
            f.doc_id,
            f.edinet_code,
            f.security_code,
            f.form_type,
            im.industry_33,
            f.period_end,
            f.xbrl_path,
            f.zip_path
        FROM filings f
        INNER JOIN issuer_master im
            ON f.edinet_code = im.edinet_code
        WHERE f.parse_status = 'raw_facts_saved'
          AND im.is_listed = 1
          AND im.exchange = 'TSE'
          {form_filter}
        ORDER BY f.submit_date ASC, f.doc_id ASC
        LIMIT ?
        """,
        (*form_params, limit),
    )
    return cur.fetchall()

def mark_normalized_metrics_saved(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            parse_status = 'normalized_metrics_saved'
        WHERE doc_id = ?
        """,
        (doc_id,),
    )
    conn.commit()


def mark_normalized_metrics_error(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            parse_status = 'normalized_metrics_error'
        WHERE doc_id = ?
        """,
        (doc_id,),
    )
    conn.commit()


def fetch_derived_metrics_target_filings(
    conn: sqlite3.Connection,
    *,
    rule_version: str,
    limit: int = 10,
    form_codes: tuple[str, ...] | list[str] | str | None = None,
) -> list[sqlite3.Row]:
    cur = conn.cursor()
    form_filter, form_params = _form_code_filter(form_codes)
    cur.execute(
        f"""
        SELECT
            f.doc_id,
            f.edinet_code,
            f.security_code,
            f.form_type,
            f.period_end,
            im.industry_33,
            f.accounting_standard,
            f.document_display_unit,
            f.xbrl_path,
            f.parse_status,
            IFNULL(dm.metric_count, 0) AS derived_metric_count
        FROM filings f
        INNER JOIN issuer_master im
            ON f.edinet_code = im.edinet_code
        LEFT JOIN (
            SELECT
                doc_id,
                COUNT(*) AS metric_count
            FROM derived_metrics
            WHERE rule_version = ?
            GROUP BY doc_id
        ) dm
            ON f.doc_id = dm.doc_id
        WHERE im.is_listed = 1
          AND im.exchange = 'TSE'
          {form_filter}
          AND (
                f.parse_status IN ('normalized_metrics_saved', 'derived_metrics_error')
                OR (
                    f.parse_status = 'derived_metrics_saved'
                    AND IFNULL(dm.metric_count, 0) = 0
                )
          )
        ORDER BY f.submit_date ASC, f.doc_id ASC
        LIMIT ?
        """,
        (rule_version, *form_params, limit),
    )
    return cur.fetchall()


def mark_derived_metrics_saved(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            parse_status = 'derived_metrics_saved'
        WHERE doc_id = ?
        """,
        (doc_id,),
    )
    conn.commit()


def mark_derived_metrics_error(conn: sqlite3.Connection, doc_id: str) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            parse_status = 'derived_metrics_error'
        WHERE doc_id = ?
        """,
        (doc_id,),
    )
    conn.commit()

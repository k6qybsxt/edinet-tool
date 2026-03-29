from __future__ import annotations

import sqlite3


def fetch_pending_filings(conn: sqlite3.Connection, limit: int = 10) -> list[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            doc_id,
            edinet_code,
            security_code,
            form_type,
            submit_date
        FROM filings
        WHERE download_status = 'pending'
        ORDER BY submit_date ASC, doc_id ASC
        LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


def fetch_downloaded_filings_without_xbrl(conn: sqlite3.Connection, limit: int = 10) -> list[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            doc_id,
            submit_date,
            zip_path,
            xbrl_path
        FROM filings
        WHERE download_status = 'downloaded'
          AND (xbrl_path IS NULL OR xbrl_path = '')
        ORDER BY submit_date ASC, doc_id ASC
        LIMIT ?
        """,
        (limit,),
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


def mark_xbrl_extract_success(conn: sqlite3.Connection, doc_id: str, xbrl_path: str) -> None:
    conn.execute(
        """
        UPDATE filings
        SET
            xbrl_path = ?,
            parse_status = 'xbrl_ready'
        WHERE doc_id = ?
        """,
        (xbrl_path, doc_id),
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

def fetch_xbrl_ready_filings(conn: sqlite3.Connection, limit: int = 10) -> list[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            doc_id,
            xbrl_path
        FROM filings
        WHERE parse_status = 'xbrl_ready'
          AND xbrl_path IS NOT NULL
          AND xbrl_path <> ''
        ORDER BY submit_date ASC, doc_id ASC
        LIMIT ?
        """,
        (limit,),
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

def fetch_raw_facts_saved_filings(conn: sqlite3.Connection, limit: int = 10) -> list[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            doc_id,
            edinet_code,
            security_code
        FROM filings
        WHERE parse_status = 'raw_facts_saved'
        ORDER BY submit_date ASC, doc_id ASC
        LIMIT ?
        """,
        (limit,),
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
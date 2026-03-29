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
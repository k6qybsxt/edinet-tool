from __future__ import annotations

import sqlite3


def upsert_filing(conn: sqlite3.Connection, filing: dict) -> None:
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
            updated_at = excluded.updated_at
        """,
        filing,
    )


def upsert_filings(conn: sqlite3.Connection, filings: list[dict]) -> int:
    count = 0

    for filing in filings:
        upsert_filing(conn, filing)
        count += 1

    conn.commit()
    return count
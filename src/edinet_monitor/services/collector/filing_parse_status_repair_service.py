from __future__ import annotations

import sqlite3
from typing import Any


def repair_filing_parse_statuses(conn: sqlite3.Connection) -> dict[str, Any]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute(
        """
        WITH raw_doc_ids AS (
            SELECT DISTINCT doc_id
            FROM raw_facts
        ),
        normalized_doc_ids AS (
            SELECT DISTINCT doc_id
            FROM normalized_metrics
        ),
        derived_doc_ids AS (
            SELECT DISTINCT doc_id
            FROM derived_metrics
        )
        SELECT
            f.doc_id,
            f.parse_status AS current_parse_status,
            CASE
                WHEN dd.doc_id IS NOT NULL THEN 'derived_metrics_saved'
                WHEN nd.doc_id IS NOT NULL THEN 'normalized_metrics_saved'
                WHEN rd.doc_id IS NOT NULL THEN 'raw_facts_saved'
                WHEN f.xbrl_path IS NOT NULL AND f.xbrl_path <> '' THEN 'xbrl_ready'
                ELSE f.parse_status
            END AS repaired_parse_status
        FROM filings f
        LEFT JOIN raw_doc_ids rd
            ON rd.doc_id = f.doc_id
        LEFT JOIN normalized_doc_ids nd
            ON nd.doc_id = f.doc_id
        LEFT JOIN derived_doc_ids dd
            ON dd.doc_id = f.doc_id
        ORDER BY f.submit_date ASC, f.doc_id ASC
        """
    ).fetchall()

    updated_total = 0
    status_change_totals: dict[str, int] = {}

    for row in rows:
        current_status = str(row["current_parse_status"] or "")
        repaired_status = str(row["repaired_parse_status"] or "")
        if current_status == repaired_status:
            continue

        cur.execute(
            """
            UPDATE filings
            SET parse_status = ?
            WHERE doc_id = ?
            """,
            (repaired_status, row["doc_id"]),
        )
        updated_total += 1
        status_key = f"{current_status}->{repaired_status}"
        status_change_totals[status_key] = status_change_totals.get(status_key, 0) + 1

    conn.commit()

    return {
        "checked_total": len(rows),
        "updated_total": updated_total,
        "status_change_totals": dict(sorted(status_change_totals.items())),
    }

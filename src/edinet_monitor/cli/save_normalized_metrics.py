from __future__ import annotations

import sqlite3

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.download_queue_service import (
    fetch_raw_facts_saved_filings,
    mark_normalized_metrics_error,
    mark_normalized_metrics_saved,
)
from edinet_monitor.services.normalizer.metric_normalize_service import normalize_raw_fact_rows
from edinet_monitor.services.normalizer.normalized_metric_store_service import (
    delete_normalized_metrics_by_doc_id,
    insert_normalized_metrics,
)


def fetch_raw_fact_rows(conn: sqlite3.Connection, doc_id: str) -> list[dict]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
            doc_id,
            tag_name,
            context_ref,
            unit_ref,
            period_type,
            period_start,
            period_end,
            instant_date,
            consolidation,
            value_text
        FROM raw_facts
        WHERE doc_id = ?
        """,
        (doc_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def main() -> None:
    conn = get_connection()
    try:
        filings = fetch_raw_facts_saved_filings(conn, limit=5)
        print(f"raw_facts_saved_rows={len(filings)}")

        for filing in filings:
            doc_id = filing["doc_id"]
            edinet_code = filing["edinet_code"]
            security_code = filing["security_code"]

            print(f"[DEBUG] target_doc_id={doc_id}")

            try:
                raw_rows = fetch_raw_fact_rows(conn, doc_id)
                normalized_rows = normalize_raw_fact_rows(
                    raw_rows,
                    edinet_code=edinet_code,
                    security_code=security_code,
                )

                delete_normalized_metrics_by_doc_id(conn, doc_id)
                saved_count = insert_normalized_metrics(conn, normalized_rows)
                mark_normalized_metrics_saved(conn, doc_id)

                print(f"saved_normalized_metrics doc_id={doc_id} count={saved_count}")
            except Exception as e:
                mark_normalized_metrics_error(conn, doc_id)
                print(f"normalized_metrics_error doc_id={doc_id} error={repr(e)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
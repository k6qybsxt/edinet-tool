from __future__ import annotations

import argparse
import sqlite3
from typing import Any

from edinet_monitor.db.schema import create_tables, get_connection
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


def run_save_normalized_metrics(*, batch_size: int = 100) -> dict[str, Any]:
    create_tables()

    conn = get_connection()
    total_target = 0
    total_saved_docs = 0
    total_saved_rows = 0
    total_errors = 0
    loop_count = 0

    try:
        while True:
            filings = fetch_raw_facts_saved_filings(conn, limit=batch_size)
            print(f"raw_facts_saved_rows={len(filings)}")

            if not filings:
                break

            loop_count += 1
            total_target += len(filings)

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

                    print(
                        f"[DEBUG] doc_id={doc_id} raw_row_count={len(raw_rows)} normalized_row_count={len(normalized_rows)}"
                    )

                    delete_normalized_metrics_by_doc_id(conn, doc_id)
                    saved_count = insert_normalized_metrics(conn, normalized_rows)

                    if saved_count <= 0:
                        mark_normalized_metrics_error(conn, doc_id)
                        total_errors += 1
                        print(f"normalized_metrics_error doc_id={doc_id} error='saved_count=0'")
                        continue

                    mark_normalized_metrics_saved(conn, doc_id)
                    total_saved_docs += 1
                    total_saved_rows += saved_count
                    print(f"saved_normalized_metrics doc_id={doc_id} count={saved_count}")

                except Exception as e:
                    mark_normalized_metrics_error(conn, doc_id)
                    total_errors += 1
                    print(f"normalized_metrics_error doc_id={doc_id} error={repr(e)}")
    finally:
        conn.close()

    print(f"normalized_metrics_target_total={total_target}")
    print(f"normalized_metrics_saved_docs_total={total_saved_docs}")
    print(f"normalized_metrics_saved_rows_total={total_saved_rows}")
    print(f"normalized_metrics_error_total={total_errors}")

    return {
        "loop_count": loop_count,
        "target_total": total_target,
        "saved_docs_total": total_saved_docs,
        "saved_rows_total": total_saved_rows,
        "error_total": total_errors,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=100)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_save_normalized_metrics(batch_size=args.batch_size)


if __name__ == "__main__":
    main()

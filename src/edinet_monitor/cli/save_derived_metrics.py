from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

from edinet_monitor.config.settings import DEFAULT_DERIVED_METRICS_RULE_VERSION
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.collector.download_queue_service import (
    fetch_derived_metrics_target_filings,
    mark_derived_metrics_error,
    mark_derived_metrics_saved,
    update_filing_parse_metadata,
)
from edinet_monitor.services.derived_metrics.derived_metric_service import (
    calculate_derived_metrics,
)
from edinet_monitor.services.derived_metrics.derived_metric_store_service import (
    delete_derived_metrics_by_doc_id,
    insert_derived_metrics,
)
from edinet_monitor.services.parser.xbrl_parse_service import parse_xbrl_to_raw


def fetch_normalized_metric_rows(conn: sqlite3.Connection, doc_id: str) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT
            doc_id,
            edinet_code,
            security_code,
            metric_key,
            fiscal_year,
            period_end,
            value_num,
            source_tag,
            consolidation,
            rule_version
        FROM normalized_metrics
        WHERE doc_id = ?
        ORDER BY metric_key ASC
        """,
        (doc_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def ensure_filing_parse_metadata(
    conn: sqlite3.Connection,
    filing: dict[str, Any],
) -> dict[str, Any]:
    accounting_standard = str(filing.get("accounting_standard") or "")
    document_display_unit = str(filing.get("document_display_unit") or "")

    if accounting_standard and document_display_unit:
        return filing

    xbrl_path = str(filing.get("xbrl_path") or "")
    if not xbrl_path:
        return filing

    parsed = parse_xbrl_to_raw(Path(xbrl_path))
    parsed_meta = dict(parsed.get("meta") or {})
    parsed_out = dict(parsed.get("out") or {})

    accounting_standard = str(parsed_meta.get("accounting_standard") or accounting_standard)
    document_display_unit = str(
        parsed_meta.get("document_display_unit")
        or parsed_out.get("DocumentDisplayUnit")
        or document_display_unit
    )

    update_filing_parse_metadata(
        conn,
        str(filing["doc_id"]),
        accounting_standard=accounting_standard,
        document_display_unit=document_display_unit,
    )
    filing["accounting_standard"] = accounting_standard
    filing["document_display_unit"] = document_display_unit
    return filing


def run_save_derived_metrics(
    *,
    batch_size: int = 100,
    run_all: bool = False,
    rule_version: str = DEFAULT_DERIVED_METRICS_RULE_VERSION,
) -> dict[str, Any]:
    create_tables()

    conn = get_connection()
    total_target = 0
    total_saved_docs = 0
    total_saved_rows = 0
    total_errors = 0
    loop_count = 0

    try:
        while True:
            filings = fetch_derived_metrics_target_filings(
                conn,
                rule_version=rule_version,
                limit=batch_size,
            )
            print(f"derived_metrics_target_rows={len(filings)}")

            if not filings:
                break

            loop_count += 1
            total_target += len(filings)

            for filing_row in filings:
                filing = dict(filing_row)
                doc_id = str(filing["doc_id"])

                print(f"[DEBUG] target_doc_id={doc_id}")

                try:
                    filing = ensure_filing_parse_metadata(conn, filing)
                    normalized_rows = fetch_normalized_metric_rows(conn, doc_id)
                    derived_rows = calculate_derived_metrics(
                        normalized_rows,
                        form_type=str(filing.get("form_type") or ""),
                        accounting_standard=str(filing.get("accounting_standard") or ""),
                        document_display_unit=str(filing.get("document_display_unit") or ""),
                        rule_version=rule_version,
                    )

                    print(
                        f"[DEBUG] doc_id={doc_id} normalized_row_count={len(normalized_rows)} derived_row_count={len(derived_rows)}"
                    )

                    delete_derived_metrics_by_doc_id(conn, doc_id)
                    saved_count = insert_derived_metrics(conn, derived_rows)

                    if saved_count <= 0:
                        mark_derived_metrics_error(conn, doc_id)
                        total_errors += 1
                        print(f"derived_metrics_error doc_id={doc_id} error='saved_count=0'")
                        continue

                    mark_derived_metrics_saved(conn, doc_id)
                    total_saved_docs += 1
                    total_saved_rows += saved_count
                    print(f"saved_derived_metrics doc_id={doc_id} count={saved_count}")
                except Exception as e:
                    mark_derived_metrics_error(conn, doc_id)
                    total_errors += 1
                    print(f"derived_metrics_error doc_id={doc_id} error={repr(e)}")

            if not run_all:
                break
    finally:
        conn.close()

    print(f"derived_metrics_target_total={total_target}")
    print(f"derived_metrics_saved_docs_total={total_saved_docs}")
    print(f"derived_metrics_saved_rows_total={total_saved_rows}")
    print(f"derived_metrics_error_total={total_errors}")

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
    parser.add_argument("--run-all", action="store_true")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_save_derived_metrics(
        batch_size=args.batch_size,
        run_all=args.run_all,
    )


if __name__ == "__main__":
    main()

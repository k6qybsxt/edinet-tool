from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.collector.download_queue_service import (
    fetch_xbrl_ready_filings,
    mark_raw_facts_error,
    mark_raw_facts_saved,
    update_filing_parse_metadata,
)
from edinet_monitor.services.parser.raw_fact_mapper import to_raw_fact_rows
from edinet_monitor.services.parser.raw_fact_store_service import (
    delete_raw_facts_by_doc_id,
    insert_raw_facts,
)
from edinet_monitor.services.parser.xbrl_parse_service import parse_xbrl_to_raw


def run_save_raw_facts(*, batch_size: int = 20, run_all: bool = False) -> dict[str, Any]:
    create_tables()

    conn = get_connection()
    total_target = 0
    total_saved_docs = 0
    total_saved_rows = 0
    total_errors = 0
    loop_count = 0

    try:
        while True:
            rows = fetch_xbrl_ready_filings(conn, limit=batch_size)
            print(f"xbrl_ready_rows={len(rows)}")

            if not rows:
                break

            loop_count += 1
            total_target += len(rows)

            for row in rows:
                doc_id = row["doc_id"]
                xbrl_path = Path(row["xbrl_path"])

                print(f"[DEBUG] target_doc_id={doc_id}")
                print(f"[DEBUG] xbrl_path={xbrl_path}")

                try:
                    parsed = parse_xbrl_to_raw(xbrl_path)
                    raw_rows = to_raw_fact_rows(doc_id, parsed)
                    parsed_meta = dict(parsed.get("meta") or {})
                    parsed_out = dict(parsed.get("out") or {})
                    accounting_standard = str(parsed_meta.get("accounting_standard") or "")
                    document_display_unit = str(
                        parsed_meta.get("document_display_unit")
                        or parsed_out.get("DocumentDisplayUnit")
                        or ""
                    )

                    delete_raw_facts_by_doc_id(conn, doc_id)
                    saved_count = insert_raw_facts(conn, raw_rows)
                    update_filing_parse_metadata(
                        conn,
                        doc_id,
                        accounting_standard=accounting_standard,
                        document_display_unit=document_display_unit,
                    )
                    mark_raw_facts_saved(conn, doc_id)

                    total_saved_docs += 1
                    total_saved_rows += saved_count
                    print(f"saved_raw_facts doc_id={doc_id} count={saved_count}")
                except Exception as e:
                    mark_raw_facts_error(conn, doc_id)
                    total_errors += 1
                    print(f"raw_facts_error doc_id={doc_id} error={repr(e)}")

            if not run_all:
                break
    finally:
        conn.close()

    print(f"raw_facts_target_total={total_target}")
    print(f"raw_facts_saved_docs_total={total_saved_docs}")
    print(f"raw_facts_saved_rows_total={total_saved_rows}")
    print(f"raw_facts_error_total={total_errors}")

    return {
        "loop_count": loop_count,
        "target_total": total_target,
        "saved_docs_total": total_saved_docs,
        "saved_rows_total": total_saved_rows,
        "error_total": total_errors,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--run-all", action="store_true")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_save_raw_facts(
        batch_size=args.batch_size,
        run_all=args.run_all,
    )


if __name__ == "__main__":
    main()

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
from edinet_monitor.services.collector.document_filter_service import (
    is_half_form_type,
    normalize_form_codes,
)
from edinet_monitor.services.edinet_storage_path_service import resolve_storage_paths
from edinet_monitor.services.parser.raw_fact_mapper import to_raw_fact_rows
from edinet_monitor.services.parser.raw_fact_store_service import (
    delete_raw_facts_by_doc_id,
    insert_raw_facts,
)
from edinet_monitor.services.parser.xbrl_parse_service import parse_xbrl_to_raw
from edinet_monitor.services.performance_log_service import PerformanceLog


def _parse_mode_for_form_type(form_type: str) -> str:
    return "half" if is_half_form_type(form_type) else "full"


def run_save_raw_facts(
    *,
    batch_size: int = 20,
    run_all: bool = False,
    form_codes: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    create_tables()

    conn = get_connection()
    target_form_codes = normalize_form_codes(form_codes)
    perf_log = PerformanceLog(
        command_name="save_raw_facts",
        workers=1,
        batch_size=batch_size,
        parameters={
            "batch_size": batch_size,
            "run_all": bool(run_all),
            "form_codes": list(target_form_codes),
        },
    )
    total_target = 0
    total_saved_docs = 0
    total_saved_rows = 0
    total_errors = 0
    loop_count = 0
    unhandled_error: Exception | None = None

    try:
        while True:
            with perf_log.measure("db_read", "fetch_xbrl_ready_filings"):
                rows = fetch_xbrl_ready_filings(conn, limit=batch_size, form_codes=target_form_codes)
            print(f"xbrl_ready_rows={len(rows)}")

            if not rows:
                break

            loop_count += 1
            total_target += len(rows)

            for row in rows:
                doc_id = row["doc_id"]
                form_type = str(row["form_type"] or "")
                xbrl_path = Path(row["xbrl_path"])
                xbrl_member_name = str(row["xbrl_member_name"] or "")
                resolved = resolve_storage_paths(dict(row))
                if not xbrl_path.exists() and resolved.xbrl_path is not None:
                    xbrl_path = resolved.xbrl_path
                    with perf_log.measure("db_write", "update_resolved_storage_path"):
                        conn.execute(
                            """
                            UPDATE filings
                            SET xbrl_path = ?,
                                zip_path = CASE WHEN ? <> '' THEN ? ELSE zip_path END
                            WHERE doc_id = ?
                            """,
                            (
                                str(resolved.xbrl_path),
                                str(resolved.zip_path or ""),
                                str(resolved.zip_path or ""),
                                doc_id,
                            ),
                        )
                        conn.commit()

                print(f"[DEBUG] target_doc_id={doc_id}")
                print(f"[DEBUG] xbrl_path={xbrl_path}")

                try:
                    with perf_log.measure("parse", "parse_xbrl_to_raw_and_map"):
                        parsed = parse_xbrl_to_raw(xbrl_path, mode=_parse_mode_for_form_type(form_type))
                        raw_rows = to_raw_fact_rows(
                            doc_id,
                            parsed,
                            xbrl_member_name=xbrl_member_name,
                        )
                    parsed_meta = dict(parsed.get("meta") or {})
                    parsed_out = dict(parsed.get("out") or {})
                    accounting_standard = str(parsed_meta.get("accounting_standard") or "")
                    document_display_unit = str(
                        parsed_meta.get("document_display_unit")
                        or parsed_out.get("DocumentDisplayUnit")
                        or ""
                    )

                    with perf_log.measure("db_write", "save_raw_facts_doc"):
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
                    with perf_log.measure("db_write", "mark_raw_facts_error"):
                        mark_raw_facts_error(conn, doc_id)
                    total_errors += 1
                    print(f"raw_facts_error doc_id={doc_id} error={repr(e)}")

            if not run_all:
                break
    except Exception as e:
        unhandled_error = e
        raise
    finally:
        status = "error" if unhandled_error else ("completed_with_errors" if total_errors else "success")
        perf_log.finish(
            conn,
            status=status,
            target_total=total_target,
            success_total=total_saved_docs,
            error_total=total_errors,
            output_rows_total=total_saved_rows,
            error_summary={"unhandled_error": repr(unhandled_error)} if unhandled_error else {},
            summary={
                "loop_count": loop_count,
                "saved_rows_total": total_saved_rows,
            },
        )
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
    parser.add_argument("--form-codes", default="", help="Comma-separated form codes. Example: 043A00")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_save_raw_facts(
        batch_size=args.batch_size,
        run_all=args.run_all,
        form_codes=normalize_form_codes(args.form_codes or None),
    )


if __name__ == "__main__":
    main()

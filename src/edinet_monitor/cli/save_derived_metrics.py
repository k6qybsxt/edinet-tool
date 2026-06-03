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
from edinet_monitor.services.collector.document_filter_service import (
    is_half_form_type,
    normalize_form_codes,
)
from edinet_monitor.services.derived_metrics.derived_metric_service import (
    calculate_derived_metrics,
)
from edinet_monitor.services.derived_metrics.historical_growth_reference_service import (
    fetch_half_progress_annual_values_bulk,
    fetch_historical_growth_values_bulk,
)
from edinet_monitor.services.derived_metrics.derived_metric_store_service import (
    delete_derived_metrics_by_doc_id,
    insert_derived_metrics,
)
from edinet_monitor.services.parser.xbrl_parse_service import parse_xbrl_to_raw
from edinet_monitor.services.performance_log_service import PerformanceLog


RUN_ALL_TARGET_FETCH_LIMIT = 1_000_000


def _chunked(items: list[Any], chunk_size: int) -> list[list[Any]]:
    size = max(int(chunk_size or 1), 1)
    return [items[index:index + size] for index in range(0, len(items), size)]


def fetch_normalized_metric_rows(conn: sqlite3.Connection, doc_id: str) -> list[dict[str, Any]]:
    return fetch_normalized_metric_rows_by_doc_ids(conn, [doc_id]).get(str(doc_id), [])


def fetch_normalized_metric_rows_by_doc_ids(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    ordered_doc_ids = [str(doc_id) for doc_id in doc_ids if str(doc_id or "")]
    if not ordered_doc_ids:
        return {}

    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    grouped: dict[str, list[dict[str, Any]]] = {doc_id: [] for doc_id in ordered_doc_ids}
    for chunk in [ordered_doc_ids[index:index + 900] for index in range(0, len(ordered_doc_ids), 900)]:
        placeholders = ",".join("?" for _ in chunk)
        rows = cur.execute(
            f"""
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
            WHERE doc_id IN ({placeholders})
            ORDER BY doc_id ASC, metric_key ASC, period_end ASC, consolidation ASC
            """,
            chunk,
        ).fetchall()
        for row in rows:
            grouped.setdefault(str(row["doc_id"] or ""), []).append(dict(row))
    return grouped


def _count_historical_reference_values(
    values_by_doc_id: dict[str, dict[str, dict[int, dict[str, Any]]]]
) -> int:
    return sum(
        len(offset_values)
        for metric_values in values_by_doc_id.values()
        for offset_values in metric_values.values()
    )


def _count_half_progress_reference_values(
    values_by_doc_id: dict[str, dict[str, dict[str, Any]]]
) -> int:
    return sum(len(metric_values) for metric_values in values_by_doc_id.values())


def ensure_filing_parse_metadata(
    conn: sqlite3.Connection,
    filing: dict[str, Any],
    *,
    commit: bool = True,
) -> dict[str, Any]:
    accounting_standard = str(filing.get("accounting_standard") or "")
    document_display_unit = str(filing.get("document_display_unit") or "")

    if accounting_standard and document_display_unit:
        return filing

    xbrl_path = str(filing.get("xbrl_path") or "")
    if not xbrl_path:
        return filing

    parse_mode = "half" if is_half_form_type(filing.get("form_type")) else "full"
    parsed = parse_xbrl_to_raw(Path(xbrl_path), mode=parse_mode)
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
        commit=commit,
    )
    filing["accounting_standard"] = accounting_standard
    filing["document_display_unit"] = document_display_unit
    return filing


def run_save_derived_metrics(
    *,
    batch_size: int = 100,
    run_all: bool = False,
    form_codes: tuple[str, ...] | None = None,
    rule_version: str = DEFAULT_DERIVED_METRICS_RULE_VERSION,
) -> dict[str, Any]:
    create_tables()

    conn = get_connection()
    target_form_codes = normalize_form_codes(form_codes)
    perf_log = PerformanceLog(
        command_name="save_derived_metrics",
        workers=1,
        batch_size=batch_size,
        parameters={
            "batch_size": batch_size,
            "run_all": bool(run_all),
            "form_codes": list(target_form_codes),
            "rule_version": rule_version,
        },
    )
    total_target = 0
    total_saved_docs = 0
    total_saved_rows = 0
    total_errors = 0
    loop_count = 0
    bulk_reference_batch_count = 0
    normalized_input_rows_total = 0
    historical_reference_rows_total = 0
    half_progress_reference_rows_total = 0
    unhandled_error: Exception | None = None

    try:
        with perf_log.measure("db_read", "fetch_derived_metrics_target_filings"):
            target_filings = fetch_derived_metrics_target_filings(
                conn,
                rule_version=rule_version,
                limit=RUN_ALL_TARGET_FETCH_LIMIT if run_all else batch_size,
                form_codes=target_form_codes,
            )
        print(f"derived_metrics_target_rows={len(target_filings)}")

        filing_batches = _chunked([dict(row) for row in target_filings], batch_size)
        for filing_dicts in filing_batches:
            loop_count += 1
            total_target += len(filing_dicts)
            doc_ids = [str(filing["doc_id"]) for filing in filing_dicts]
            bulk_reference_batch_count += 1

            with perf_log.measure("db_read", "fetch_normalized_metric_rows_bulk"):
                normalized_rows_by_doc_id = fetch_normalized_metric_rows_by_doc_ids(conn, doc_ids)
            normalized_input_rows_total += sum(
                len(rows) for rows in normalized_rows_by_doc_id.values()
            )

            with perf_log.measure("db_read", "fetch_historical_growth_values_bulk"):
                historical_growth_values_by_doc_id = fetch_historical_growth_values_bulk(
                    conn,
                    filing_dicts,
                )
            historical_reference_rows_total += _count_historical_reference_values(
                historical_growth_values_by_doc_id
            )

            with perf_log.measure("db_read", "fetch_half_progress_annual_values_bulk"):
                half_progress_annual_values_by_doc_id = fetch_half_progress_annual_values_bulk(
                    conn,
                    filing_dicts,
                )
            half_progress_reference_rows_total += _count_half_progress_reference_values(
                half_progress_annual_values_by_doc_id
            )

            for filing in filing_dicts:
                doc_id = str(filing["doc_id"])

                print(f"[DEBUG] target_doc_id={doc_id}")

                try:
                    with perf_log.measure("parse", "ensure_filing_parse_metadata"):
                        filing = ensure_filing_parse_metadata(conn, filing, commit=False)
                    normalized_rows = normalized_rows_by_doc_id.get(doc_id, [])
                    historical_growth_values = historical_growth_values_by_doc_id.get(doc_id, {})
                    half_progress_annual_values = half_progress_annual_values_by_doc_id.get(doc_id, {})
                    with perf_log.measure("compute", "calculate_derived_metrics"):
                        derived_rows = calculate_derived_metrics(
                            normalized_rows,
                            form_type=str(filing.get("form_type") or ""),
                            industry_33=str(filing.get("industry_33") or ""),
                            accounting_standard=str(filing.get("accounting_standard") or ""),
                            document_display_unit=str(filing.get("document_display_unit") or ""),
                            rule_version=rule_version,
                            historical_growth_values=historical_growth_values,
                            half_progress_annual_values=half_progress_annual_values,
                        )

                    print(
                        f"[DEBUG] doc_id={doc_id} normalized_row_count={len(normalized_rows)} derived_row_count={len(derived_rows)}"
                    )

                    with perf_log.measure("db_write", "save_derived_metrics_doc"):
                        delete_derived_metrics_by_doc_id(conn, doc_id, commit=False)
                        saved_count = insert_derived_metrics(conn, derived_rows, commit=False)
                        if saved_count > 0:
                            mark_derived_metrics_saved(conn, doc_id, commit=False)
                            conn.commit()

                    if saved_count <= 0:
                        conn.rollback()
                        with perf_log.measure("db_write", "mark_derived_metrics_error"):
                            mark_derived_metrics_error(conn, doc_id)
                        total_errors += 1
                        print(f"derived_metrics_error doc_id={doc_id} error='saved_count=0'")
                        continue

                    total_saved_docs += 1
                    total_saved_rows += saved_count
                    print(f"saved_derived_metrics doc_id={doc_id} count={saved_count}")
                except Exception as e:
                    conn.rollback()
                    with perf_log.measure("db_write", "mark_derived_metrics_error"):
                        mark_derived_metrics_error(conn, doc_id)
                    total_errors += 1
                    print(f"derived_metrics_error doc_id={doc_id} error={repr(e)}")
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
                "rule_version": rule_version,
                "bulk_reference_batch_count": bulk_reference_batch_count,
                "normalized_input_rows": normalized_input_rows_total,
                "historical_reference_rows": historical_reference_rows_total,
                "half_progress_reference_rows": half_progress_reference_rows_total,
            },
        )
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
        "normalized_input_rows": normalized_input_rows_total,
        "historical_reference_rows": historical_reference_rows_total,
        "half_progress_reference_rows": half_progress_reference_rows_total,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--run-all", action="store_true")
    parser.add_argument("--form-codes", default="", help="Comma-separated form codes. Example: 043A00")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_save_derived_metrics(
        batch_size=args.batch_size,
        run_all=args.run_all,
        form_codes=normalize_form_codes(args.form_codes or None),
    )


if __name__ == "__main__":
    main()

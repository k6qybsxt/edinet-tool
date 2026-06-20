from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import sqlite3
import time
from typing import Any

from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.collector.download_queue_service import (
    fetch_raw_facts_saved_filings,
    mark_normalized_metrics_error,
    mark_normalized_metrics_saved,
)
from edinet_monitor.services.collector.document_filter_service import normalize_form_codes
from edinet_monitor.services.normalizer.metric_normalize_service import normalize_raw_fact_rows
from edinet_monitor.services.normalizer.normalized_metric_store_service import (
    delete_normalized_metrics_by_doc_id,
    insert_normalized_metrics,
)
from edinet_monitor.services.performance_log_service import PerformanceLog
from edinet_monitor.services.db_reflection_preflight_guard_service import (
    mark_db_reflection_preflight_guard_success,
    run_db_reflection_preflight_guard,
)


RAW_FACT_WANTED_COLUMNS = [
    "doc_id",
    "tag_name",
    "context_ref",
    "unit_ref",
    "period_type",
    "period_start",
    "period_end",
    "instant_date",
    "consolidation",
    "decimals",
    "is_nil",
    "context_dimensions_json",
    "unit_measures_json",
    "value_text",
]


def _chunked(items: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    size = max(int(chunk_size or 1), 1)
    return [items[index:index + size] for index in range(0, len(items), size)]


def fetch_raw_fact_rows_by_doc_ids(
    conn: sqlite3.Connection,
    doc_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    ordered_doc_ids = [str(doc_id) for doc_id in doc_ids if str(doc_id or "")]
    if not ordered_doc_ids:
        return {}

    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    raw_fact_columns = {
        str(row[1])
        for row in cur.execute("PRAGMA table_info(raw_facts)").fetchall()
    }
    select_parts = [
        column if column in raw_fact_columns else f"'' AS {column}"
        for column in RAW_FACT_WANTED_COLUMNS
    ]
    placeholders = ",".join("?" for _ in ordered_doc_ids)
    rows = cur.execute(
        f"""
        SELECT {", ".join(select_parts)}
        FROM raw_facts
        WHERE doc_id IN ({placeholders})
        ORDER BY doc_id ASC, id ASC
        """,
        ordered_doc_ids,
    ).fetchall()

    grouped = {doc_id: [] for doc_id in ordered_doc_ids}
    for row in rows:
        grouped.setdefault(str(row["doc_id"] or ""), []).append(dict(row))
    return grouped


def fetch_raw_fact_rows(conn: sqlite3.Connection, doc_id: str) -> list[dict]:
    return fetch_raw_fact_rows_by_doc_ids(conn, [doc_id]).get(str(doc_id), [])


def _normalize_metric_job(job: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    filing = dict(job.get("filing") or {})
    doc_id = str(filing.get("doc_id") or "")
    raw_rows = list(job.get("raw_rows") or [])
    try:
        normalized_rows = normalize_raw_fact_rows(
            raw_rows,
            edinet_code=str(filing.get("edinet_code") or ""),
            security_code=str(filing.get("security_code") or ""),
            industry_33=str(filing.get("industry_33") or ""),
            xbrl_path=str(filing.get("xbrl_path") or ""),
            zip_path=str(filing.get("zip_path") or ""),
            filing_period_end=str(filing.get("period_end") or ""),
            form_type=str(filing.get("form_type") or ""),
            enable_period_fallback=bool(job.get("enable_period_fallback")),
            enforce_candidate_validation=bool(job.get("enforce_candidate_validation")),
        )
        return {
            "ok": True,
            "order_index": int(job.get("order_index", 0) or 0),
            "doc_id": doc_id,
            "raw_row_count": len(raw_rows),
            "normalized_rows": normalized_rows,
            "elapsed_seconds": round(max(time.perf_counter() - started, 0.0), 6),
            "error": "",
        }
    except Exception as e:
        return {
            "ok": False,
            "order_index": int(job.get("order_index", 0) or 0),
            "doc_id": doc_id,
            "raw_row_count": len(raw_rows),
            "normalized_rows": [],
            "elapsed_seconds": round(max(time.perf_counter() - started, 0.0), 6),
            "error": repr(e),
        }


def _normalize_metric_chunk(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_normalize_metric_job(job) for job in jobs]


def _run_normalize_jobs(
    jobs: list[dict[str, Any]],
    *,
    workers: int,
    normalize_chunk_size: int,
    executor: ProcessPoolExecutor | None = None,
) -> tuple[list[dict[str, Any]], int, float]:
    chunks = _chunked(jobs, normalize_chunk_size)
    if not chunks:
        return [], 0, 0.0

    results: list[dict[str, Any]] = []
    if workers <= 1:
        for chunk in chunks:
            results.extend(_normalize_metric_chunk(chunk))
    else:
        owns_executor = executor is None
        target_executor = executor or ProcessPoolExecutor(max_workers=int(workers))
        try:
            future_to_chunk = {
                target_executor.submit(_normalize_metric_chunk, chunk): chunk
                for chunk in chunks
            }
            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    results.extend(future.result())
                except Exception as e:
                    for job in chunk:
                        filing = dict(job.get("filing") or {})
                        results.append(
                            {
                                "ok": False,
                                "order_index": int(job.get("order_index", 0) or 0),
                                "doc_id": str(filing.get("doc_id") or ""),
                                "raw_row_count": len(list(job.get("raw_rows") or [])),
                                "normalized_rows": [],
                                "elapsed_seconds": 0.0,
                                "error": repr(e),
                            }
                        )
        finally:
            if owns_executor:
                target_executor.shutdown()

    results.sort(key=lambda result: int(result.get("order_index", 0) or 0))
    worker_elapsed_total = round(
        sum(float(result.get("elapsed_seconds", 0.0) or 0.0) for result in results),
        6,
    )
    return results, len(chunks), worker_elapsed_total


def run_save_normalized_metrics(
    *,
    batch_size: int = 100,
    form_codes: tuple[str, ...] | None = None,
    enable_period_fallback: bool = False,
    enforce_candidate_validation: bool = False,
    workers: int = 1,
    normalize_chunk_size: int = 5,
) -> dict[str, Any]:
    create_tables()

    conn = get_connection()
    target_form_codes = normalize_form_codes(form_codes)
    target_workers = max(int(workers or 1), 1)
    target_normalize_chunk_size = max(int(normalize_chunk_size or 1), 1)
    normalize_window_size = max(target_workers * target_normalize_chunk_size, 1)
    perf_log = PerformanceLog(
        command_name="save_normalized_metrics",
        workers=target_workers,
        batch_size=batch_size,
        parameters={
            "batch_size": batch_size,
            "form_codes": list(target_form_codes),
            "enable_period_fallback": bool(enable_period_fallback),
            "enforce_candidate_validation": bool(enforce_candidate_validation),
            "workers": target_workers,
            "normalize_chunk_size": target_normalize_chunk_size,
            "normalize_window_size": normalize_window_size,
        },
    )
    total_target = 0
    total_saved_docs = 0
    total_saved_rows = 0
    total_errors = 0
    loop_count = 0
    raw_facts_row_total = 0
    normalize_window_count = 0
    normalize_chunk_count_total = 0
    worker_normalize_elapsed_seconds_total = 0.0
    normalize_executor: ProcessPoolExecutor | None = None
    unhandled_error: Exception | None = None

    try:
        while True:
            with perf_log.measure("db_read", "fetch_raw_facts_saved_filings"):
                filings = fetch_raw_facts_saved_filings(conn, limit=batch_size, form_codes=target_form_codes)
            print(f"raw_facts_saved_rows={len(filings)}")

            if not filings:
                break

            loop_count += 1
            total_target += len(filings)

            filing_dicts = [dict(filing_row) for filing_row in filings]
            for filing_window in _chunked(filing_dicts, normalize_window_size):
                normalize_window_count += 1
                doc_ids = [str(filing.get("doc_id") or "") for filing in filing_window]
                with perf_log.measure(
                    "db_read",
                    "fetch_raw_fact_rows_bulk",
                    count_total=len(doc_ids),
                ):
                    raw_rows_by_doc_id = fetch_raw_fact_rows_by_doc_ids(conn, doc_ids)
                window_raw_row_total = sum(len(rows) for rows in raw_rows_by_doc_id.values())
                raw_facts_row_total += window_raw_row_total

                normalize_jobs = [
                    {
                        "order_index": order_index,
                        "filing": filing,
                        "raw_rows": raw_rows_by_doc_id.get(str(filing.get("doc_id") or ""), []),
                        "enable_period_fallback": bool(enable_period_fallback),
                        "enforce_candidate_validation": bool(enforce_candidate_validation),
                    }
                    for order_index, filing in enumerate(filing_window)
                ]
                if target_workers > 1 and normalize_executor is None:
                    normalize_executor = ProcessPoolExecutor(max_workers=target_workers)
                with perf_log.measure(
                    "compute",
                    "normalize_raw_fact_rows",
                    count_total=len(normalize_jobs),
                ):
                    normalize_results, normalize_chunk_count, worker_elapsed = _run_normalize_jobs(
                        normalize_jobs,
                        workers=target_workers,
                        normalize_chunk_size=target_normalize_chunk_size,
                        executor=normalize_executor,
                    )
                normalize_chunk_count_total += normalize_chunk_count
                worker_normalize_elapsed_seconds_total = round(
                    worker_normalize_elapsed_seconds_total + worker_elapsed,
                    6,
                )

                for normalize_result in normalize_results:
                    doc_id = str(normalize_result.get("doc_id") or "")
                    raw_row_count = int(normalize_result.get("raw_row_count", 0) or 0)
                    try:
                        if not normalize_result.get("ok"):
                            raise RuntimeError(
                                str(normalize_result.get("error") or "normalization failed")
                            )
                        normalized_rows = list(normalize_result.get("normalized_rows") or [])
                        print(
                            f"[DEBUG] doc_id={doc_id} raw_row_count={raw_row_count} "
                            f"normalized_row_count={len(normalized_rows)}"
                        )

                        with perf_log.measure("db_write", "save_normalized_metrics_doc"):
                            delete_normalized_metrics_by_doc_id(conn, doc_id, commit=False)
                            saved_count = insert_normalized_metrics(conn, normalized_rows, commit=False)
                            if saved_count > 0:
                                mark_normalized_metrics_saved(conn, doc_id, commit=False)
                                conn.commit()

                        if saved_count <= 0:
                            conn.rollback()
                            with perf_log.measure("db_write", "mark_normalized_metrics_error"):
                                mark_normalized_metrics_error(conn, doc_id)
                            total_errors += 1
                            print(f"normalized_metrics_error doc_id={doc_id} error='saved_count=0'")
                            continue

                        total_saved_docs += 1
                        total_saved_rows += saved_count
                        print(f"saved_normalized_metrics doc_id={doc_id} count={saved_count}")

                    except Exception as e:
                        conn.rollback()
                        with perf_log.measure("db_write", "mark_normalized_metrics_error"):
                            mark_normalized_metrics_error(conn, doc_id)
                        total_errors += 1
                        print(f"normalized_metrics_error doc_id={doc_id} error={repr(e)}")
    except Exception as e:
        unhandled_error = e
        raise
    finally:
        if normalize_executor is not None:
            normalize_executor.shutdown()
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
                "enable_period_fallback": int(enable_period_fallback),
                "enforce_candidate_validation": int(enforce_candidate_validation),
                "raw_facts_row_total": raw_facts_row_total,
                "normalize_window_count": normalize_window_count,
                "normalize_chunk_count": normalize_chunk_count_total,
                "worker_normalize_elapsed_seconds_total": worker_normalize_elapsed_seconds_total,
            },
        )
        conn.close()

    print(f"normalized_metrics_target_total={total_target}")
    print(f"normalized_metrics_saved_docs_total={total_saved_docs}")
    print(f"normalized_metrics_saved_rows_total={total_saved_rows}")
    print(f"normalized_metrics_error_total={total_errors}")
    print(f"enable_period_fallback={int(enable_period_fallback)}")
    print(f"enforce_candidate_validation={int(enforce_candidate_validation)}")

    return {
        "loop_count": loop_count,
        "target_total": total_target,
        "saved_docs_total": total_saved_docs,
        "saved_rows_total": total_saved_rows,
        "error_total": total_errors,
        "enable_period_fallback": int(enable_period_fallback),
        "enforce_candidate_validation": int(enforce_candidate_validation),
        "workers": target_workers,
        "normalize_chunk_size": target_normalize_chunk_size,
        "normalize_window_size": normalize_window_size,
        "raw_facts_row_total": raw_facts_row_total,
        "normalize_window_count": normalize_window_count,
        "normalize_chunk_count": normalize_chunk_count_total,
        "worker_normalize_elapsed_seconds_total": worker_normalize_elapsed_seconds_total,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--form-codes", default="", help="Comma-separated form codes. Example: 043A00")
    parser.add_argument("--enable-period-fallback", action="store_true")
    parser.add_argument("--enforce-candidate-validation", action="store_true")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--normalize-chunk-size", type=int, default=5)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    guard_result = run_db_reflection_preflight_guard(cli_name="save_normalized_metrics")
    run_save_normalized_metrics(
        batch_size=args.batch_size,
        form_codes=normalize_form_codes(args.form_codes or None),
        enable_period_fallback=args.enable_period_fallback,
        enforce_candidate_validation=args.enforce_candidate_validation,
        workers=args.workers,
        normalize_chunk_size=args.normalize_chunk_size,
    )
    mark_db_reflection_preflight_guard_success(guard_result)


if __name__ == "__main__":
    main()

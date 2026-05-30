from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import time
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


def _chunked(items: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    size = max(int(chunk_size or 1), 1)
    return [items[index:index + size] for index in range(0, len(items), size)]


def _parse_raw_fact_job(job: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    doc_id = str(job.get("doc_id") or "")
    form_type = str(job.get("form_type") or "")
    xbrl_path = Path(str(job.get("xbrl_path") or ""))
    xbrl_member_name = str(job.get("xbrl_member_name") or "")
    try:
        parsed = parse_xbrl_to_raw(xbrl_path, mode=_parse_mode_for_form_type(form_type))
        raw_rows = to_raw_fact_rows(
            doc_id,
            parsed,
            xbrl_member_name=xbrl_member_name,
        )
        parsed_meta = dict(parsed.get("meta") or {})
        parsed_out = dict(parsed.get("out") or {})
        return {
            "ok": True,
            "order_index": int(job.get("order_index", 0) or 0),
            "doc_id": doc_id,
            "raw_rows": raw_rows,
            "accounting_standard": str(parsed_meta.get("accounting_standard") or ""),
            "document_display_unit": str(
                parsed_meta.get("document_display_unit")
                or parsed_out.get("DocumentDisplayUnit")
                or ""
            ),
            "elapsed_seconds": round(max(time.perf_counter() - started, 0.0), 6),
            "error": "",
        }
    except Exception as e:
        return {
            "ok": False,
            "order_index": int(job.get("order_index", 0) or 0),
            "doc_id": doc_id,
            "raw_rows": [],
            "accounting_standard": "",
            "document_display_unit": "",
            "elapsed_seconds": round(max(time.perf_counter() - started, 0.0), 6),
            "error": repr(e),
        }


def _parse_raw_fact_chunk(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_parse_raw_fact_job(job) for job in jobs]


def _run_parse_jobs(
    jobs: list[dict[str, Any]],
    *,
    workers: int,
    parse_chunk_size: int,
) -> tuple[list[dict[str, Any]], int, float]:
    chunks = _chunked(jobs, parse_chunk_size)
    if not chunks:
        return [], 0, 0.0

    results: list[dict[str, Any]] = []
    if workers <= 1:
        for chunk in chunks:
            results.extend(_parse_raw_fact_chunk(chunk))
    else:
        with ProcessPoolExecutor(max_workers=int(workers)) as executor:
            future_to_chunk = {
                executor.submit(_parse_raw_fact_chunk, chunk): chunk
                for chunk in chunks
            }
            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    results.extend(future.result())
                except Exception as e:
                    for job in chunk:
                        results.append(
                            {
                                "ok": False,
                                "order_index": int(job.get("order_index", 0) or 0),
                                "doc_id": str(job.get("doc_id") or ""),
                                "raw_rows": [],
                                "accounting_standard": "",
                                "document_display_unit": "",
                                "elapsed_seconds": 0.0,
                                "error": repr(e),
                            }
                        )

    results.sort(key=lambda result: int(result.get("order_index", 0) or 0))
    worker_elapsed_total = round(
        sum(float(result.get("elapsed_seconds", 0.0) or 0.0) for result in results),
        6,
    )
    return results, len(chunks), worker_elapsed_total


def run_save_raw_facts(
    *,
    batch_size: int = 20,
    run_all: bool = False,
    form_codes: tuple[str, ...] | None = None,
    workers: int = 1,
    parse_chunk_size: int = 5,
) -> dict[str, Any]:
    create_tables()

    conn = get_connection()
    target_form_codes = normalize_form_codes(form_codes)
    perf_log = PerformanceLog(
        command_name="save_raw_facts",
        workers=max(int(workers or 1), 1),
        batch_size=batch_size,
        parameters={
            "batch_size": batch_size,
            "run_all": bool(run_all),
            "form_codes": list(target_form_codes),
            "workers": max(int(workers or 1), 1),
            "parse_chunk_size": max(int(parse_chunk_size or 1), 1),
        },
    )
    target_workers = max(int(workers or 1), 1)
    target_parse_chunk_size = max(int(parse_chunk_size or 1), 1)
    total_target = 0
    total_saved_docs = 0
    total_saved_rows = 0
    total_errors = 0
    loop_count = 0
    parse_chunk_count_total = 0
    worker_parse_elapsed_seconds_total = 0.0
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

            parse_jobs: list[dict[str, Any]] = []
            for order_index, row in enumerate(rows):
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

                parse_jobs.append(
                    {
                        "order_index": order_index,
                        "doc_id": str(doc_id),
                        "form_type": form_type,
                        "xbrl_path": str(xbrl_path),
                        "xbrl_member_name": xbrl_member_name,
                    }
                )

            with perf_log.measure("parse", "parse_xbrl_to_raw_and_map", count_total=len(parse_jobs)):
                parse_results, parse_chunk_count, worker_parse_elapsed = _run_parse_jobs(
                    parse_jobs,
                    workers=target_workers,
                    parse_chunk_size=target_parse_chunk_size,
                )
            parse_chunk_count_total += parse_chunk_count
            worker_parse_elapsed_seconds_total = round(
                worker_parse_elapsed_seconds_total + worker_parse_elapsed,
                6,
            )

            for parse_result in parse_results:
                doc_id = str(parse_result.get("doc_id") or "")
                try:
                    if not parse_result.get("ok"):
                        raise RuntimeError(str(parse_result.get("error") or "parse failed"))
                    raw_rows = list(parse_result.get("raw_rows") or [])
                    accounting_standard = str(parse_result.get("accounting_standard") or "")
                    document_display_unit = str(parse_result.get("document_display_unit") or "")

                    with perf_log.measure("db_write", "save_raw_facts_doc"):
                        delete_raw_facts_by_doc_id(conn, doc_id, commit=False)
                        saved_count = insert_raw_facts(conn, raw_rows, commit=False)
                        update_filing_parse_metadata(
                            conn,
                            doc_id,
                            accounting_standard=accounting_standard,
                            document_display_unit=document_display_unit,
                            commit=False,
                        )
                        mark_raw_facts_saved(conn, doc_id, commit=False)
                        conn.commit()

                    total_saved_docs += 1
                    total_saved_rows += saved_count
                    print(f"saved_raw_facts doc_id={doc_id} count={saved_count}")
                except Exception as e:
                    conn.rollback()
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
                "worker_parse_elapsed_seconds_total": worker_parse_elapsed_seconds_total,
                "parse_chunk_count": parse_chunk_count_total,
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
        "workers": target_workers,
        "parse_chunk_size": target_parse_chunk_size,
        "parse_chunk_count": parse_chunk_count_total,
        "worker_parse_elapsed_seconds_total": worker_parse_elapsed_seconds_total,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--run-all", action="store_true")
    parser.add_argument("--form-codes", default="", help="Comma-separated form codes. Example: 043A00")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--parse-chunk-size", type=int, default=5)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_save_raw_facts(
        batch_size=args.batch_size,
        run_all=args.run_all,
        form_codes=normalize_form_codes(args.form_codes or None),
        workers=args.workers,
        parse_chunk_size=args.parse_chunk_size,
    )


if __name__ == "__main__":
    main()

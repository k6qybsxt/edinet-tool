from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import time
from typing import Any

from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.download_queue_service import (
    fetch_downloaded_filings_without_xbrl,
    mark_xbrl_extract_error,
    mark_xbrl_extract_success,
)
from edinet_monitor.services.collector.document_filter_service import normalize_form_codes
from edinet_monitor.services.edinet_storage_path_service import resolve_storage_paths
from edinet_monitor.services.performance_log_service import PerformanceLog
from edinet_monitor.services.segment_scope_service import fetch_segment_scope_filings
from edinet_monitor.services.storage.path_service import build_xbrl_save_path
from edinet_monitor.services.storage.zip_extract_service import (
    extract_period_end_from_xbrl_member_name,
    extract_preferred_xbrl,
)


def _chunked(items: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    size = max(int(chunk_size or 1), 1)
    return [items[index:index + size] for index in range(0, len(items), size)]


def _windowed(items: list[dict[str, Any]], window_size: int) -> list[list[dict[str, Any]]]:
    size = max(int(window_size or 1), 1)
    return [items[index:index + size] for index in range(0, len(items), size)]


def _extract_xbrl_job(job: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    doc_id = str(job.get("doc_id") or "")
    try:
        extracted = extract_preferred_xbrl(
            Path(str(job.get("zip_path") or "")),
            Path(str(job.get("xbrl_path") or "")),
            form_type=str(job.get("form_type") or ""),
        )
        return {
            "ok": True,
            "order_index": int(job.get("order_index", 0) or 0),
            "doc_id": doc_id,
            "zip_path": str(job.get("zip_path") or ""),
            "xbrl_path": str(extracted.output_path),
            "member_name": extracted.member_name,
            "member_names": list(extracted.member_names),
            "period_end": extract_period_end_from_xbrl_member_name(extracted.member_name),
            "elapsed_seconds": round(max(time.perf_counter() - started, 0.0), 6),
            "error": "",
        }
    except Exception as e:
        return {
            "ok": False,
            "order_index": int(job.get("order_index", 0) or 0),
            "doc_id": doc_id,
            "zip_path": str(job.get("zip_path") or ""),
            "xbrl_path": str(job.get("xbrl_path") or ""),
            "member_name": "",
            "member_names": [],
            "period_end": "",
            "elapsed_seconds": round(max(time.perf_counter() - started, 0.0), 6),
            "error": repr(e),
        }


def _extract_xbrl_chunk(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_extract_xbrl_job(job) for job in jobs]


def _run_extract_jobs(
    jobs: list[dict[str, Any]],
    *,
    workers: int,
    extract_chunk_size: int,
    executor: ThreadPoolExecutor | None = None,
) -> tuple[list[dict[str, Any]], int, float]:
    chunks = _chunked(jobs, extract_chunk_size)
    if not chunks:
        return [], 0, 0.0

    results: list[dict[str, Any]] = []
    if workers <= 1:
        for chunk in chunks:
            results.extend(_extract_xbrl_chunk(chunk))
    else:
        owned_executor = executor is None
        active_executor = executor or ThreadPoolExecutor(max_workers=int(workers))
        try:
            future_to_chunk = {
                active_executor.submit(_extract_xbrl_chunk, chunk): chunk
                for chunk in chunks
            }
            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    results.extend(future.result())
                except Exception as e:
                    for job in chunk:
                        failed = _extract_xbrl_job_result_for_error(job, e)
                        results.append(failed)
        finally:
            if owned_executor:
                active_executor.shutdown()

    results.sort(key=lambda result: int(result.get("order_index", 0) or 0))
    worker_elapsed_total = round(
        sum(float(result.get("elapsed_seconds", 0.0) or 0.0) for result in results),
        6,
    )
    return results, len(chunks), worker_elapsed_total


def _extract_xbrl_job_result_for_error(job: dict[str, Any], error: Exception) -> dict[str, Any]:
    return {
        "ok": False,
        "order_index": int(job.get("order_index", 0) or 0),
        "doc_id": str(job.get("doc_id") or ""),
        "zip_path": str(job.get("zip_path") or ""),
        "xbrl_path": str(job.get("xbrl_path") or ""),
        "member_name": "",
        "member_names": [],
        "period_end": "",
        "elapsed_seconds": 0.0,
        "error": repr(error),
    }


def run_extract_xbrl_from_zips(
    *,
    batch_size: int = 20,
    run_all: bool = False,
    form_codes: tuple[str, ...] | None = None,
    period_ranks: str | None = None,
    codes: tuple[str, ...] | None = None,
    force: bool = False,
    workers: int = 1,
    extract_chunk_size: int = 5,
) -> dict[str, Any]:
    conn = get_connection()
    target_form_codes = normalize_form_codes(form_codes)
    target_workers = max(int(workers or 1), 1)
    target_extract_chunk_size = max(int(extract_chunk_size or 1), 1)
    perf_log = PerformanceLog(
        command_name="extract_xbrl_from_zips",
        workers=target_workers,
        batch_size=batch_size,
        parameters={
            "batch_size": batch_size,
            "run_all": bool(run_all),
            "form_codes": list(target_form_codes),
            "period_ranks": period_ranks or "",
            "codes": list(codes or ()),
            "force": bool(force),
            "workers": target_workers,
            "extract_chunk_size": target_extract_chunk_size,
        },
    )
    executor = ThreadPoolExecutor(max_workers=target_workers) if target_workers > 1 else None
    total_target = 0
    total_extracted = 0
    total_errors = 0
    loop_count = 0
    extract_chunk_count_total = 0
    worker_extract_elapsed_seconds_total = 0.0
    attempted_doc_ids: set[str] = set()
    unhandled_error: Exception | None = None

    def mark_error(doc_id: str, error: str) -> None:
        nonlocal total_errors
        conn.rollback()
        with perf_log.measure("db_write", "mark_xbrl_extract_error"):
            mark_xbrl_extract_error(conn, doc_id)
        total_errors += 1
        print(f"extract_error doc_id={doc_id} error={error}")

    def update_resolved_zip_path(doc_id: str, old_zip_path: str, zip_path: Path) -> None:
        if str(zip_path) == old_zip_path:
            return
        with perf_log.measure("db_write", "update_resolved_zip_path"):
            conn.execute(
                "UPDATE filings SET zip_path = ? WHERE doc_id = ?",
                (str(zip_path), doc_id),
            )
            conn.commit()

    def prepare_jobs(rows: list[Any], *, period_scope: bool) -> list[dict[str, Any]]:
        nonlocal total_extracted
        jobs: list[dict[str, Any]] = []
        for order_index, original_row in enumerate(rows):
            row = dict(original_row)
            doc_id = str(row.get("doc_id") or "")
            resolved = resolve_storage_paths(row)
            if resolved.zip_path is None:
                mark_error(doc_id, "zip_missing")
                continue
            update_resolved_zip_path(doc_id, str(row.get("zip_path") or ""), resolved.zip_path)
            if period_scope and resolved.xbrl_path is not None and not force:
                total_extracted += 1
                print(f"existing_xbrl doc_id={doc_id} xbrl_path={resolved.xbrl_path}")
                continue
            jobs.append(
                {
                    "order_index": order_index,
                    "doc_id": doc_id,
                    "form_type": str(row.get("form_type") or ""),
                    "zip_path": str(resolved.zip_path),
                    "xbrl_path": str(build_xbrl_save_path(str(row.get("submit_date") or ""), doc_id)),
                }
            )
        return jobs

    def extract_and_save(jobs: list[dict[str, Any]]) -> None:
        nonlocal total_extracted
        nonlocal total_errors
        nonlocal extract_chunk_count_total
        nonlocal worker_extract_elapsed_seconds_total
        window_size = target_workers * target_extract_chunk_size
        for window in _windowed(jobs, window_size):
            with perf_log.measure("file_io", "inspect_and_extract_xbrl", count_total=len(window)):
                extract_results, extract_chunk_count, worker_elapsed = _run_extract_jobs(
                    window,
                    workers=target_workers,
                    extract_chunk_size=target_extract_chunk_size,
                    executor=executor,
                )
            extract_chunk_count_total += extract_chunk_count
            worker_extract_elapsed_seconds_total = round(
                worker_extract_elapsed_seconds_total + worker_elapsed,
                6,
            )
            for extracted in extract_results:
                doc_id = str(extracted.get("doc_id") or "")
                print(f"[DEBUG] target_doc_id={doc_id}")
                print(f"[DEBUG] zip_path={extracted.get('zip_path')}")
                print(f"[DEBUG] xbrl_members={list(extracted.get('member_names') or [])[:5]}")
                if not extracted.get("ok"):
                    mark_error(doc_id, str(extracted.get("error") or "extract failed"))
                    continue
                try:
                    with perf_log.measure("db_write", "mark_xbrl_extract_success"):
                        mark_xbrl_extract_success(
                            conn,
                            doc_id,
                            str(extracted.get("xbrl_path") or ""),
                            str(extracted.get("member_name") or ""),
                            period_end=str(extracted.get("period_end") or ""),
                            commit=False,
                        )
                        conn.commit()
                    total_extracted += 1
                    print(
                        f"extracted doc_id={doc_id} "
                        f"xbrl_path={extracted.get('xbrl_path')} "
                        f"xbrl_member_name={extracted.get('member_name')}"
                    )
                except Exception as e:
                    mark_error(doc_id, repr(e))

    try:
        if period_ranks:
            with perf_log.measure("db_read", "fetch_segment_scope_filings"):
                rows = fetch_segment_scope_filings(
                    conn,
                    form_codes=target_form_codes,
                    period_ranks=period_ranks,
                    codes=list(codes or ()),
                )
            print(f"period_rank_scope_rows={len(rows)}")
            total_target = len(rows)
            loop_count = 1
            extract_and_save(prepare_jobs(rows, period_scope=True))
        else:
            while True:
                with perf_log.measure("db_read", "fetch_downloaded_filings_without_xbrl"):
                    rows = fetch_downloaded_filings_without_xbrl(
                        conn,
                        limit=batch_size,
                        form_codes=target_form_codes,
                        exclude_doc_ids=attempted_doc_ids,
                    )
                print(f"downloaded_rows_without_xbrl={len(rows)}")

                if not rows:
                    break

                loop_count += 1
                total_target += len(rows)
                attempted_doc_ids.update(str(row["doc_id"]) for row in rows)
                extract_and_save(prepare_jobs(rows, period_scope=False))

                if not run_all:
                    break
    except Exception as e:
        unhandled_error = e
        raise
    finally:
        if executor is not None:
            executor.shutdown()
        status = "error" if unhandled_error else ("completed_with_errors" if total_errors else "success")
        perf_log.finish(
            conn,
            status=status,
            target_total=total_target,
            success_total=total_extracted,
            skipped_total=max(total_target - total_extracted - total_errors, 0),
            error_total=total_errors,
            error_summary={"unhandled_error": repr(unhandled_error)} if unhandled_error else {},
            summary={
                "loop_count": loop_count,
                "extract_chunk_count": extract_chunk_count_total,
                "worker_extract_elapsed_seconds_total": worker_extract_elapsed_seconds_total,
            },
        )
        conn.close()

    print(f"xbrl_extract_target_total={total_target}")
    print(f"xbrl_extracted_total={total_extracted}")
    print(f"xbrl_extract_error_total={total_errors}")

    return {
        "loop_count": loop_count,
        "target_total": total_target,
        "extracted_total": total_extracted,
        "error_total": total_errors,
        "extract_chunk_count": extract_chunk_count_total,
        "worker_extract_elapsed_seconds_total": worker_extract_elapsed_seconds_total,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--run-all", action="store_true")
    parser.add_argument("--form-codes", default="", help="Comma-separated form codes. Example: 043A00")
    parser.add_argument("--period-ranks", default="", help="Comma-separated: latest,5,10")
    parser.add_argument("--codes", default="all", help="Comma-separated security codes, or all.")
    parser.add_argument("--force", action="store_true", help="Re-extract even when xbrl_path already exists.")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--extract-chunk-size", type=int, default=5)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_extract_xbrl_from_zips(
        batch_size=args.batch_size,
        run_all=args.run_all,
        form_codes=normalize_form_codes(args.form_codes or None),
        period_ranks=args.period_ranks or None,
        codes=tuple(part.strip() for part in str(args.codes or "").split(",") if part.strip() and part.strip().lower() != "all"),
        force=args.force,
        workers=args.workers,
        extract_chunk_size=args.extract_chunk_size,
    )


if __name__ == "__main__":
    main()

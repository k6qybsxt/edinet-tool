from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
from time import perf_counter, sleep
from typing import Any, Callable

from edinet_monitor.config.settings import (
    DOWNLOAD_CONNECT_TIMEOUT_SEC,
    DOWNLOAD_COOLDOWN_FAILURE_STREAK,
    DOWNLOAD_COOLDOWN_SEC,
    DOWNLOAD_MAX_RETRIES,
    DOWNLOAD_READ_TIMEOUT_SEC,
    DOWNLOAD_RETRY_WAIT_SEC,
)
from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.collector.document_download_service import download_document_zip
from edinet_monitor.services.collector.download_queue_service import (
    fetch_pending_filings,
    mark_download_error,
    mark_download_success,
)
from edinet_monitor.services.collector.download_wave_service import (
    iter_download_waves,
    run_download_wave,
    validate_download_workers,
)
from edinet_monitor.services.collector.edinet_api_key_guard import validate_edinet_api_key
from edinet_monitor.services.collector.manifest_download_service import process_manifest_download_row
from edinet_monitor.services.performance_log_service import PerformanceLog
from edinet_monitor.services.storage.path_service import build_zip_save_path


def _process_daily_download_job(job: dict[str, Any]) -> dict[str, Any]:
    row = dict(job["row"])
    result = process_manifest_download_row(
        row,
        api_key=str(job["api_key"]),
        downloader=job["downloader"],
        connect_timeout_sec=int(job["connect_timeout_sec"]),
        read_timeout_sec=int(job["read_timeout_sec"]),
        max_retries=int(job["max_retries"]),
        retry_wait_sec=float(job["retry_wait_sec"]),
        sleep_func=job["sleep_func"],
        timer_func=job["timer_func"],
    )
    return {
        "doc_id": str(row.get("doc_id") or ""),
        "row": row,
        "result": result,
    }


def run_download_filing_zips(
    *,
    api_key: str,
    batch_size: int = 20,
    run_all: bool = False,
    workers: int = 1,
    retry_errors: bool = False,
    connect_timeout_sec: int = DOWNLOAD_CONNECT_TIMEOUT_SEC,
    read_timeout_sec: int = DOWNLOAD_READ_TIMEOUT_SEC,
    max_retries: int = DOWNLOAD_MAX_RETRIES,
    retry_wait_sec: float = DOWNLOAD_RETRY_WAIT_SEC,
    cooldown_failure_streak: int = DOWNLOAD_COOLDOWN_FAILURE_STREAK,
    cooldown_sec: float = DOWNLOAD_COOLDOWN_SEC,
    downloader: Callable[..., Path] = download_document_zip,
    sleep_func: Callable[[float], None] = sleep,
    timer_func: Callable[[], float] = perf_counter,
    wall_timer_func: Callable[[], float] = perf_counter,
) -> dict[str, Any]:
    conn = get_connection()
    target_workers = validate_download_workers(workers)
    perf_log = PerformanceLog(
        command_name="download_filing_zips",
        workers=target_workers,
        batch_size=batch_size,
        parameters={
            "batch_size": int(batch_size),
            "run_all": bool(run_all),
            "workers": target_workers,
            "retry_errors": bool(retry_errors),
            "connect_timeout_sec": int(connect_timeout_sec),
            "read_timeout_sec": int(read_timeout_sec),
            "max_retries": int(max_retries),
            "retry_wait_sec": float(retry_wait_sec),
            "cooldown_failure_streak": int(cooldown_failure_streak),
            "cooldown_sec": float(cooldown_sec),
        },
        timer_func=timer_func,
    )
    executor = ThreadPoolExecutor(max_workers=target_workers) if target_workers > 1 else None
    attempted_doc_ids: set[str] = set()
    total_target = 0
    total_downloaded = 0
    total_existing = 0
    total_errors = 0
    total_download_elapsed_seconds = 0.0
    total_retry_wait_elapsed_seconds = 0.0
    total_cooldown_elapsed_seconds = 0.0
    total_download_wall_elapsed_seconds = 0.0
    loop_count = 0
    wave_count = 0
    cooldown_count = 0
    consecutive_cooldown_errors = 0
    error_type_totals: dict[str, int] = {}
    unhandled_error: Exception | None = None

    try:
        while True:
            with perf_log.measure("db_read", "fetch_pending_filings"):
                rows = fetch_pending_filings(
                    conn,
                    limit=batch_size,
                    retry_errors=retry_errors,
                    exclude_doc_ids=attempted_doc_ids,
                )
            print(f"[DEBUG] pending_rows={len(rows)}")

            if not rows:
                break

            loop_count += 1
            total_target += len(rows)
            attempted_doc_ids.update(str(row["doc_id"]) for row in rows)

            for wave_rows in iter_download_waves(rows, workers=target_workers):
                jobs: list[dict[str, Any]] = []
                for row in wave_rows:
                    doc_id = str(row["doc_id"])
                    submit_date = str(row["submit_date"] or "")
                    output_path = build_zip_save_path(submit_date, doc_id)
                    print(f"[DEBUG] target_doc_id={doc_id} submit_date={submit_date}")
                    print(f"[DEBUG] output_path={output_path}")
                    jobs.append(
                        {
                            "row": {
                                "doc_id": doc_id,
                                "submit_date": submit_date,
                                "zip_path": str(output_path),
                                "download_status": str(row["download_status"] or "pending"),
                            },
                            "api_key": api_key,
                            "downloader": downloader,
                            "connect_timeout_sec": connect_timeout_sec,
                            "read_timeout_sec": read_timeout_sec,
                            "max_retries": max_retries,
                            "retry_wait_sec": retry_wait_sec,
                            "sleep_func": sleep_func,
                            "timer_func": timer_func,
                        }
                    )

                wave_started = wall_timer_func()
                with perf_log.measure("file_io", "download_zip_wave", count_total=len(jobs)):
                    wave_results = run_download_wave(
                        jobs,
                        workers=target_workers,
                        job_func=_process_daily_download_job,
                        executor=executor,
                    )
                total_download_wall_elapsed_seconds += max(wall_timer_func() - wave_started, 0.0)
                wave_count += 1
                should_cooldown = False

                for wave_result in wave_results:
                    doc_id = str(wave_result["doc_id"])
                    result = dict(wave_result["result"])
                    total_download_elapsed_seconds += float(result.get("download_elapsed_seconds", 0.0) or 0.0)
                    total_retry_wait_elapsed_seconds += float(result.get("retry_wait_elapsed_seconds", 0.0) or 0.0)

                    if result["result"] in {"downloaded", "existing"}:
                        with perf_log.measure("db_write", "mark_download_success"):
                            mark_download_success(conn, doc_id, str(result["path"]))
                        if result["result"] == "existing":
                            total_existing += 1
                            print(f"existing_zip doc_id={doc_id} path={result['path']}")
                        else:
                            total_downloaded += 1
                            print(f"downloaded doc_id={doc_id} path={result['path']}")
                        consecutive_cooldown_errors = 0
                        continue

                    with perf_log.measure("db_write", "mark_download_error"):
                        mark_download_error(conn, doc_id)
                    total_errors += 1
                    error_type = str(result.get("error_type") or "unknown_error")
                    error_type_totals[error_type] = error_type_totals.get(error_type, 0) + 1
                    if result.get("cooldown_eligible"):
                        consecutive_cooldown_errors += 1
                    else:
                        consecutive_cooldown_errors = 0
                    if (
                        cooldown_failure_streak > 0
                        and cooldown_sec > 0
                        and consecutive_cooldown_errors >= cooldown_failure_streak
                    ):
                        should_cooldown = True
                    print(
                        "download_error "
                        f"doc_id={doc_id} "
                        f"error_type={result.get('error_type')} "
                        f"retryable={result.get('retryable')} "
                        f"status_code={result.get('status_code')}"
                    )

                if should_cooldown:
                    cooldown_count += 1
                    print(
                        f"cooldown_start consecutive_failures={consecutive_cooldown_errors} "
                        f"cooldown_sec={cooldown_sec}"
                    )
                    cooldown_started = timer_func()
                    sleep_func(cooldown_sec)
                    total_cooldown_elapsed_seconds += max(timer_func() - cooldown_started, 0.0)
                    print("cooldown_end=1")
                    consecutive_cooldown_errors = 0

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
            success_total=total_downloaded + total_existing,
            error_total=total_errors,
            error_summary={"unhandled_error": repr(unhandled_error)} if unhandled_error else {},
            summary={
                "loop_count": loop_count,
                "workers": target_workers,
                "wave_count": wave_count,
                "existing_total": total_existing,
                "download_elapsed_seconds": round(total_download_elapsed_seconds, 3),
                "retry_wait_elapsed_seconds": round(total_retry_wait_elapsed_seconds, 3),
                "cooldown_count": cooldown_count,
                "cooldown_elapsed_seconds": round(total_cooldown_elapsed_seconds, 3),
                "download_wall_elapsed_seconds": round(total_download_wall_elapsed_seconds, 3),
                "error_type_totals": dict(sorted(error_type_totals.items())),
            },
        )
        conn.close()

    print(f"download_target_total={total_target}")
    print(f"downloaded_total={total_downloaded}")
    print(f"existing_total={total_existing}")
    print(f"download_error_total={total_errors}")
    print(f"workers={target_workers}")
    print(f"wave_count={wave_count}")
    print(f"download_wall_elapsed_seconds={round(total_download_wall_elapsed_seconds, 3)}")

    return {
        "loop_count": loop_count,
        "target_total": total_target,
        "downloaded_total": total_downloaded,
        "existing_total": total_existing,
        "error_total": total_errors,
        "workers": target_workers,
        "wave_count": wave_count,
        "download_elapsed_seconds": round(total_download_elapsed_seconds, 3),
        "retry_wait_elapsed_seconds": round(total_retry_wait_elapsed_seconds, 3),
        "cooldown_count": cooldown_count,
        "cooldown_elapsed_seconds": round(total_cooldown_elapsed_seconds, 3),
        "download_wall_elapsed_seconds": round(total_download_wall_elapsed_seconds, 3),
        "error_type_totals": dict(sorted(error_type_totals.items())),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--run-all", action="store_true")
    parser.add_argument("--workers", type=int, choices=[1, 2], default=1)
    parser.add_argument("--retry-errors", action="store_true")
    return parser


def main() -> None:
    api_key = validate_edinet_api_key(os.getenv("EDINET_API_KEY"))
    args = build_arg_parser().parse_args()
    run_download_filing_zips(
        api_key=api_key,
        batch_size=args.batch_size,
        run_all=args.run_all,
        workers=args.workers,
        retry_errors=args.retry_errors,
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import os
from datetime import datetime
from time import perf_counter
from typing import Any, Callable
from uuid import uuid4

from edinet_monitor.cli.collect_document_list_to_db import collect_document_list_for_dates
from edinet_monitor.cli.download_filing_zips import run_download_filing_zips
from edinet_monitor.cli.extract_xbrl_from_zips import run_extract_xbrl_from_zips
from edinet_monitor.cli.run_screening import run_screening
from edinet_monitor.cli.save_derived_metrics import run_save_derived_metrics
from edinet_monitor.cli.save_normalized_metrics import run_save_normalized_metrics
from edinet_monitor.cli.save_raw_facts import run_save_raw_facts
from edinet_monitor.config.settings import XBRL_RETENTION_ENABLED, XBRL_RETENTION_MONTHS
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.collector.target_date_service import resolve_target_dates
from edinet_monitor.services.storage.pipeline_run_store_service import (
    upsert_pipeline_run,
    upsert_pipeline_run_chunk,
)
from edinet_monitor.services.storage.xbrl_retention_service import cleanup_old_xbrl_storage
from edinet_monitor.screening.screening_rule_service import DEFAULT_RULE_NAME, list_rule_names


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target-date",
        default=os.getenv("EDINET_TARGET_DATE", "").strip(),
        help="Single target date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--date-from",
        default=os.getenv("EDINET_DATE_FROM", "").strip(),
        help="Start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--date-to",
        default=os.getenv("EDINET_DATE_TO", "").strip(),
        help="End date in YYYY-MM-DD format.",
    )
    parser.add_argument("--download-batch-size", type=int, default=20)
    parser.add_argument("--extract-batch-size", type=int, default=20)
    parser.add_argument("--raw-batch-size", type=int, default=20)
    parser.add_argument("--normalized-batch-size", type=int, default=100)
    parser.add_argument("--derived-batch-size", type=int, default=100)
    parser.add_argument("--screening-date", default="")
    parser.add_argument(
        "--screening-rule-name",
        default=DEFAULT_RULE_NAME,
        choices=list_rule_names(),
    )
    return parser


def _format_timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _execute_stage(
    *,
    run_id: str,
    stage_key: str,
    stage_func: Callable[..., dict[str, Any]],
    chunk_rows: list[dict[str, Any]],
    stage_summary_by_key: dict[str, dict[str, Any]],
    timer_func: Callable[[], float],
    timestamp_now_func: Callable[[], datetime],
    stage_kwargs: dict[str, Any] | None = None,
    manifest_rows: int = 0,
    downloaded_total: int = 0,
    existing_total: int = 0,
    error_total: int = 0,
    cooldown_count: int = 0,
    effective_download_profile: str = "",
) -> dict[str, Any]:
    kwargs = dict(stage_kwargs or {})
    started_at = timestamp_now_func()
    started_at_text = _format_timestamp(started_at)
    timer_started = timer_func()
    stage_status = "completed"
    stage_error = ""
    summary: dict[str, Any] = {}

    try:
        summary = dict(stage_func(**kwargs) or {})
        return summary
    except Exception as exc:
        stage_status = "failed"
        stage_error = repr(exc)
        raise
    finally:
        finished_at = timestamp_now_func()
        finished_at_text = _format_timestamp(finished_at)
        elapsed_seconds = round(max(timer_func() - timer_started, 0.0), 3)

        chunk_rows.append(
            {
                "run_id": run_id,
                "run_type": "daily_pipeline",
                "chunk_key": stage_key,
                "chunk_granularity": "stage",
                "chunk_date_from": "",
                "chunk_date_to": "",
                "manifest_name": stage_key,
                "manifest_path": "",
                "started_at": started_at_text,
                "finished_at": finished_at_text,
                "elapsed_seconds": elapsed_seconds,
                "chunk_status": stage_status,
                "chunk_error": stage_error,
                "manifest_rows": int(manifest_rows or 0),
                "effective_download_profile": effective_download_profile,
                "downloaded_total": int(downloaded_total or 0),
                "existing_total": int(existing_total or 0),
                "error_total": int(error_total or 0),
                "cooldown_count": int(cooldown_count or 0),
                "download_elapsed_seconds": 0.0,
                "retry_wait_elapsed_seconds": 0.0,
                "cooldown_elapsed_seconds": 0.0,
                "error_type_totals": {},
                "collect_summary": {},
                "manifest_summary": {},
                "download_summary": {},
                "summary": dict(summary),
            }
        )
        stage_summary_by_key[stage_key] = {
            "started_at": started_at_text,
            "finished_at": finished_at_text,
            "elapsed_seconds": elapsed_seconds,
            "status": stage_status,
            "error": stage_error,
            "summary": dict(summary),
        }


def run_daily_pipeline(
    *,
    target_date_text: str = "",
    date_from_text: str = "",
    date_to_text: str = "",
    download_batch_size: int = 20,
    extract_batch_size: int = 20,
    raw_batch_size: int = 20,
    normalized_batch_size: int = 100,
    derived_batch_size: int = 100,
    screening_date: str | None = None,
    screening_rule_name: str | None = None,
    api_key: str | None = None,
    resolve_target_dates_func: Callable[..., list] = resolve_target_dates,
    collect_func: Callable[..., dict[str, Any]] = collect_document_list_for_dates,
    download_func: Callable[..., dict[str, Any]] = run_download_filing_zips,
    extract_func: Callable[..., dict[str, Any]] = run_extract_xbrl_from_zips,
    raw_func: Callable[..., dict[str, Any]] = run_save_raw_facts,
    normalized_func: Callable[..., dict[str, Any]] = run_save_normalized_metrics,
    derived_func: Callable[..., dict[str, Any]] = run_save_derived_metrics,
    screening_func: Callable[..., dict[str, Any]] = run_screening,
    xbrl_retention_func: Callable[..., dict[str, Any]] = cleanup_old_xbrl_storage,
    create_tables_func: Callable[[], None] = create_tables,
    connection_factory: Callable[[], Any] = get_connection,
    timestamp_now_func: Callable[[], datetime] = datetime.now,
    timer_func: Callable[[], float] = perf_counter,
) -> dict[str, Any]:
    effective_api_key = str(api_key or os.getenv("EDINET_API_KEY") or "").strip()
    if not effective_api_key:
        raise RuntimeError("Set EDINET_API_KEY before running.")

    create_tables_func()

    started_at = timestamp_now_func()
    started_at_text = _format_timestamp(started_at)
    timer_started = timer_func()
    run_id = f"daily_{started_at.strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    run_status = "completed"
    run_error = ""

    chunk_rows: list[dict[str, Any]] = []
    stage_summary_by_key: dict[str, dict[str, Any]] = {}

    target_dates = resolve_target_dates_func(
        target_date_text=target_date_text,
        date_from_text=date_from_text,
        date_to_text=date_to_text,
    )
    resolved_target_dates = [target.isoformat() for target in target_dates]
    effective_target_date = resolved_target_dates[0] if len(resolved_target_dates) == 1 else ""
    effective_date_from = resolved_target_dates[0] if resolved_target_dates else ""
    effective_date_to = resolved_target_dates[-1] if resolved_target_dates else ""

    collect_summary: dict[str, Any] = {}
    download_summary: dict[str, Any] = {}
    extract_summary: dict[str, Any] = {}
    raw_summary: dict[str, Any] = {}
    normalized_summary: dict[str, Any] = {}
    derived_summary: dict[str, Any] = {}
    screening_summary: dict[str, Any] = {}
    xbrl_retention_summary: dict[str, Any] = {}

    try:
        collect_summary = _execute_stage(
            run_id=run_id,
            stage_key="collect",
            stage_func=collect_func,
            chunk_rows=chunk_rows,
            stage_summary_by_key=stage_summary_by_key,
            timer_func=timer_func,
            timestamp_now_func=timestamp_now_func,
            stage_kwargs={
                "target_dates": target_dates,
                "api_key": effective_api_key,
            },
        )
        if chunk_rows:
            chunk_rows[-1]["manifest_rows"] = int(
                collect_summary.get("totals", {}).get("filing_saved_count", 0) or 0
            )
            chunk_rows[-1]["summary"] = dict(collect_summary)

        download_summary = _execute_stage(
            run_id=run_id,
            stage_key="download",
            stage_func=download_func,
            chunk_rows=chunk_rows,
            stage_summary_by_key=stage_summary_by_key,
            timer_func=timer_func,
            timestamp_now_func=timestamp_now_func,
            stage_kwargs={
                "api_key": effective_api_key,
                "batch_size": download_batch_size,
                "run_all": True,
            },
        )
        if chunk_rows:
            chunk_rows[-1]["manifest_rows"] = int(download_summary.get("target_total", 0) or 0)
            chunk_rows[-1]["downloaded_total"] = int(download_summary.get("downloaded_total", 0) or 0)
            chunk_rows[-1]["error_total"] = int(download_summary.get("error_total", 0) or 0)
            chunk_rows[-1]["summary"] = dict(download_summary)

        extract_summary = _execute_stage(
            run_id=run_id,
            stage_key="extract_xbrl",
            stage_func=extract_func,
            chunk_rows=chunk_rows,
            stage_summary_by_key=stage_summary_by_key,
            timer_func=timer_func,
            timestamp_now_func=timestamp_now_func,
            stage_kwargs={
                "batch_size": extract_batch_size,
                "run_all": True,
            },
        )
        if chunk_rows:
            chunk_rows[-1]["manifest_rows"] = int(extract_summary.get("target_total", 0) or 0)
            chunk_rows[-1]["downloaded_total"] = int(extract_summary.get("extracted_total", 0) or 0)
            chunk_rows[-1]["error_total"] = int(extract_summary.get("error_total", 0) or 0)
            chunk_rows[-1]["summary"] = dict(extract_summary)

        raw_summary = _execute_stage(
            run_id=run_id,
            stage_key="save_raw_facts",
            stage_func=raw_func,
            chunk_rows=chunk_rows,
            stage_summary_by_key=stage_summary_by_key,
            timer_func=timer_func,
            timestamp_now_func=timestamp_now_func,
            stage_kwargs={
                "batch_size": raw_batch_size,
                "run_all": True,
            },
        )
        if chunk_rows:
            chunk_rows[-1]["manifest_rows"] = int(raw_summary.get("target_total", 0) or 0)
            chunk_rows[-1]["downloaded_total"] = int(raw_summary.get("saved_docs_total", 0) or 0)
            chunk_rows[-1]["error_total"] = int(raw_summary.get("error_total", 0) or 0)
            chunk_rows[-1]["summary"] = dict(raw_summary)

        normalized_summary = _execute_stage(
            run_id=run_id,
            stage_key="save_normalized_metrics",
            stage_func=normalized_func,
            chunk_rows=chunk_rows,
            stage_summary_by_key=stage_summary_by_key,
            timer_func=timer_func,
            timestamp_now_func=timestamp_now_func,
            stage_kwargs={
                "batch_size": normalized_batch_size,
            },
        )
        if chunk_rows:
            chunk_rows[-1]["manifest_rows"] = int(normalized_summary.get("target_total", 0) or 0)
            chunk_rows[-1]["downloaded_total"] = int(normalized_summary.get("saved_docs_total", 0) or 0)
            chunk_rows[-1]["error_total"] = int(normalized_summary.get("error_total", 0) or 0)
            chunk_rows[-1]["summary"] = dict(normalized_summary)

        derived_summary = _execute_stage(
            run_id=run_id,
            stage_key="save_derived_metrics",
            stage_func=derived_func,
            chunk_rows=chunk_rows,
            stage_summary_by_key=stage_summary_by_key,
            timer_func=timer_func,
            timestamp_now_func=timestamp_now_func,
            stage_kwargs={
                "batch_size": derived_batch_size,
                "run_all": True,
            },
        )
        if chunk_rows:
            chunk_rows[-1]["manifest_rows"] = int(derived_summary.get("target_total", 0) or 0)
            chunk_rows[-1]["downloaded_total"] = int(derived_summary.get("saved_docs_total", 0) or 0)
            chunk_rows[-1]["error_total"] = int(derived_summary.get("error_total", 0) or 0)
            chunk_rows[-1]["summary"] = dict(derived_summary)

        screening_summary = _execute_stage(
            run_id=run_id,
            stage_key="screening",
            stage_func=screening_func,
            chunk_rows=chunk_rows,
            stage_summary_by_key=stage_summary_by_key,
            timer_func=timer_func,
            timestamp_now_func=timestamp_now_func,
            stage_kwargs={
                "screening_date": screening_date or None,
                "rule_name": screening_rule_name or None,
            },
        )
        if chunk_rows:
            chunk_rows[-1]["manifest_rows"] = int(screening_summary.get("target_count", 0) or 0)
            chunk_rows[-1]["downloaded_total"] = int(screening_summary.get("hit_count", 0) or 0)
            chunk_rows[-1]["summary"] = dict(screening_summary)

        def _run_xbrl_retention_stage() -> dict[str, Any]:
            retention_conn = connection_factory()
            try:
                return xbrl_retention_func(
                    retention_conn,
                    enabled=XBRL_RETENTION_ENABLED,
                    keep_months=XBRL_RETENTION_MONTHS,
                )
            finally:
                retention_conn.close()

        xbrl_retention_summary = _execute_stage(
            run_id=run_id,
            stage_key="xbrl_retention",
            stage_func=_run_xbrl_retention_stage,
            chunk_rows=chunk_rows,
            stage_summary_by_key=stage_summary_by_key,
            timer_func=timer_func,
            timestamp_now_func=timestamp_now_func,
        )
        if chunk_rows:
            chunk_rows[-1]["manifest_rows"] = int(xbrl_retention_summary.get("target_total", 0) or 0)
            chunk_rows[-1]["downloaded_total"] = int(xbrl_retention_summary.get("deleted_total", 0) or 0)
            chunk_rows[-1]["error_total"] = int(xbrl_retention_summary.get("error_total", 0) or 0)
            chunk_rows[-1]["summary"] = dict(xbrl_retention_summary)
    except Exception as exc:
        run_status = "failed"
        run_error = repr(exc)
        raise
    finally:
        finished_at = timestamp_now_func()
        finished_at_text = _format_timestamp(finished_at)
        elapsed_seconds = round(max(timer_func() - timer_started, 0.0), 3)
        run_summary = {
            "target_dates": resolved_target_dates,
            "stages": dict(stage_summary_by_key),
            "collect_summary": dict(collect_summary),
            "download_summary": dict(download_summary),
            "extract_summary": dict(extract_summary),
            "raw_summary": dict(raw_summary),
            "normalized_summary": dict(normalized_summary),
            "derived_summary": dict(derived_summary),
            "screening_summary": dict(screening_summary),
            "xbrl_retention_summary": dict(xbrl_retention_summary),
        }

        pipeline_conn = connection_factory()
        try:
            upsert_pipeline_run(
                pipeline_conn,
                run_id=run_id,
                run_type="daily_pipeline",
                started_at=started_at_text,
                finished_at=finished_at_text,
                elapsed_seconds=elapsed_seconds,
                run_status=run_status,
                run_error=run_error,
                target_date=effective_target_date,
                date_from=effective_date_from,
                date_to=effective_date_to,
                chunks=len(chunk_rows),
                manifest_rows_total=int(collect_summary.get("totals", {}).get("filing_saved_count", 0) or 0),
                downloaded_total=int(download_summary.get("downloaded_total", 0) or 0),
                existing_total=0,
                error_total=(
                    int(download_summary.get("error_total", 0) or 0)
                    + int(extract_summary.get("error_total", 0) or 0)
                    + int(raw_summary.get("error_total", 0) or 0)
                    + int(normalized_summary.get("error_total", 0) or 0)
                    + int(derived_summary.get("error_total", 0) or 0)
                    + int(xbrl_retention_summary.get("error_total", 0) or 0)
                ),
                cooldown_total=0,
                download_elapsed_seconds=float(stage_summary_by_key.get("download", {}).get("elapsed_seconds", 0.0) or 0.0),
                retry_wait_elapsed_seconds=0.0,
                cooldown_elapsed_seconds=0.0,
                effective_profile_totals={},
                error_type_totals={},
                raw_retention_summary=dict(xbrl_retention_summary),
                summary=run_summary,
            )
            for chunk_row in chunk_rows:
                upsert_pipeline_run_chunk(
                    pipeline_conn,
                    **chunk_row,
                )
            pipeline_conn.commit()
        finally:
            pipeline_conn.close()

    print(f"daily_pipeline_run_id={run_id}")
    print(f"daily_pipeline_started_at={started_at_text}")
    print(f"daily_pipeline_finished_at={finished_at_text}")
    print(f"daily_pipeline_elapsed_seconds={elapsed_seconds}")

    print("daily_pipeline_completed=1")
    print(f"daily_target_dates={','.join(collect_summary['target_dates'])}")
    print(f"daily_collect_saved_total={collect_summary['totals']['filing_saved_count']}")
    print(f"daily_downloaded_total={download_summary['downloaded_total']}")
    print(f"daily_xbrl_extracted_total={extract_summary['extracted_total']}")
    print(f"daily_raw_facts_saved_docs_total={raw_summary['saved_docs_total']}")
    print(f"daily_normalized_metrics_saved_docs_total={normalized_summary['saved_docs_total']}")
    print(f"daily_derived_metrics_saved_docs_total={derived_summary['saved_docs_total']}")
    print(f"daily_screening_rule_name={screening_summary['rule_name']}")
    print(f"daily_screening_target_count={screening_summary['target_count']}")
    print(f"daily_screening_hit_count={screening_summary['hit_count']}")
    print(f"daily_xbrl_retention_enabled={int(XBRL_RETENTION_ENABLED)}")
    print(f"daily_xbrl_retention_months={XBRL_RETENTION_MONTHS}")
    print(f"daily_xbrl_retention_status={xbrl_retention_summary['status']}")
    print(f"daily_xbrl_retention_reason={xbrl_retention_summary['reason']}")
    print(f"daily_xbrl_retention_reference_month={xbrl_retention_summary['reference_month']}")
    print(f"daily_xbrl_retention_keep_from_month={xbrl_retention_summary['keep_from_month']}")
    print(f"daily_xbrl_retention_target_total={xbrl_retention_summary['target_total']}")
    print(f"daily_xbrl_retention_deleted_total={xbrl_retention_summary['deleted_total']}")
    print(f"daily_xbrl_retention_missing_file_total={xbrl_retention_summary['missing_file_total']}")
    print(f"daily_xbrl_retention_error_total={xbrl_retention_summary['error_total']}")

    return {
        "run_id": run_id,
        "started_at": started_at_text,
        "finished_at": finished_at_text,
        "elapsed_seconds": elapsed_seconds,
        "run_status": run_status,
        "target_dates": resolved_target_dates,
        "collect_summary": dict(collect_summary),
        "download_summary": dict(download_summary),
        "extract_summary": dict(extract_summary),
        "raw_summary": dict(raw_summary),
        "normalized_summary": dict(normalized_summary),
        "derived_summary": dict(derived_summary),
        "screening_summary": dict(screening_summary),
        "xbrl_retention_summary": dict(xbrl_retention_summary),
        "stage_summary_by_key": dict(stage_summary_by_key),
    }


def main() -> None:
    args = build_arg_parser().parse_args()
    run_daily_pipeline(
        target_date_text=args.target_date,
        date_from_text=args.date_from,
        date_to_text=args.date_to,
        download_batch_size=args.download_batch_size,
        extract_batch_size=args.extract_batch_size,
        raw_batch_size=args.raw_batch_size,
        normalized_batch_size=args.normalized_batch_size,
        derived_batch_size=args.derived_batch_size,
        screening_date=args.screening_date or None,
        screening_rule_name=args.screening_rule_name or None,
    )


if __name__ == "__main__":
    main()

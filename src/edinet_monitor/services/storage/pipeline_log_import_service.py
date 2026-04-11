from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from edinet_monitor.config.settings import (
    ZIP_BACKFILL_CHUNK_LOG_PATH,
    ZIP_BACKFILL_RUN_LOG_PATH,
)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        rows.append(json.loads(text))
    return rows


def import_zip_backfill_run_logs(
    conn: sqlite3.Connection,
    *,
    run_log_path: Path = ZIP_BACKFILL_RUN_LOG_PATH,
    chunk_log_path: Path = ZIP_BACKFILL_CHUNK_LOG_PATH,
) -> dict[str, Any]:
    run_rows = _read_jsonl(run_log_path)
    chunk_rows = _read_jsonl(chunk_log_path)

    inserted_runs = 0
    inserted_chunks = 0

    for row in run_rows:
        conn.execute(
            """
            INSERT INTO pipeline_runs (
                run_id,
                run_type,
                started_at,
                finished_at,
                elapsed_seconds,
                run_status,
                run_error,
                target_date,
                date_from,
                date_to,
                manifest_prefix,
                manifest_granularity,
                requested_download_profile,
                download_auto_peak_threshold,
                prepare_only,
                overwrite_manifests,
                chunks,
                manifest_rows_total,
                downloaded_total,
                existing_total,
                error_total,
                cooldown_total,
                download_elapsed_seconds,
                retry_wait_elapsed_seconds,
                cooldown_elapsed_seconds,
                effective_profile_totals_json,
                error_type_totals_json,
                raw_retention_summary_json,
                created_at,
                updated_at
            )
            VALUES (
                :run_id,
                'zip_backfill',
                :started_at,
                :finished_at,
                :elapsed_seconds,
                :run_status,
                :run_error,
                :target_date,
                :date_from,
                :date_to,
                :manifest_prefix,
                :manifest_granularity,
                :requested_download_profile,
                :download_auto_peak_threshold,
                :prepare_only,
                :overwrite_manifests,
                :chunks,
                :manifest_rows_total,
                :downloaded_total,
                :existing_total,
                :error_total,
                :cooldown_total,
                :download_elapsed_seconds,
                :retry_wait_elapsed_seconds,
                :cooldown_elapsed_seconds,
                :effective_profile_totals_json,
                :error_type_totals_json,
                :raw_retention_summary_json,
                :created_at,
                :updated_at
            )
            ON CONFLICT(run_id) DO UPDATE SET
                started_at = excluded.started_at,
                finished_at = excluded.finished_at,
                elapsed_seconds = excluded.elapsed_seconds,
                run_status = excluded.run_status,
                run_error = excluded.run_error,
                target_date = excluded.target_date,
                date_from = excluded.date_from,
                date_to = excluded.date_to,
                manifest_prefix = excluded.manifest_prefix,
                manifest_granularity = excluded.manifest_granularity,
                requested_download_profile = excluded.requested_download_profile,
                download_auto_peak_threshold = excluded.download_auto_peak_threshold,
                prepare_only = excluded.prepare_only,
                overwrite_manifests = excluded.overwrite_manifests,
                chunks = excluded.chunks,
                manifest_rows_total = excluded.manifest_rows_total,
                downloaded_total = excluded.downloaded_total,
                existing_total = excluded.existing_total,
                error_total = excluded.error_total,
                cooldown_total = excluded.cooldown_total,
                download_elapsed_seconds = excluded.download_elapsed_seconds,
                retry_wait_elapsed_seconds = excluded.retry_wait_elapsed_seconds,
                cooldown_elapsed_seconds = excluded.cooldown_elapsed_seconds,
                effective_profile_totals_json = excluded.effective_profile_totals_json,
                error_type_totals_json = excluded.error_type_totals_json,
                raw_retention_summary_json = excluded.raw_retention_summary_json,
                updated_at = excluded.updated_at
            """,
            {
                "run_id": str(row.get("run_id") or ""),
                "started_at": str(row.get("started_at") or ""),
                "finished_at": str(row.get("finished_at") or ""),
                "elapsed_seconds": float(row.get("elapsed_seconds", 0.0) or 0.0),
                "run_status": str(row.get("run_status") or ""),
                "run_error": str(row.get("run_error") or ""),
                "target_date": str(row.get("target_date") or ""),
                "date_from": str(row.get("date_from") or ""),
                "date_to": str(row.get("date_to") or ""),
                "manifest_prefix": str(row.get("manifest_prefix") or ""),
                "manifest_granularity": str(row.get("manifest_granularity") or ""),
                "requested_download_profile": str(row.get("requested_download_profile") or ""),
                "download_auto_peak_threshold": int(row.get("download_auto_peak_threshold", 0) or 0),
                "prepare_only": int(bool(row.get("prepare_only", False))),
                "overwrite_manifests": int(bool(row.get("overwrite_manifests", False))),
                "chunks": int(row.get("chunks", 0) or 0),
                "manifest_rows_total": int(row.get("manifest_rows_total", 0) or 0),
                "downloaded_total": int(row.get("downloaded_total", 0) or 0),
                "existing_total": int(row.get("existing_total", 0) or 0),
                "error_total": int(row.get("error_total", 0) or 0),
                "cooldown_total": int(row.get("cooldown_total", 0) or 0),
                "download_elapsed_seconds": float(row.get("download_elapsed_seconds", 0.0) or 0.0),
                "retry_wait_elapsed_seconds": float(row.get("retry_wait_elapsed_seconds", 0.0) or 0.0),
                "cooldown_elapsed_seconds": float(row.get("cooldown_elapsed_seconds", 0.0) or 0.0),
                "effective_profile_totals_json": json.dumps(row.get("effective_profile_totals", {}), ensure_ascii=False, sort_keys=True),
                "error_type_totals_json": json.dumps(row.get("error_type_totals", {}), ensure_ascii=False, sort_keys=True),
                "raw_retention_summary_json": json.dumps(
                    {
                        key: value
                        for key, value in row.items()
                        if str(key).startswith("raw_retention_")
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "created_at": str(row.get("started_at") or ""),
                "updated_at": str(row.get("finished_at") or row.get("started_at") or ""),
            },
        )
        inserted_runs += 1

    for row in chunk_rows:
        conn.execute(
            """
            INSERT INTO pipeline_run_chunks (
                run_id,
                run_type,
                chunk_key,
                chunk_granularity,
                chunk_date_from,
                chunk_date_to,
                manifest_name,
                manifest_path,
                started_at,
                finished_at,
                elapsed_seconds,
                chunk_status,
                chunk_error,
                manifest_rows,
                effective_download_profile,
                downloaded_total,
                existing_total,
                error_total,
                cooldown_count,
                download_elapsed_seconds,
                retry_wait_elapsed_seconds,
                cooldown_elapsed_seconds,
                error_type_totals_json,
                collect_summary_json,
                manifest_summary_json,
                download_summary_json,
                created_at,
                updated_at
            )
            VALUES (
                :run_id,
                'zip_backfill',
                :chunk_key,
                :chunk_granularity,
                :chunk_date_from,
                :chunk_date_to,
                :manifest_name,
                :manifest_path,
                :started_at,
                :finished_at,
                :elapsed_seconds,
                :chunk_status,
                :chunk_error,
                :manifest_rows,
                :effective_download_profile,
                :downloaded_total,
                :existing_total,
                :error_total,
                :cooldown_count,
                :download_elapsed_seconds,
                :retry_wait_elapsed_seconds,
                :cooldown_elapsed_seconds,
                :error_type_totals_json,
                :collect_summary_json,
                :manifest_summary_json,
                :download_summary_json,
                :created_at,
                :updated_at
            )
            ON CONFLICT(run_id, chunk_key, manifest_name) DO UPDATE SET
                chunk_granularity = excluded.chunk_granularity,
                chunk_date_from = excluded.chunk_date_from,
                chunk_date_to = excluded.chunk_date_to,
                manifest_path = excluded.manifest_path,
                started_at = excluded.started_at,
                finished_at = excluded.finished_at,
                elapsed_seconds = excluded.elapsed_seconds,
                chunk_status = excluded.chunk_status,
                chunk_error = excluded.chunk_error,
                manifest_rows = excluded.manifest_rows,
                effective_download_profile = excluded.effective_download_profile,
                downloaded_total = excluded.downloaded_total,
                existing_total = excluded.existing_total,
                error_total = excluded.error_total,
                cooldown_count = excluded.cooldown_count,
                download_elapsed_seconds = excluded.download_elapsed_seconds,
                retry_wait_elapsed_seconds = excluded.retry_wait_elapsed_seconds,
                cooldown_elapsed_seconds = excluded.cooldown_elapsed_seconds,
                error_type_totals_json = excluded.error_type_totals_json,
                collect_summary_json = excluded.collect_summary_json,
                manifest_summary_json = excluded.manifest_summary_json,
                download_summary_json = excluded.download_summary_json,
                updated_at = excluded.updated_at
            """,
            {
                "run_id": str(row.get("run_id") or ""),
                "chunk_key": str(row.get("chunk_key") or ""),
                "chunk_granularity": str(row.get("chunk_granularity") or ""),
                "chunk_date_from": str(row.get("chunk_date_from") or ""),
                "chunk_date_to": str(row.get("chunk_date_to") or ""),
                "manifest_name": str(row.get("manifest_name") or ""),
                "manifest_path": str(row.get("manifest_path") or ""),
                "started_at": str(row.get("started_at") or ""),
                "finished_at": str(row.get("finished_at") or ""),
                "elapsed_seconds": float(row.get("elapsed_seconds", 0.0) or 0.0),
                "chunk_status": str(row.get("chunk_status") or ""),
                "chunk_error": str(row.get("chunk_error") or ""),
                "manifest_rows": int(row.get("manifest_rows", 0) or 0),
                "effective_download_profile": str(row.get("effective_download_profile") or ""),
                "downloaded_total": int(row.get("downloaded_total", 0) or 0),
                "existing_total": int(row.get("existing_total", 0) or 0),
                "error_total": int(row.get("error_total", 0) or 0),
                "cooldown_count": int(row.get("cooldown_count", 0) or 0),
                "download_elapsed_seconds": float(row.get("download_elapsed_seconds", 0.0) or 0.0),
                "retry_wait_elapsed_seconds": float(row.get("retry_wait_elapsed_seconds", 0.0) or 0.0),
                "cooldown_elapsed_seconds": float(row.get("cooldown_elapsed_seconds", 0.0) or 0.0),
                "error_type_totals_json": json.dumps(row.get("error_type_totals", {}), ensure_ascii=False, sort_keys=True),
                "collect_summary_json": json.dumps(row.get("collect_summary", {}), ensure_ascii=False, sort_keys=True),
                "manifest_summary_json": json.dumps(row.get("manifest_summary", {}), ensure_ascii=False, sort_keys=True),
                "download_summary_json": json.dumps(row.get("download_summary", {}), ensure_ascii=False, sort_keys=True),
                "created_at": str(row.get("started_at") or ""),
                "updated_at": str(row.get("finished_at") or row.get("started_at") or ""),
            },
        )
        inserted_chunks += 1

    conn.commit()

    return {
        "run_log_path": str(run_log_path),
        "chunk_log_path": str(chunk_log_path),
        "run_rows": len(run_rows),
        "chunk_rows": len(chunk_rows),
        "inserted_runs": inserted_runs,
        "inserted_chunks": inserted_chunks,
    }

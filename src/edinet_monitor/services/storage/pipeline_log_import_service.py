from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from edinet_monitor.config.settings import (
    ZIP_BACKFILL_CHUNK_LOG_PATH,
    ZIP_BACKFILL_RUN_LOG_PATH,
)
from edinet_monitor.services.storage.pipeline_run_store_service import (
    upsert_pipeline_run,
    upsert_pipeline_run_chunk,
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
        upsert_pipeline_run(
            conn,
            run_id=str(row.get("run_id") or ""),
            run_type="zip_backfill",
            started_at=str(row.get("started_at") or ""),
            finished_at=str(row.get("finished_at") or ""),
            elapsed_seconds=float(row.get("elapsed_seconds", 0.0) or 0.0),
            run_status=str(row.get("run_status") or ""),
            run_error=str(row.get("run_error") or ""),
            target_date=str(row.get("target_date") or ""),
            date_from=str(row.get("date_from") or ""),
            date_to=str(row.get("date_to") or ""),
            manifest_prefix=str(row.get("manifest_prefix") or ""),
            manifest_granularity=str(row.get("manifest_granularity") or ""),
            requested_download_profile=str(row.get("requested_download_profile") or ""),
            download_auto_peak_threshold=int(row.get("download_auto_peak_threshold", 0) or 0),
            prepare_only=bool(row.get("prepare_only", False)),
            overwrite_manifests=bool(row.get("overwrite_manifests", False)),
            chunks=int(row.get("chunks", 0) or 0),
            manifest_rows_total=int(row.get("manifest_rows_total", 0) or 0),
            downloaded_total=int(row.get("downloaded_total", 0) or 0),
            existing_total=int(row.get("existing_total", 0) or 0),
            error_total=int(row.get("error_total", 0) or 0),
            cooldown_total=int(row.get("cooldown_total", 0) or 0),
            download_elapsed_seconds=float(row.get("download_elapsed_seconds", 0.0) or 0.0),
            retry_wait_elapsed_seconds=float(row.get("retry_wait_elapsed_seconds", 0.0) or 0.0),
            cooldown_elapsed_seconds=float(row.get("cooldown_elapsed_seconds", 0.0) or 0.0),
            effective_profile_totals=dict(row.get("effective_profile_totals", {}) or {}),
            error_type_totals=dict(row.get("error_type_totals", {}) or {}),
            raw_retention_summary={
                key: value
                for key, value in row.items()
                if str(key).startswith("raw_retention_")
            },
            summary=dict(row),
        )
        inserted_runs += 1

    for row in chunk_rows:
        upsert_pipeline_run_chunk(
            conn,
            run_id=str(row.get("run_id") or ""),
            run_type="zip_backfill",
            chunk_key=str(row.get("chunk_key") or ""),
            chunk_granularity=str(row.get("chunk_granularity") or ""),
            chunk_date_from=str(row.get("chunk_date_from") or ""),
            chunk_date_to=str(row.get("chunk_date_to") or ""),
            manifest_name=str(row.get("manifest_name") or ""),
            manifest_path=str(row.get("manifest_path") or ""),
            started_at=str(row.get("started_at") or ""),
            finished_at=str(row.get("finished_at") or ""),
            elapsed_seconds=float(row.get("elapsed_seconds", 0.0) or 0.0),
            chunk_status=str(row.get("chunk_status") or ""),
            chunk_error=str(row.get("chunk_error") or ""),
            manifest_rows=int(row.get("manifest_rows", 0) or 0),
            effective_download_profile=str(row.get("effective_download_profile") or ""),
            downloaded_total=int(row.get("downloaded_total", 0) or 0),
            existing_total=int(row.get("existing_total", 0) or 0),
            error_total=int(row.get("error_total", 0) or 0),
            cooldown_count=int(row.get("cooldown_count", 0) or 0),
            download_elapsed_seconds=float(row.get("download_elapsed_seconds", 0.0) or 0.0),
            retry_wait_elapsed_seconds=float(row.get("retry_wait_elapsed_seconds", 0.0) or 0.0),
            cooldown_elapsed_seconds=float(row.get("cooldown_elapsed_seconds", 0.0) or 0.0),
            error_type_totals=dict(row.get("error_type_totals", {}) or {}),
            collect_summary=dict(row.get("collect_summary", {}) or {}),
            manifest_summary=dict(row.get("manifest_summary", {}) or {}),
            download_summary=dict(row.get("download_summary", {}) or {}),
            summary=dict(row),
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

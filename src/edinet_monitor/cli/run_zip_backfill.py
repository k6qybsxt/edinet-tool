from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

from edinet_monitor.cli.collect_document_list_to_manifest import (
    collect_document_manifest_for_dates,
)
from edinet_monitor.cli.download_manifest_zips import run_download_manifest_zips
from edinet_monitor.config.settings import TSE_LISTING_MASTER_CSV_PATH, ensure_data_dirs
from edinet_monitor.services.collector.issuer_master_csv_service import load_allowed_edinet_codes
from edinet_monitor.services.storage.manifest_service import (
    build_manifest_path,
    read_manifest_rows,
    summarize_manifest_rows,
)


@dataclass(frozen=True)
class MonthChunk:
    month_key: str
    start_date: date
    end_date: date
    target_dates: list[date]


def iter_month_chunks(start_date: date, end_date: date) -> list[MonthChunk]:
    if start_date > end_date:
        raise ValueError("date_from must be earlier than or equal to date_to.")

    chunks: list[MonthChunk] = []
    cursor = start_date

    while cursor <= end_date:
        month_start = cursor.replace(day=1)
        if cursor.month == 12:
            next_month_start = cursor.replace(year=cursor.year + 1, month=1, day=1)
        else:
            next_month_start = cursor.replace(month=cursor.month + 1, day=1)

        month_end = next_month_start - timedelta(days=1)
        chunk_start = max(cursor, start_date)
        chunk_end = min(month_end, end_date)
        target_dates = [
            chunk_start + timedelta(days=offset)
            for offset in range((chunk_end - chunk_start).days + 1)
        ]
        chunks.append(
            MonthChunk(
                month_key=f"{chunk_start.year:04d}-{chunk_start.month:02d}",
                start_date=chunk_start,
                end_date=chunk_end,
                target_dates=target_dates,
            )
        )
        cursor = chunk_end + timedelta(days=1)

    return chunks


def build_month_manifest_name(prefix: str, month_key: str) -> str:
    cleaned_prefix = str(prefix or "document_manifest").strip() or "document_manifest"
    return f"{cleaned_prefix}_{month_key}"


def run_zip_backfill(
    *,
    api_key: str,
    start_date: date,
    end_date: date,
    manifest_prefix: str = "document_manifest",
    master_csv_path: Path = TSE_LISTING_MASTER_CSV_PATH,
    prepare_only: bool = False,
    overwrite_manifests: bool = False,
    month_limit: int = 0,
    download_batch_size: int = 20,
    download_run_all: bool = False,
    download_retry_errors: bool = False,
    download_max_docs: int = 0,
    collect_func: Callable[..., dict[str, Any]] = collect_document_manifest_for_dates,
    download_func: Callable[..., dict[str, Any]] = run_download_manifest_zips,
    allowed_codes_loader: Callable[[Path], set[str]] = load_allowed_edinet_codes,
    manifest_path_builder: Callable[[str], Path] = build_manifest_path,
    ensure_dirs_func: Callable[[], None] = ensure_data_dirs,
) -> dict[str, Any]:
    ensure_dirs_func()
    allowed_edinet_codes = allowed_codes_loader(master_csv_path)
    month_chunks = iter_month_chunks(start_date, end_date)

    if month_limit > 0:
        month_chunks = month_chunks[:month_limit]

    monthly_results: list[dict[str, Any]] = []
    total_manifest_rows = 0
    total_downloaded = 0
    total_existing = 0
    total_errors = 0

    for chunk in month_chunks:
        manifest_name = build_month_manifest_name(manifest_prefix, chunk.month_key)
        manifest_path = manifest_path_builder(manifest_name)
        manifest_exists = manifest_path.exists()

        print(f"month_key={chunk.month_key}")
        print(f"month_date_from={chunk.start_date.isoformat()}")
        print(f"month_date_to={chunk.end_date.isoformat()}")
        print(f"month_manifest_path={manifest_path}")
        print(f"month_manifest_exists={manifest_exists}")

        if not manifest_exists or overwrite_manifests:
            collect_summary = collect_func(
                chunk.target_dates,
                api_key=api_key,
                allowed_edinet_codes=allowed_edinet_codes,
                manifest_path=manifest_path,
                overwrite=overwrite_manifests,
            )
        else:
            existing_rows = read_manifest_rows(manifest_path)
            collect_summary = {
                "target_dates": [target_date.isoformat() for target_date in chunk.target_dates],
                "manifest_path": str(manifest_path),
                "daily_summaries": [],
                "totals": {
                    "dates": len(chunk.target_dates),
                    "all_results": 0,
                    "target_results": 0,
                    "issuer_target_results": 0,
                    "incoming_manifest_rows": 0,
                },
                "existing_manifest_rows": len(existing_rows),
                "saved_manifest_rows": len(existing_rows),
                "reused_existing_manifest": True,
            }
            print("manifest_reused_existing=1")

        manifest_summary = summarize_manifest_rows(read_manifest_rows(manifest_path))
        total_manifest_rows += int(manifest_summary["manifest_rows"])

        if prepare_only:
            download_summary = {
                "manifest_path": str(manifest_path),
                "manifest_rows": manifest_summary["manifest_rows"],
                "target_total": 0,
                "downloaded_total": 0,
                "existing_total": 0,
                "error_total": 0,
                "processed_total": 0,
                "skipped": True,
            }
            print("download_skipped=1")
        else:
            download_summary = download_func(
                api_key=api_key,
                manifest_path=manifest_path,
                batch_size=download_batch_size,
                run_all=download_run_all,
                retry_errors=download_retry_errors,
                max_docs=download_max_docs,
            )
            total_downloaded += int(download_summary["downloaded_total"])
            total_existing += int(download_summary["existing_total"])
            total_errors += int(download_summary["error_total"])

        monthly_results.append(
            {
                "month_key": chunk.month_key,
                "manifest_name": manifest_name,
                "manifest_path": str(manifest_path),
                "collect_summary": collect_summary,
                "manifest_summary": manifest_summary,
                "download_summary": download_summary,
            }
        )

    print(f"backfill_months={len(month_chunks)}")
    print(f"backfill_manifest_rows_total={total_manifest_rows}")
    print(f"backfill_downloaded_total={total_downloaded}")
    print(f"backfill_existing_total={total_existing}")
    print(f"backfill_error_total={total_errors}")

    return {
        "months": len(month_chunks),
        "manifest_rows_total": total_manifest_rows,
        "downloaded_total": total_downloaded,
        "existing_total": total_existing,
        "error_total": total_errors,
        "monthly_results": monthly_results,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date-from",
        default=os.getenv("EDINET_DATE_FROM", "").strip(),
        required=True,
        help="Start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--date-to",
        default=os.getenv("EDINET_DATE_TO", "").strip(),
        required=True,
        help="End date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--manifest-prefix",
        default=os.getenv("EDINET_MANIFEST_PREFIX", "document_manifest").strip(),
        help="Prefix for monthly manifest names.",
    )
    parser.add_argument(
        "--master-csv-path",
        default=os.getenv("EDINET_TSE_MASTER_CSV", "").strip(),
        help="Path to TSE issuer master CSV.",
    )
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--overwrite-manifests", action="store_true")
    parser.add_argument("--month-limit", type=int, default=0)
    parser.add_argument("--download-batch-size", type=int, default=20)
    parser.add_argument("--download-run-all", action="store_true")
    parser.add_argument("--download-retry-errors", action="store_true")
    parser.add_argument("--download-max-docs", type=int, default=0)
    return parser


def main() -> None:
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("Set EDINET_API_KEY before running.")

    args = build_arg_parser().parse_args()
    start_date = date.fromisoformat(args.date_from)
    end_date = date.fromisoformat(args.date_to)
    master_csv_path = Path(args.master_csv_path or TSE_LISTING_MASTER_CSV_PATH)

    run_zip_backfill(
        api_key=api_key,
        start_date=start_date,
        end_date=end_date,
        manifest_prefix=args.manifest_prefix,
        master_csv_path=master_csv_path,
        prepare_only=args.prepare_only,
        overwrite_manifests=args.overwrite_manifests,
        month_limit=args.month_limit,
        download_batch_size=args.download_batch_size,
        download_run_all=args.download_run_all,
        download_retry_errors=args.download_retry_errors,
        download_max_docs=args.download_max_docs,
    )


if __name__ == "__main__":
    main()

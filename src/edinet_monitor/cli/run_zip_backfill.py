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
from edinet_monitor.cli.download_manifest_zips import (
    DOWNLOAD_PROFILES,
    resolve_download_runtime_settings,
    resolve_submit_filters,
    run_download_manifest_zips,
)
from edinet_monitor.config.settings import (
    DOWNLOAD_PROFILE_DEFAULT,
    TSE_LISTING_MASTER_CSV_PATH,
    ensure_data_dirs,
)
from edinet_monitor.services.collector.issuer_master_csv_service import load_allowed_edinet_codes
from edinet_monitor.services.storage.manifest_service import (
    build_manifest_path,
    read_manifest_rows,
    summarize_manifest_rows,
)


AUTO_DOWNLOAD_PROFILE = "auto"
AUTO_DOWNLOAD_PROFILE_MANIFEST_GRANULARITY = "month"
AUTO_DOWNLOAD_PEAK_THRESHOLD = 100


@dataclass(frozen=True)
class BackfillChunk:
    chunk_key: str
    start_date: date
    end_date: date
    target_dates: list[date]
    granularity: str

    @property
    def month_key(self) -> str:
        return self.chunk_key


def iter_manifest_chunks(start_date: date, end_date: date, *, granularity: str = "month") -> list[BackfillChunk]:
    if start_date > end_date:
        raise ValueError("date_from must be earlier than or equal to date_to.")

    if granularity not in {"month", "day"}:
        raise ValueError("manifest_granularity must be 'month' or 'day'.")

    chunks: list[BackfillChunk] = []
    cursor = start_date

    while cursor <= end_date:
        if granularity == "day":
            chunks.append(
                BackfillChunk(
                    chunk_key=cursor.isoformat(),
                    start_date=cursor,
                    end_date=cursor,
                    target_dates=[cursor],
                    granularity=granularity,
                )
            )
            cursor += timedelta(days=1)
            continue

        month_start = cursor.replace(day=1)
        if cursor.month == 12:
            next_month_start = cursor.replace(year=cursor.year + 1, month=1, day=1)
        else:
            next_month_start = cursor.replace(month=cursor.month + 1, day=1)

        month_end = next_month_start - timedelta(days=1)
        chunk_start = max(month_start, start_date)
        chunk_end = min(month_end, end_date)
        target_dates = [
            chunk_start + timedelta(days=offset)
            for offset in range((chunk_end - chunk_start).days + 1)
        ]
        chunks.append(
            BackfillChunk(
                chunk_key=f"{chunk_start.year:04d}-{chunk_start.month:02d}",
                start_date=chunk_start,
                end_date=chunk_end,
                target_dates=target_dates,
                granularity=granularity,
            )
        )
        cursor = chunk_end + timedelta(days=1)

    return chunks


def iter_month_chunks(start_date: date, end_date: date) -> list[BackfillChunk]:
    return iter_manifest_chunks(start_date, end_date, granularity="month")


def build_chunk_manifest_name(prefix: str, chunk_key: str) -> str:
    cleaned_prefix = str(prefix or "document_manifest").strip() or "document_manifest"
    return f"{cleaned_prefix}_{chunk_key}"


def build_month_manifest_name(prefix: str, month_key: str) -> str:
    return build_chunk_manifest_name(prefix, month_key)


def resolve_manifest_granularity(*, manifest_granularity: str, download_profile: str) -> str:
    normalized = str(manifest_granularity or "").strip().lower()
    if normalized:
        if normalized not in {"month", "day"}:
            raise ValueError("manifest_granularity must be 'month' or 'day'.")
        return normalized
    if download_profile == AUTO_DOWNLOAD_PROFILE:
        return AUTO_DOWNLOAD_PROFILE_MANIFEST_GRANULARITY
    return DOWNLOAD_PROFILES[download_profile].manifest_granularity


def resolve_effective_download_profile(
    *,
    requested_profile: str,
    manifest_rows: int,
    auto_peak_threshold: int,
) -> str:
    normalized = str(requested_profile or DOWNLOAD_PROFILE_DEFAULT).strip().lower() or DOWNLOAD_PROFILE_DEFAULT
    if normalized != AUTO_DOWNLOAD_PROFILE:
        return normalized
    if manifest_rows >= auto_peak_threshold:
        return "peak"
    return "normal"


def run_zip_backfill(
    *,
    api_key: str,
    start_date: date,
    end_date: date,
    manifest_prefix: str = "document_manifest",
    manifest_granularity: str = "month",
    master_csv_path: Path = TSE_LISTING_MASTER_CSV_PATH,
    prepare_only: bool = False,
    overwrite_manifests: bool = False,
    month_limit: int = 0,
    download_profile: str = DOWNLOAD_PROFILE_DEFAULT,
    download_auto_peak_threshold: int = AUTO_DOWNLOAD_PEAK_THRESHOLD,
    download_batch_size: int | None = None,
    download_run_all: bool = False,
    download_retry_errors: bool = False,
    download_max_docs: int = 0,
    download_connect_timeout_sec: int | None = None,
    download_read_timeout_sec: int | None = None,
    download_max_retries: int | None = None,
    download_retry_wait_sec: float | None = None,
    download_progress_every: int | None = None,
    download_cooldown_failure_streak: int | None = None,
    download_cooldown_sec: float | None = None,
    download_submit_date_text: str = "",
    download_submit_date_from_text: str = "",
    download_submit_date_to_text: str = "",
    download_submit_time_from_text: str = "",
    download_submit_time_to_text: str = "",
    collect_func: Callable[..., dict[str, Any]] = collect_document_manifest_for_dates,
    download_func: Callable[..., dict[str, Any]] = run_download_manifest_zips,
    allowed_codes_loader: Callable[[Path], set[str]] = load_allowed_edinet_codes,
    manifest_path_builder: Callable[[str], Path] = build_manifest_path,
    ensure_dirs_func: Callable[[], None] = ensure_data_dirs,
) -> dict[str, Any]:
    ensure_dirs_func()
    manifest_granularity = resolve_manifest_granularity(
        manifest_granularity=manifest_granularity,
        download_profile=download_profile,
    )

    print(f"requested_download_profile={download_profile}")
    print(f"download_auto_peak_threshold={download_auto_peak_threshold}")
    print(f"manifest_granularity={manifest_granularity}")

    allowed_edinet_codes = allowed_codes_loader(master_csv_path)
    chunks = iter_manifest_chunks(start_date, end_date, granularity=manifest_granularity)

    if month_limit > 0:
        chunks = chunks[:month_limit]

    monthly_results: list[dict[str, Any]] = []
    total_manifest_rows = 0
    total_downloaded = 0
    total_existing = 0
    total_errors = 0
    total_cooldowns = 0
    aggregate_error_type_totals: dict[str, int] = {}
    effective_profile_totals: dict[str, int] = {}

    for chunk in chunks:
        manifest_name = build_chunk_manifest_name(manifest_prefix, chunk.chunk_key)
        manifest_path = manifest_path_builder(manifest_name)
        manifest_exists = manifest_path.exists()

        print(f"chunk_key={chunk.chunk_key}")
        print(f"chunk_granularity={chunk.granularity}")
        print(f"chunk_date_from={chunk.start_date.isoformat()}")
        print(f"chunk_date_to={chunk.end_date.isoformat()}")
        print(f"chunk_manifest_path={manifest_path}")
        print(f"chunk_manifest_exists={manifest_exists}")

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
        effective_profile_name = resolve_effective_download_profile(
            requested_profile=download_profile,
            manifest_rows=int(manifest_summary["manifest_rows"]),
            auto_peak_threshold=download_auto_peak_threshold,
        )
        effective_profile_totals[effective_profile_name] = effective_profile_totals.get(effective_profile_name, 0) + 1

        print(f"chunk_manifest_rows={manifest_summary['manifest_rows']}")
        print(f"chunk_effective_download_profile={effective_profile_name}")

        if prepare_only:
            download_summary = {
                "manifest_path": str(manifest_path),
                "manifest_rows": manifest_summary["manifest_rows"],
                "target_total": 0,
                "downloaded_total": 0,
                "existing_total": 0,
                "error_total": 0,
                "processed_total": 0,
                "cooldown_count": 0,
                "error_type_totals": {},
                "skipped": True,
            }
            print("download_skipped=1")
        else:
            runtime_settings = resolve_download_runtime_settings(
                profile_name=effective_profile_name,
                batch_size=download_batch_size,
                connect_timeout_sec=download_connect_timeout_sec,
                read_timeout_sec=download_read_timeout_sec,
                max_retries=download_max_retries,
                retry_wait_sec=download_retry_wait_sec,
                progress_every=download_progress_every,
                cooldown_failure_streak=download_cooldown_failure_streak,
                cooldown_sec=download_cooldown_sec,
            )
            download_summary = download_func(
                api_key=api_key,
                manifest_path=manifest_path,
                batch_size=runtime_settings["batch_size"],
                run_all=download_run_all,
                retry_errors=download_retry_errors,
                max_docs=download_max_docs,
                connect_timeout_sec=runtime_settings["connect_timeout_sec"],
                read_timeout_sec=runtime_settings["read_timeout_sec"],
                max_retries=runtime_settings["max_retries"],
                retry_wait_sec=runtime_settings["retry_wait_sec"],
                progress_every=runtime_settings["progress_every"],
                cooldown_failure_streak=runtime_settings["cooldown_failure_streak"],
                cooldown_sec=runtime_settings["cooldown_sec"],
                submit_date_text=download_submit_date_text,
                submit_date_from_text=download_submit_date_from_text,
                submit_date_to_text=download_submit_date_to_text,
                submit_time_from_text=download_submit_time_from_text,
                submit_time_to_text=download_submit_time_to_text,
            )
            total_downloaded += int(download_summary.get("downloaded_total", 0))
            total_existing += int(download_summary.get("existing_total", 0))
            total_errors += int(download_summary.get("error_total", 0))
            total_cooldowns += int(download_summary.get("cooldown_count", 0))
            for error_type, count in dict(download_summary.get("error_type_totals", {})).items():
                aggregate_error_type_totals[error_type] = aggregate_error_type_totals.get(error_type, 0) + int(count)

        monthly_results.append(
            {
                "chunk_key": chunk.chunk_key,
                "manifest_name": manifest_name,
                "manifest_path": str(manifest_path),
                "effective_download_profile": effective_profile_name,
                "collect_summary": collect_summary,
                "manifest_summary": manifest_summary,
                "download_summary": download_summary,
            }
        )

    print(f"backfill_chunks={len(chunks)}")
    print(f"backfill_manifest_rows_total={total_manifest_rows}")
    print(f"backfill_downloaded_total={total_downloaded}")
    print(f"backfill_existing_total={total_existing}")
    print(f"backfill_error_total={total_errors}")
    print(f"backfill_cooldown_total={total_cooldowns}")
    profile_parts = [f"{profile_name}:{count}" for profile_name, count in sorted(effective_profile_totals.items())]
    print(f"backfill_effective_profile_totals={'|'.join(profile_parts)}")
    if aggregate_error_type_totals:
        parts = [f"{error_type}:{count}" for error_type, count in sorted(aggregate_error_type_totals.items())]
        print(f"backfill_error_type_totals={'|'.join(parts)}")
    else:
        print("backfill_error_type_totals=none")

    return {
        "months": len(chunks),
        "chunks": len(chunks),
        "manifest_rows_total": total_manifest_rows,
        "downloaded_total": total_downloaded,
        "existing_total": total_existing,
        "error_total": total_errors,
        "cooldown_total": total_cooldowns,
        "effective_profile_totals": dict(sorted(effective_profile_totals.items())),
        "error_type_totals": dict(sorted(aggregate_error_type_totals.items())),
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
        help="Prefix for generated manifest names.",
    )
    parser.add_argument(
        "--manifest-granularity",
        choices=["month", "day"],
        default=os.getenv("EDINET_MANIFEST_GRANULARITY", "").strip().lower(),
        help="Split manifests by month or day. Empty means profile default.",
    )
    parser.add_argument(
        "--master-csv-path",
        default=os.getenv("EDINET_TSE_MASTER_CSV", "").strip(),
        help="Path to TSE issuer master CSV.",
    )
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--overwrite-manifests", action="store_true")
    parser.add_argument("--month-limit", type=int, default=0, help="Optional cap for generated chunks.")
    parser.add_argument(
        "--download-profile",
        choices=sorted([*DOWNLOAD_PROFILES.keys(), AUTO_DOWNLOAD_PROFILE]),
        default=os.getenv("EDINET_DOWNLOAD_PROFILE", DOWNLOAD_PROFILE_DEFAULT).strip().lower() or DOWNLOAD_PROFILE_DEFAULT,
    )
    parser.add_argument("--download-auto-peak-threshold", type=int, default=AUTO_DOWNLOAD_PEAK_THRESHOLD)
    parser.add_argument("--download-batch-size", type=int, default=None)
    parser.add_argument("--download-run-all", action="store_true")
    parser.add_argument("--download-retry-errors", action="store_true")
    parser.add_argument("--download-max-docs", type=int, default=0)
    parser.add_argument("--download-connect-timeout-sec", type=int, default=None)
    parser.add_argument("--download-read-timeout-sec", type=int, default=None)
    parser.add_argument("--download-max-retries", type=int, default=None)
    parser.add_argument("--download-retry-wait-sec", type=float, default=None)
    parser.add_argument("--download-progress-every", type=int, default=None)
    parser.add_argument("--download-cooldown-failure-streak", type=int, default=None)
    parser.add_argument("--download-cooldown-sec", type=float, default=None)
    parser.add_argument(
        "--download-submit-date",
        default=os.getenv("EDINET_SUBMIT_DATE", "").strip(),
        help="Download only rows whose submit date is YYYY-MM-DD.",
    )
    parser.add_argument(
        "--download-submit-date-from",
        default=os.getenv("EDINET_SUBMIT_DATE_FROM", "").strip(),
        help="Download start submit date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--download-submit-date-to",
        default=os.getenv("EDINET_SUBMIT_DATE_TO", "").strip(),
        help="Download end submit date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--download-submit-time-from",
        default=os.getenv("EDINET_SUBMIT_TIME_FROM", "").strip(),
        help="Download start submit time in HH:MM format.",
    )
    parser.add_argument(
        "--download-submit-time-to",
        default=os.getenv("EDINET_SUBMIT_TIME_TO", "").strip(),
        help="Download end submit time in HH:MM format.",
    )
    return parser


def main() -> None:
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("Set EDINET_API_KEY before running.")

    args = build_arg_parser().parse_args()
    start_date = date.fromisoformat(args.date_from)
    end_date = date.fromisoformat(args.date_to)
    master_csv_path = Path(args.master_csv_path or TSE_LISTING_MASTER_CSV_PATH)
    (
        download_submit_date_text,
        download_submit_date_from_text,
        download_submit_date_to_text,
        download_submit_time_from_text,
        download_submit_time_to_text,
    ) = resolve_submit_filters(
        submit_date_text=args.download_submit_date,
        submit_date_from_text=args.download_submit_date_from,
        submit_date_to_text=args.download_submit_date_to,
        submit_time_from_text=args.download_submit_time_from,
        submit_time_to_text=args.download_submit_time_to,
    )

    run_zip_backfill(
        api_key=api_key,
        start_date=start_date,
        end_date=end_date,
        manifest_prefix=args.manifest_prefix,
        manifest_granularity=args.manifest_granularity,
        master_csv_path=master_csv_path,
        prepare_only=args.prepare_only,
        overwrite_manifests=args.overwrite_manifests,
        month_limit=args.month_limit,
        download_profile=args.download_profile,
        download_auto_peak_threshold=args.download_auto_peak_threshold,
        download_batch_size=args.download_batch_size,
        download_run_all=args.download_run_all,
        download_retry_errors=args.download_retry_errors,
        download_max_docs=args.download_max_docs,
        download_connect_timeout_sec=args.download_connect_timeout_sec,
        download_read_timeout_sec=args.download_read_timeout_sec,
        download_max_retries=args.download_max_retries,
        download_retry_wait_sec=args.download_retry_wait_sec,
        download_progress_every=args.download_progress_every,
        download_cooldown_failure_streak=args.download_cooldown_failure_streak,
        download_cooldown_sec=args.download_cooldown_sec,
        download_submit_date_text=download_submit_date_text,
        download_submit_date_from_text=download_submit_date_from_text,
        download_submit_date_to_text=download_submit_date_to_text,
        download_submit_time_from_text=download_submit_time_from_text,
        download_submit_time_to_text=download_submit_time_to_text,
    )


if __name__ == "__main__":
    main()

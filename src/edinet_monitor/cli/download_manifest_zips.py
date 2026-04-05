from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Any, Callable

from edinet_monitor.config.settings import (
    DOWNLOAD_CONNECT_TIMEOUT_SEC,
    DOWNLOAD_COOLDOWN_FAILURE_STREAK,
    DOWNLOAD_COOLDOWN_SEC,
    DOWNLOAD_MAX_RETRIES,
    DOWNLOAD_PEAK_BATCH_SIZE,
    DOWNLOAD_PEAK_CONNECT_TIMEOUT_SEC,
    DOWNLOAD_PEAK_COOLDOWN_FAILURE_STREAK,
    DOWNLOAD_PEAK_COOLDOWN_SEC,
    DOWNLOAD_PEAK_MANIFEST_GRANULARITY,
    DOWNLOAD_PEAK_MAX_RETRIES,
    DOWNLOAD_PEAK_PROGRESS_EVERY,
    DOWNLOAD_PEAK_READ_TIMEOUT_SEC,
    DOWNLOAD_PEAK_RETRY_WAIT_SEC,
    DOWNLOAD_PROFILE_DEFAULT,
    DOWNLOAD_PROGRESS_EVERY,
    DOWNLOAD_READ_TIMEOUT_SEC,
    DOWNLOAD_RETRY_WAIT_SEC,
    ensure_data_dirs,
)
from edinet_monitor.services.collector.document_download_service import download_document_zip
from edinet_monitor.services.collector.manifest_download_service import (
    matches_manifest_row_submit_filter,
    process_manifest_download_row,
    select_manifest_row_indexes,
)
from edinet_monitor.services.storage.manifest_service import (
    build_manifest_path,
    read_manifest_rows,
    summarize_manifest_rows,
    write_manifest_rows,
)


@dataclass(frozen=True)
class DownloadProfile:
    batch_size: int
    connect_timeout_sec: int
    read_timeout_sec: int
    max_retries: int
    retry_wait_sec: float
    progress_every: int
    cooldown_failure_streak: int
    cooldown_sec: float
    manifest_granularity: str


DOWNLOAD_PROFILES: dict[str, DownloadProfile] = {
    "normal": DownloadProfile(
        batch_size=20,
        connect_timeout_sec=DOWNLOAD_CONNECT_TIMEOUT_SEC,
        read_timeout_sec=DOWNLOAD_READ_TIMEOUT_SEC,
        max_retries=DOWNLOAD_MAX_RETRIES,
        retry_wait_sec=DOWNLOAD_RETRY_WAIT_SEC,
        progress_every=DOWNLOAD_PROGRESS_EVERY,
        cooldown_failure_streak=DOWNLOAD_COOLDOWN_FAILURE_STREAK,
        cooldown_sec=DOWNLOAD_COOLDOWN_SEC,
        manifest_granularity="month",
    ),
    "peak": DownloadProfile(
        batch_size=DOWNLOAD_PEAK_BATCH_SIZE,
        connect_timeout_sec=DOWNLOAD_PEAK_CONNECT_TIMEOUT_SEC,
        read_timeout_sec=DOWNLOAD_PEAK_READ_TIMEOUT_SEC,
        max_retries=DOWNLOAD_PEAK_MAX_RETRIES,
        retry_wait_sec=DOWNLOAD_PEAK_RETRY_WAIT_SEC,
        progress_every=DOWNLOAD_PEAK_PROGRESS_EVERY,
        cooldown_failure_streak=DOWNLOAD_PEAK_COOLDOWN_FAILURE_STREAK,
        cooldown_sec=DOWNLOAD_PEAK_COOLDOWN_SEC,
        manifest_granularity=DOWNLOAD_PEAK_MANIFEST_GRANULARITY,
    ),
}


def validate_submit_time_text(time_text: str) -> str:
    normalized = str(time_text or "").strip()
    if not normalized:
        return ""
    try:
        return datetime.strptime(normalized, "%H:%M").strftime("%H:%M")
    except ValueError as exc:
        raise ValueError("submit_time_from and submit_time_to must use HH:MM format.") from exc


def resolve_download_profile(profile_name: str) -> DownloadProfile:
    normalized = str(profile_name or DOWNLOAD_PROFILE_DEFAULT).strip().lower() or DOWNLOAD_PROFILE_DEFAULT
    if normalized not in DOWNLOAD_PROFILES:
        raise ValueError(f"Unknown download profile: {profile_name}")
    return DOWNLOAD_PROFILES[normalized]


def resolve_download_runtime_settings(
    *,
    profile_name: str,
    batch_size: int | None,
    connect_timeout_sec: int | None,
    read_timeout_sec: int | None,
    max_retries: int | None,
    retry_wait_sec: float | None,
    progress_every: int | None,
    cooldown_failure_streak: int | None,
    cooldown_sec: float | None,
) -> dict[str, Any]:
    profile = resolve_download_profile(profile_name)
    return {
        "profile_name": str(profile_name or DOWNLOAD_PROFILE_DEFAULT).strip().lower() or DOWNLOAD_PROFILE_DEFAULT,
        "batch_size": profile.batch_size if batch_size is None or batch_size <= 0 else batch_size,
        "connect_timeout_sec": (
            profile.connect_timeout_sec if connect_timeout_sec is None or connect_timeout_sec <= 0 else connect_timeout_sec
        ),
        "read_timeout_sec": profile.read_timeout_sec if read_timeout_sec is None or read_timeout_sec <= 0 else read_timeout_sec,
        "max_retries": profile.max_retries if max_retries is None or max_retries < 0 else max_retries,
        "retry_wait_sec": profile.retry_wait_sec if retry_wait_sec is None or retry_wait_sec < 0 else retry_wait_sec,
        "progress_every": profile.progress_every if progress_every is None or progress_every < 0 else progress_every,
        "cooldown_failure_streak": (
            profile.cooldown_failure_streak
            if cooldown_failure_streak is None or cooldown_failure_streak < 0
            else cooldown_failure_streak
        ),
        "cooldown_sec": profile.cooldown_sec if cooldown_sec is None or cooldown_sec < 0 else cooldown_sec,
        "recommended_manifest_granularity": profile.manifest_granularity,
    }


def build_filtered_manifest_summary(
    rows: list[dict[str, Any]],
    *,
    target_date_text: str = "",
    date_from_text: str = "",
    date_to_text: str = "",
    time_from_text: str = "",
    time_to_text: str = "",
) -> dict[str, Any]:
    filtered_rows = [
        row
        for row in rows
        if matches_manifest_row_submit_filter(
            row,
            target_date_text=target_date_text,
            date_from_text=date_from_text,
            date_to_text=date_to_text,
            time_from_text=time_from_text,
            time_to_text=time_to_text,
        )
    ]
    return summarize_manifest_rows(filtered_rows)


def print_error_type_counts(*, label: str, error_type_counts: dict[str, int]) -> None:
    if not error_type_counts:
        print(f"{label}_error_type_counts=none")
        return

    parts = [f"{error_type}:{count}" for error_type, count in sorted(error_type_counts.items())]
    print(f"{label}_error_type_counts={'|'.join(parts)}")


def print_progress_snapshot(
    *,
    label: str,
    rows: list[dict[str, Any]],
    processed_total: int,
    downloaded_total: int,
    existing_total: int,
    error_total: int,
    retry_errors: bool,
    target_date_text: str,
    date_from_text: str,
    date_to_text: str,
    time_from_text: str,
    time_to_text: str,
) -> None:
    remaining_rows = select_manifest_row_indexes(
        rows,
        limit=len(rows),
        retry_errors=retry_errors,
        target_date_text=target_date_text,
        date_from_text=date_from_text,
        date_to_text=date_to_text,
        time_from_text=time_from_text,
        time_to_text=time_to_text,
    )
    filtered_summary = build_filtered_manifest_summary(
        rows,
        target_date_text=target_date_text,
        date_from_text=date_from_text,
        date_to_text=date_to_text,
        time_from_text=time_from_text,
        time_to_text=time_to_text,
    )

    print(f"{label}_processed_total={processed_total}")
    print(f"{label}_downloaded_total={downloaded_total}")
    print(f"{label}_existing_total={existing_total}")
    print(f"{label}_error_total={error_total}")
    print(f"{label}_remaining_total={len(remaining_rows)}")
    print(f"{label}_filtered_manifest_rows={filtered_summary['manifest_rows']}")
    print(f"{label}_filtered_pending_rows={filtered_summary['pending_rows']}")
    print(f"{label}_filtered_downloaded_rows={filtered_summary['downloaded_rows']}")
    print(f"{label}_filtered_error_rows={filtered_summary['error_rows']}")
    print(f"{label}_filtered_retryable_error_rows={filtered_summary['retryable_error_rows']}")
    print_error_type_counts(label=label, error_type_counts=filtered_summary["error_type_counts"])


def run_download_manifest_zips(
    *,
    api_key: str,
    manifest_path: Path,
    batch_size: int = 20,
    run_all: bool = False,
    retry_errors: bool = False,
    max_docs: int = 0,
    connect_timeout_sec: int = DOWNLOAD_CONNECT_TIMEOUT_SEC,
    read_timeout_sec: int = DOWNLOAD_READ_TIMEOUT_SEC,
    max_retries: int = DOWNLOAD_MAX_RETRIES,
    retry_wait_sec: float = DOWNLOAD_RETRY_WAIT_SEC,
    progress_every: int = DOWNLOAD_PROGRESS_EVERY,
    cooldown_failure_streak: int = DOWNLOAD_COOLDOWN_FAILURE_STREAK,
    cooldown_sec: float = DOWNLOAD_COOLDOWN_SEC,
    submit_date_text: str = "",
    submit_date_from_text: str = "",
    submit_date_to_text: str = "",
    submit_time_from_text: str = "",
    submit_time_to_text: str = "",
    downloader: Callable[..., Path] = download_document_zip,
    sleep_func: Callable[[float], None] = sleep,
) -> dict[str, Any]:
    rows = read_manifest_rows(manifest_path)
    if not rows:
        print(f"manifest_path={manifest_path}")
        print("manifest_rows=0")
        return {
            "manifest_path": str(manifest_path),
            "manifest_rows": 0,
            "target_total": 0,
            "downloaded_total": 0,
            "existing_total": 0,
            "error_total": 0,
            "processed_total": 0,
            "error_type_totals": {},
        }

    total_target = 0
    total_downloaded = 0
    total_existing = 0
    total_errors = 0
    total_processed = 0
    consecutive_cooldown_errors = 0
    cooldown_count = 0
    error_type_totals: dict[str, int] = {}

    initial_summary = build_filtered_manifest_summary(
        rows,
        target_date_text=submit_date_text,
        date_from_text=submit_date_from_text,
        date_to_text=submit_date_to_text,
        time_from_text=submit_time_from_text,
        time_to_text=submit_time_to_text,
    )

    print(f"manifest_path={manifest_path}")
    print(f"initial_manifest_rows={initial_summary['manifest_rows']}")
    print(f"initial_pending_rows={initial_summary['pending_rows']}")
    print(f"initial_downloaded_rows={initial_summary['downloaded_rows']}")
    print(f"initial_error_rows={initial_summary['error_rows']}")
    print(f"initial_retryable_error_rows={initial_summary['retryable_error_rows']}")
    print_error_type_counts(label="initial", error_type_counts=initial_summary["error_type_counts"])

    while True:
        remaining_limit = batch_size
        if max_docs > 0:
            remaining_limit = min(remaining_limit, max(max_docs - total_processed, 0))

        if remaining_limit <= 0:
            break

        target_indexes = select_manifest_row_indexes(
            rows,
            limit=remaining_limit,
            retry_errors=retry_errors,
            target_date_text=submit_date_text,
            date_from_text=submit_date_from_text,
            date_to_text=submit_date_to_text,
            time_from_text=submit_time_from_text,
            time_to_text=submit_time_to_text,
        )

        print(f"manifest_target_rows={len(target_indexes)}")

        if not target_indexes:
            break

        total_target += len(target_indexes)

        for idx in target_indexes:
            row = rows[idx]
            doc_id = str(row.get("doc_id") or "")
            submit_date = str(row.get("submit_date") or "")
            zip_path = str(row.get("zip_path") or "")

            print(f"[DEBUG] target_doc_id={doc_id} submit_date={submit_date}")
            print(f"[DEBUG] output_path={zip_path}")

            result = process_manifest_download_row(
                row,
                api_key=api_key,
                downloader=downloader,
                connect_timeout_sec=connect_timeout_sec,
                read_timeout_sec=read_timeout_sec,
                max_retries=max_retries,
                retry_wait_sec=retry_wait_sec,
                sleep_func=sleep_func,
            )
            rows[idx] = row
            total_processed += 1

            if result["result"] == "existing":
                total_existing += 1
                consecutive_cooldown_errors = 0
                print(
                    f"existing_zip doc_id={doc_id} path={result['path']} attempts_used={result['attempts_used']}"
                )
            elif result["result"] == "downloaded":
                total_downloaded += 1
                consecutive_cooldown_errors = 0
                print(
                    f"downloaded doc_id={doc_id} path={result['path']} attempts_used={result['attempts_used']}"
                )
            else:
                total_errors += 1
                error_type = str(result["error_type"] or "unknown_error")
                error_type_totals[error_type] = error_type_totals.get(error_type, 0) + 1
                if result["cooldown_eligible"]:
                    consecutive_cooldown_errors += 1
                else:
                    consecutive_cooldown_errors = 0
                print(
                    "download_error "
                    f"doc_id={doc_id} "
                    f"error_type={result['error_type']} "
                    f"retryable={result['retryable']} "
                    f"status_code={result['status_code']} "
                    f"attempts_used={result['attempts_used']} "
                    f"error={row.get('download_error')}"
                )

            write_manifest_rows(manifest_path, rows)

            should_print_progress = progress_every > 0 and (
                total_processed == 1 or total_processed % progress_every == 0
            )
            if should_print_progress:
                print_progress_snapshot(
                    label="progress",
                    rows=rows,
                    processed_total=total_processed,
                    downloaded_total=total_downloaded,
                    existing_total=total_existing,
                    error_total=total_errors,
                    retry_errors=retry_errors,
                    target_date_text=submit_date_text,
                    date_from_text=submit_date_from_text,
                    date_to_text=submit_date_to_text,
                    time_from_text=submit_time_from_text,
                    time_to_text=submit_time_to_text,
                )

            if (
                cooldown_failure_streak > 0
                and cooldown_sec > 0
                and consecutive_cooldown_errors >= cooldown_failure_streak
            ):
                cooldown_count += 1
                print(
                    f"cooldown_start consecutive_failures={consecutive_cooldown_errors} "
                    f"cooldown_sec={cooldown_sec}"
                )
                sleep_func(cooldown_sec)
                print("cooldown_end=1")
                consecutive_cooldown_errors = 0

            if max_docs > 0 and total_processed >= max_docs:
                break

        if not run_all:
            break

        if max_docs > 0 and total_processed >= max_docs:
            break

    print_progress_snapshot(
        label="final",
        rows=rows,
        processed_total=total_processed,
        downloaded_total=total_downloaded,
        existing_total=total_existing,
        error_total=total_errors,
        retry_errors=retry_errors,
        target_date_text=submit_date_text,
        date_from_text=submit_date_from_text,
        date_to_text=submit_date_to_text,
        time_from_text=submit_time_from_text,
        time_to_text=submit_time_to_text,
    )

    print(f"download_target_total={total_target}")
    print(f"downloaded_total={total_downloaded}")
    print(f"existing_total={total_existing}")
    print(f"download_error_total={total_errors}")
    print(f"processed_total={total_processed}")
    print(f"cooldown_count={cooldown_count}")
    print_error_type_counts(label="run", error_type_counts=error_type_totals)

    return {
        "manifest_path": str(manifest_path),
        "manifest_rows": len(rows),
        "target_total": total_target,
        "downloaded_total": total_downloaded,
        "existing_total": total_existing,
        "error_total": total_errors,
        "processed_total": total_processed,
        "cooldown_count": cooldown_count,
        "error_type_totals": dict(sorted(error_type_totals.items())),
        "initial_summary": initial_summary,
        "final_summary": build_filtered_manifest_summary(
            rows,
            target_date_text=submit_date_text,
            date_from_text=submit_date_from_text,
            date_to_text=submit_date_to_text,
            time_from_text=submit_time_from_text,
            time_to_text=submit_time_to_text,
        ),
    }


def resolve_submit_filters(
    *,
    submit_date_text: str,
    submit_date_from_text: str,
    submit_date_to_text: str,
    submit_time_from_text: str,
    submit_time_to_text: str,
) -> tuple[str, str, str, str, str]:
    submit_date_text = str(submit_date_text or "").strip()
    submit_date_from_text = str(submit_date_from_text or "").strip()
    submit_date_to_text = str(submit_date_to_text or "").strip()
    submit_time_from_text = validate_submit_time_text(submit_time_from_text)
    submit_time_to_text = validate_submit_time_text(submit_time_to_text)

    if submit_date_text and (submit_date_from_text or submit_date_to_text):
        raise ValueError("Use either submit_date or submit_date_from/submit_date_to.")

    if submit_date_from_text or submit_date_to_text:
        if not submit_date_from_text or not submit_date_to_text:
            raise ValueError("Both submit_date_from and submit_date_to are required.")
        if submit_date_from_text > submit_date_to_text:
            raise ValueError("submit_date_from must be earlier than or equal to submit_date_to.")

    if submit_time_from_text or submit_time_to_text:
        if not submit_time_from_text or not submit_time_to_text:
            raise ValueError("Both submit_time_from and submit_time_to are required.")
        if submit_time_from_text > submit_time_to_text:
            raise ValueError("submit_time_from must be earlier than or equal to submit_time_to.")

    return (
        submit_date_text,
        submit_date_from_text,
        submit_date_to_text,
        submit_time_from_text,
        submit_time_to_text,
    )


def resolve_submit_date_filters(
    *,
    submit_date_text: str,
    submit_date_from_text: str,
    submit_date_to_text: str,
) -> tuple[str, str, str]:
    resolved = resolve_submit_filters(
        submit_date_text=submit_date_text,
        submit_date_from_text=submit_date_from_text,
        submit_date_to_text=submit_date_to_text,
        submit_time_from_text="",
        submit_time_to_text="",
    )
    return resolved[0], resolved[1], resolved[2]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest-name",
        default=os.getenv("EDINET_MANIFEST_NAME", "").strip(),
        help="Manifest name without extension.",
    )
    parser.add_argument(
        "--manifest-path",
        default=os.getenv("EDINET_MANIFEST_PATH", "").strip(),
        help="Optional full path to manifest JSONL.",
    )
    parser.add_argument(
        "--download-profile",
        choices=sorted(DOWNLOAD_PROFILES.keys()),
        default=os.getenv("EDINET_DOWNLOAD_PROFILE", DOWNLOAD_PROFILE_DEFAULT).strip().lower() or DOWNLOAD_PROFILE_DEFAULT,
    )
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--run-all", action="store_true")
    parser.add_argument("--retry-errors", action="store_true")
    parser.add_argument("--max-docs", type=int, default=0, help="Optional cap for processed documents in this run.")
    parser.add_argument("--connect-timeout-sec", type=int, default=None)
    parser.add_argument("--read-timeout-sec", type=int, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--retry-wait-sec", type=float, default=None)
    parser.add_argument("--progress-every", type=int, default=None)
    parser.add_argument("--cooldown-failure-streak", type=int, default=None)
    parser.add_argument("--cooldown-sec", type=float, default=None)
    parser.add_argument(
        "--submit-date",
        default=os.getenv("EDINET_SUBMIT_DATE", "").strip(),
        help="Process only rows whose submit date is YYYY-MM-DD.",
    )
    parser.add_argument(
        "--submit-date-from",
        default=os.getenv("EDINET_SUBMIT_DATE_FROM", "").strip(),
        help="Start submit date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--submit-date-to",
        default=os.getenv("EDINET_SUBMIT_DATE_TO", "").strip(),
        help="End submit date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--submit-time-from",
        default=os.getenv("EDINET_SUBMIT_TIME_FROM", "").strip(),
        help="Start submit time in HH:MM format.",
    )
    parser.add_argument(
        "--submit-time-to",
        default=os.getenv("EDINET_SUBMIT_TIME_TO", "").strip(),
        help="End submit time in HH:MM format.",
    )
    return parser


def resolve_manifest_path(*, manifest_name: str, manifest_path_text: str) -> Path:
    if manifest_path_text:
        return Path(manifest_path_text)

    if manifest_name:
        return build_manifest_path(manifest_name)

    raise ValueError("Specify either manifest_name or manifest_path.")


def main() -> None:
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("Set EDINET_API_KEY before running.")

    args = build_arg_parser().parse_args()
    ensure_data_dirs()

    (
        submit_date_text,
        submit_date_from_text,
        submit_date_to_text,
        submit_time_from_text,
        submit_time_to_text,
    ) = resolve_submit_filters(
        submit_date_text=args.submit_date,
        submit_date_from_text=args.submit_date_from,
        submit_date_to_text=args.submit_date_to,
        submit_time_from_text=args.submit_time_from,
        submit_time_to_text=args.submit_time_to,
    )

    runtime_settings = resolve_download_runtime_settings(
        profile_name=args.download_profile,
        batch_size=args.batch_size,
        connect_timeout_sec=args.connect_timeout_sec,
        read_timeout_sec=args.read_timeout_sec,
        max_retries=args.max_retries,
        retry_wait_sec=args.retry_wait_sec,
        progress_every=args.progress_every,
        cooldown_failure_streak=args.cooldown_failure_streak,
        cooldown_sec=args.cooldown_sec,
    )

    print(f"download_profile={runtime_settings['profile_name']}")
    print(f"recommended_manifest_granularity={runtime_settings['recommended_manifest_granularity']}")

    manifest_path = resolve_manifest_path(
        manifest_name=args.manifest_name,
        manifest_path_text=args.manifest_path,
    )

    run_download_manifest_zips(
        api_key=api_key,
        manifest_path=manifest_path,
        batch_size=runtime_settings["batch_size"],
        run_all=args.run_all,
        retry_errors=args.retry_errors,
        max_docs=args.max_docs,
        connect_timeout_sec=runtime_settings["connect_timeout_sec"],
        read_timeout_sec=runtime_settings["read_timeout_sec"],
        max_retries=runtime_settings["max_retries"],
        retry_wait_sec=runtime_settings["retry_wait_sec"],
        progress_every=runtime_settings["progress_every"],
        cooldown_failure_streak=runtime_settings["cooldown_failure_streak"],
        cooldown_sec=runtime_settings["cooldown_sec"],
        submit_date_text=submit_date_text,
        submit_date_from_text=submit_date_from_text,
        submit_date_to_text=submit_date_to_text,
        submit_time_from_text=submit_time_from_text,
        submit_time_to_text=submit_time_to_text,
    )


if __name__ == "__main__":
    main()

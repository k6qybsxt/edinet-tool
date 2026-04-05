from __future__ import annotations

import argparse
import os
from pathlib import Path

from edinet_monitor.cli.download_manifest_zips import resolve_submit_filters
from edinet_monitor.services.collector.manifest_download_service import matches_manifest_row_submit_filter
from edinet_monitor.services.storage.manifest_service import (
    build_manifest_path,
    read_manifest_rows,
    summarize_manifest_rows,
)


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
        "--submit-date",
        default=os.getenv("EDINET_SUBMIT_DATE", "").strip(),
        help="Filter rows by exact submit date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--submit-date-from",
        default=os.getenv("EDINET_SUBMIT_DATE_FROM", "").strip(),
        help="Filter start submit date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--submit-date-to",
        default=os.getenv("EDINET_SUBMIT_DATE_TO", "").strip(),
        help="Filter end submit date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--submit-time-from",
        default=os.getenv("EDINET_SUBMIT_TIME_FROM", "").strip(),
        help="Filter start submit time HH:MM.",
    )
    parser.add_argument(
        "--submit-time-to",
        default=os.getenv("EDINET_SUBMIT_TIME_TO", "").strip(),
        help="Filter end submit time HH:MM.",
    )
    return parser


def resolve_manifest_path(*, manifest_name: str, manifest_path_text: str) -> Path:
    if manifest_path_text:
        return Path(manifest_path_text)

    if manifest_name:
        return build_manifest_path(manifest_name)

    raise ValueError("Specify either manifest_name or manifest_path.")


def main() -> None:
    args = build_arg_parser().parse_args()
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
    manifest_path = resolve_manifest_path(
        manifest_name=args.manifest_name,
        manifest_path_text=args.manifest_path,
    )
    rows = [
        row
        for row in read_manifest_rows(manifest_path)
        if matches_manifest_row_submit_filter(
            row,
            target_date_text=submit_date_text,
            date_from_text=submit_date_from_text,
            date_to_text=submit_date_to_text,
            time_from_text=submit_time_from_text,
            time_to_text=submit_time_to_text,
        )
    ]
    summary = summarize_manifest_rows(rows)

    print(f"manifest_path={manifest_path}")
    print(f"manifest_rows={summary['manifest_rows']}")
    print(f"pending_rows={summary['pending_rows']}")
    print(f"downloaded_rows={summary['downloaded_rows']}")
    print(f"error_rows={summary['error_rows']}")
    print(f"retryable_error_rows={summary['retryable_error_rows']}")
    print(f"other_rows={summary['other_rows']}")
    if summary["error_type_counts"]:
        parts = [f"{error_type}:{count}" for error_type, count in summary["error_type_counts"].items()]
        print(f"error_type_counts={'|'.join(parts)}")
    else:
        print("error_type_counts=none")

    for row in summary["sample_errors"]:
        print(row)


if __name__ == "__main__":
    main()

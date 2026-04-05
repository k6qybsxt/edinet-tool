from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path
from typing import Any, Callable

from edinet_monitor.config.settings import TSE_LISTING_MASTER_CSV_PATH, ensure_data_dirs
from edinet_monitor.services.collector.document_filter_service import filter_target_filings
from edinet_monitor.services.collector.document_list_service import (
    DocumentListResult,
    fetch_document_list,
)
from edinet_monitor.services.collector.document_row_mapper import to_manifest_record
from edinet_monitor.services.collector.issuer_master_csv_service import load_allowed_edinet_codes
from edinet_monitor.services.collector.target_date_service import resolve_target_dates
from edinet_monitor.services.storage.manifest_service import (
    build_manifest_path,
    merge_manifest_rows,
    read_manifest_rows,
    write_manifest_rows,
)


def build_default_manifest_name(target_dates: list[date]) -> str:
    if not target_dates:
        return "document_manifest"

    start_text = target_dates[0].isoformat()
    end_text = target_dates[-1].isoformat()

    if start_text == end_text:
        return f"document_manifest_{start_text}"

    return f"document_manifest_{start_text}_to_{end_text}"


def collect_document_manifest_for_date(
    target_date: date,
    *,
    api_key: str,
    allowed_edinet_codes: set[str],
    fetcher: Callable[..., DocumentListResult] = fetch_document_list,
) -> dict[str, Any]:
    result = fetcher(
        target_date=target_date,
        api_key=api_key,
        list_type=2,
    )
    filtered_rows = filter_target_filings(result.results)
    issuer_rows = [
        row for row in filtered_rows
        if str(row.get("edinetCode") or "") in allowed_edinet_codes
    ]
    manifest_rows = [
        to_manifest_record(row, source_date=target_date.isoformat())
        for row in issuer_rows
    ]

    return {
        "target_date": target_date.isoformat(),
        "metadata_date": result.metadata.get("date"),
        "metadata_status": result.metadata.get("status"),
        "metadata_message": result.metadata.get("message"),
        "all_results": len(result.results),
        "target_results": len(filtered_rows),
        "issuer_target_results": len(issuer_rows),
        "manifest_rows": manifest_rows,
        "sample_rows": manifest_rows[:5],
    }


def collect_document_manifest_for_dates(
    target_dates: list[date],
    *,
    api_key: str,
    allowed_edinet_codes: set[str],
    manifest_path: Path,
    append: bool = False,
    overwrite: bool = False,
    fetcher: Callable[..., DocumentListResult] = fetch_document_list,
) -> dict[str, Any]:
    if append and overwrite:
        raise ValueError("Use either append or overwrite, not both.")

    if manifest_path.exists() and not append and not overwrite:
        raise FileExistsError(
            f"Manifest already exists: {manifest_path}. Use --append or --overwrite."
        )

    daily_summaries: list[dict[str, Any]] = []
    incoming_rows: list[dict[str, Any]] = []
    totals = {
        "dates": len(target_dates),
        "all_results": 0,
        "target_results": 0,
        "issuer_target_results": 0,
        "incoming_manifest_rows": 0,
    }

    for target_date in target_dates:
        summary = collect_document_manifest_for_date(
            target_date,
            api_key=api_key,
            allowed_edinet_codes=allowed_edinet_codes,
            fetcher=fetcher,
        )
        daily_summaries.append(summary)
        incoming_rows.extend(summary["manifest_rows"])

        totals["all_results"] += int(summary["all_results"])
        totals["target_results"] += int(summary["target_results"])
        totals["issuer_target_results"] += int(summary["issuer_target_results"])
        totals["incoming_manifest_rows"] += len(summary["manifest_rows"])

        print(f"target_date={summary['target_date']}")
        print(f"metadata_date={summary['metadata_date']}")
        print(f"metadata_status={summary['metadata_status']}")
        print(f"metadata_message={summary['metadata_message']}")
        print(f"all_results={summary['all_results']}")
        print(f"target_results={summary['target_results']}")
        print(f"issuer_target_results={summary['issuer_target_results']}")
        print(f"manifest_rows={len(summary['manifest_rows'])}")

        for row in summary["sample_rows"]:
            print(
                {
                    "doc_id": row.get("doc_id"),
                    "edinet_code": row.get("edinet_code"),
                    "security_code": row.get("security_code"),
                    "company_name": row.get("company_name"),
                    "submit_date": row.get("submit_date"),
                    "zip_path": row.get("zip_path"),
                }
            )

    existing_rows = read_manifest_rows(manifest_path) if append else []
    merged_rows = merge_manifest_rows(existing_rows, incoming_rows)
    saved_count = write_manifest_rows(manifest_path, merged_rows)

    print(f"manifest_path={manifest_path}")
    print(f"target_dates_count={totals['dates']}")
    print(f"total_all_results={totals['all_results']}")
    print(f"total_target_results={totals['target_results']}")
    print(f"total_issuer_target_results={totals['issuer_target_results']}")
    print(f"total_incoming_manifest_rows={totals['incoming_manifest_rows']}")
    print(f"existing_manifest_rows={len(existing_rows)}")
    print(f"saved_manifest_rows={saved_count}")

    return {
        "target_dates": [target_date.isoformat() for target_date in target_dates],
        "manifest_path": str(manifest_path),
        "daily_summaries": daily_summaries,
        "totals": totals,
        "existing_manifest_rows": len(existing_rows),
        "saved_manifest_rows": saved_count,
    }


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
    parser.add_argument(
        "--manifest-name",
        default=os.getenv("EDINET_MANIFEST_NAME", "").strip(),
        help="Manifest name without extension.",
    )
    parser.add_argument(
        "--master-csv-path",
        default=os.getenv("EDINET_TSE_MASTER_CSV", "").strip(),
        help="Path to TSE issuer master CSV.",
    )
    parser.add_argument("--append", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main() -> None:
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("Set EDINET_API_KEY before running.")

    args = build_arg_parser().parse_args()
    target_dates = resolve_target_dates(
        target_date_text=args.target_date,
        date_from_text=args.date_from,
        date_to_text=args.date_to,
    )

    ensure_data_dirs()

    csv_path = Path(args.master_csv_path or TSE_LISTING_MASTER_CSV_PATH)
    allowed_edinet_codes = load_allowed_edinet_codes(csv_path)

    manifest_name = args.manifest_name or build_default_manifest_name(target_dates)
    manifest_path = build_manifest_path(manifest_name)

    collect_document_manifest_for_dates(
        target_dates,
        api_key=api_key,
        allowed_edinet_codes=allowed_edinet_codes,
        manifest_path=manifest_path,
        append=args.append,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import os

from edinet_monitor.cli.collect_document_list_to_db import collect_document_list_for_dates
from edinet_monitor.cli.download_filing_zips import run_download_filing_zips
from edinet_monitor.cli.extract_xbrl_from_zips import run_extract_xbrl_from_zips
from edinet_monitor.cli.run_screening import run_screening
from edinet_monitor.cli.save_normalized_metrics import run_save_normalized_metrics
from edinet_monitor.cli.save_raw_facts import run_save_raw_facts
from edinet_monitor.services.collector.target_date_service import resolve_target_dates
from edinet_monitor.screening.screening_rule_service import (
    DEFAULT_RULE_NAME,
    list_rule_names,
)


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
    parser.add_argument("--screening-date", default="")
    parser.add_argument(
        "--screening-rule-name",
        default=DEFAULT_RULE_NAME,
        choices=list_rule_names(),
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    api_key = os.getenv("EDINET_API_KEY")
    if not api_key:
        raise RuntimeError("Set EDINET_API_KEY before running.")
    target_dates = resolve_target_dates(
        target_date_text=args.target_date,
        date_from_text=args.date_from,
        date_to_text=args.date_to,
    )

    collect_summary = collect_document_list_for_dates(target_dates, api_key=api_key)
    download_summary = run_download_filing_zips(
        api_key=api_key,
        batch_size=args.download_batch_size,
        run_all=True,
    )
    extract_summary = run_extract_xbrl_from_zips(
        batch_size=args.extract_batch_size,
        run_all=True,
    )
    raw_summary = run_save_raw_facts(
        batch_size=args.raw_batch_size,
        run_all=True,
    )
    normalized_summary = run_save_normalized_metrics(
        batch_size=args.normalized_batch_size,
    )
    screening_summary = run_screening(
        screening_date=args.screening_date or None,
        rule_name=args.screening_rule_name,
    )

    print("daily_pipeline_completed=1")
    print(f"daily_target_dates={','.join(collect_summary['target_dates'])}")
    print(f"daily_collect_saved_total={collect_summary['totals']['filing_saved_count']}")
    print(f"daily_downloaded_total={download_summary['downloaded_total']}")
    print(f"daily_xbrl_extracted_total={extract_summary['extracted_total']}")
    print(f"daily_raw_facts_saved_docs_total={raw_summary['saved_docs_total']}")
    print(f"daily_normalized_metrics_saved_docs_total={normalized_summary['saved_docs_total']}")
    print(f"daily_screening_rule_name={screening_summary['rule_name']}")
    print(f"daily_screening_target_count={screening_summary['target_count']}")
    print(f"daily_screening_hit_count={screening_summary['hit_count']}")


if __name__ == "__main__":
    main()

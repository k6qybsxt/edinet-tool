from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.config.settings import DB_PATH, OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.data_quality_report_service import (
    DataQualityReportOptions,
    export_data_quality_report,
)


def _split_csv(value: str) -> tuple[str, ...]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return ()
    return tuple(part.strip() for part in text.split(",") if part.strip())


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export an EDINET + J-Quants data quality report to CLI summary and Excel."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT / "data_quality"))
    parser.add_argument("--date-from", default="")
    parser.add_argument("--date-to", default="")
    parser.add_argument("--codes", default="all")
    parser.add_argument("--industry-33", default="all")
    parser.add_argument("--coverage-warning-threshold", type=float, default=0.8)
    parser.add_argument("--extreme-ratio-threshold", type=float, default=5.0)
    parser.add_argument("--limit-preview", type=int, default=10)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    db_path = Path(args.db_path)
    create_tables(db_path)
    conn = get_connection(db_path)
    try:
        result = export_data_quality_report(
            conn,
            options=DataQualityReportOptions(
                date_from=str(args.date_from or "").strip(),
                date_to=str(args.date_to or "").strip(),
                codes=_split_csv(args.codes),
                industry_33_list=_split_csv(args.industry_33),
                output_dir=Path(args.output_dir),
                coverage_warning_threshold=args.coverage_warning_threshold,
                extreme_ratio_threshold=args.extreme_ratio_threshold,
            ),
        )
    finally:
        conn.close()

    print(f"report_id={result.run_id}")
    print(f"date_from={result.date_from}")
    print(f"date_to={result.date_to}")
    print(f"issue_count={result.issue_count}")
    print(f"critical={result.counts_by_severity.get('critical', 0)}")
    print(f"warning={result.counts_by_severity.get('warning', 0)}")
    print(f"info={result.counts_by_severity.get('info', 0)}")
    print(f"previous_run_id={result.previous_run_id}")
    print(f"excel_path={result.excel_path}")
    preview_limit = max(args.limit_preview, 0)
    for item in result.items[:preview_limit]:
        print(
            "issue="
            f"{item.severity}|{item.category}|{item.check_name}|{item.subject}|"
            f"{item.current_value}|{item.message}"
        )


if __name__ == "__main__":
    main()

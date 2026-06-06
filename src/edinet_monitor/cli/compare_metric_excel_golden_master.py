from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.services.metric_excel_golden_master_service import (
    DEFAULT_GOLDEN_MASTER_DIFF_OUTPUT_DIR,
    compare_metric_excel_golden_master,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare a metric Excel workbook or normalized JSON against a Golden Master JSON."
    )
    parser.add_argument("--golden-json", required=True)
    parser.add_argument("--actual-excel", default="")
    parser.add_argument("--actual-json", default="")
    parser.add_argument("--output-dir", default=str(DEFAULT_GOLDEN_MASTER_DIFF_OUTPUT_DIR))
    parser.add_argument("--limit-preview", type=int, default=10)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    actual_excel = str(args.actual_excel or "").strip()
    actual_json = str(args.actual_json or "").strip()
    result = compare_metric_excel_golden_master(
        golden_json_path=Path(args.golden_json),
        actual_excel_path=Path(actual_excel) if actual_excel else None,
        actual_json_path=Path(actual_json) if actual_json else None,
        output_dir=Path(args.output_dir),
    )
    print(f"comparison_id={result.comparison_id}")
    print(f"issue_count={result.issue_count}")
    print(f"critical={result.counts_by_severity.get('critical', 0)}")
    print(f"warning={result.counts_by_severity.get('warning', 0)}")
    print(f"actual_json_path={result.actual_json_path}")
    print(f"json_path={result.report_json_path}")
    print(f"excel_path={result.report_excel_path}")
    preview_limit = max(int(args.limit_preview), 0)
    for issue in result.issues[:preview_limit]:
        print(
            "issue="
            f"{issue.severity}|{issue.category}|{issue.check_name}|{issue.sheet_name}|"
            f"{issue.period_label}|{issue.field_name}|{issue.message}"
        )


if __name__ == "__main__":
    main()

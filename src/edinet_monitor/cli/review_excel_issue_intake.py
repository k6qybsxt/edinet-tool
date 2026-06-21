from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.services.excel_issue_intake_service import (
    DEFAULT_EXCEL_ISSUE_INTAKE_OUTPUT_DIR,
    ExcelIssueIntakeOptions,
    build_excel_issue_intake,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize an exported Excel workbook and extract likely issue rows/blanks before root-cause analysis."
    )
    parser.add_argument("--excel-path", required=True)
    parser.add_argument("--issue-text-path", default="")
    parser.add_argument("--output-dir", default=str(DEFAULT_EXCEL_ISSUE_INTAKE_OUTPUT_DIR))
    parser.add_argument("--limit-preview", type=int, default=10)
    parser.add_argument("--max-blank-details", type=int, default=5000)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    result = build_excel_issue_intake(
        ExcelIssueIntakeOptions(
            excel_path=Path(args.excel_path),
            issue_text_path=Path(args.issue_text_path) if args.issue_text_path else None,
            output_dir=Path(args.output_dir),
            limit_preview=max(int(args.limit_preview), 0),
            max_blank_details=max(int(args.max_blank_details), 0),
        )
    )

    print(f"intake_id={result.intake_id}")
    print(f"status={result.status}")
    print(f"sheet_count={result.summary.get('sheet_count', 0)}")
    print(f"priority_sheet_count={result.summary.get('priority_sheet_count', 0)}")
    print(f"issue_term_count={result.summary.get('issue_term_count', 0)}")
    print(f"blank_cell_count={result.summary.get('blank_cell_count', 0)}")
    print(f"blank_detail_count={result.summary.get('blank_detail_count', 0)}")
    print(f"blank_detail_truncated={result.summary.get('blank_detail_truncated', False)}")
    print(f"matched_issue_row_count={result.summary.get('matched_issue_row_count', 0)}")
    print(f"json_path={result.json_path}")
    print(f"excel_path={result.report_excel_path}")

    preview_limit = max(int(args.limit_preview), 0)
    for row in result.blank_cells[:preview_limit]:
        print(
            "blank="
            f"{row.sheet_name}|{row.row_number}|{row.column_letter}|"
            f"{row.header}|{row.security_code}|{row.company_name}|{row.metric}"
        )
    for row in result.matched_rows[:preview_limit]:
        print(
            "matched_row="
            f"{row.sheet_name}|{row.row_number}|{','.join(row.matched_issue_terms)}|"
            f"{row.security_code}|{row.company_name}|{row.metric}"
        )


if __name__ == "__main__":
    main()

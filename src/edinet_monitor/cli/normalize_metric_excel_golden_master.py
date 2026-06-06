from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.services.metric_excel_golden_master_service import (
    DEFAULT_GOLDEN_MASTER_DIR,
    write_metric_excel_normalized_json,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize metric Excel workbook(s) into style-free Golden Master JSON."
    )
    parser.add_argument("--excel-path", action="append", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_GOLDEN_MASTER_DIR))
    parser.add_argument("--output-json", default="")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    excel_paths = [Path(value) for value in args.excel_path]
    output_json = str(args.output_json or "").strip()
    if output_json and len(excel_paths) != 1:
        raise ValueError("--output-json can only be used with exactly one --excel-path")

    for excel_path in excel_paths:
        result = write_metric_excel_normalized_json(
            excel_path,
            output_path=Path(output_json) if output_json else None,
            output_dir=Path(args.output_dir),
        )
        print(f"excel_path={result.excel_path}")
        print(f"json_path={result.output_path}")
        print(f"sheet_count={result.sheet_count}")
        print(f"row_count={result.row_count}")


if __name__ == "__main__":
    main()

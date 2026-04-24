from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.metric_excel_export_service import export_metric_excel


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export selected metrics from the monitor DB to an Excel workbook."
    )
    parser.add_argument("--condition-xlsx", required=True)
    parser.add_argument("--output-dir", default=r"D:\作業用")
    parser.add_argument("--output-name", default="")
    parser.add_argument("--limit-preview", type=int, default=10)
    return parser


def _build_output_path(output_dir: str, output_name: str) -> Path:
    directory = Path(output_dir)
    if output_name:
        return directory / output_name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return directory / f"metric_export_{timestamp}.xlsx"


def main() -> None:
    args = build_arg_parser().parse_args()
    output_path = _build_output_path(args.output_dir, args.output_name)

    create_tables()
    conn = get_connection()
    try:
        result = export_metric_excel(
            conn,
            condition_xlsx=args.condition_xlsx,
            output_path=output_path,
            db_path=DB_PATH,
            preview_limit=args.limit_preview,
        )
    finally:
        conn.close()

    print(f"output_path={result.output_path}")
    print(f"target_companies={result.target_companies}")
    print(f"output_rows={result.output_rows}")
    print(f"errors={len(result.errors)}")
    print(f"warnings={len(result.warnings)}")
    for error in result.errors[: args.limit_preview]:
        print(f"error={error}")
    for warning in result.warnings[: args.limit_preview]:
        print(f"warning={warning}")

    print(f"preview_rows={len(result.preview_rows)}")
    for idx, row in enumerate(result.preview_rows, start=1):
        print(f"preview_{idx}={row}")


if __name__ == "__main__":
    main()

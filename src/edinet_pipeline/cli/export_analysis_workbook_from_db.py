from __future__ import annotations

import argparse
from pathlib import Path

from edinet_pipeline.config.settings import OUTPUT_ROOT, TEMPLATE_DIR, TEMPLATE_WORKBOOK_NAME
from edinet_pipeline.services.db_excel_export_service import (
    DEFAULT_CONDITION_XLSX_PATH,
    DEFAULT_DB_PATH,
    connect_db,
    export_analysis_workbooks_from_db,
)


def _split_codes(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export analysis workbooks from edinet_monitor DB values.",
    )
    parser.add_argument("--condition-xlsx", default=None)
    parser.add_argument("--security-codes", default=None)
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--template", default=str(TEMPLATE_DIR / TEMPLATE_WORKBOOK_NAME))
    parser.add_argument("--output-dir", default=str(OUTPUT_ROOT / "excel"))
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    condition_xlsx = args.condition_xlsx
    if condition_xlsx is None and not args.security_codes and DEFAULT_CONDITION_XLSX_PATH.exists():
        condition_xlsx = str(DEFAULT_CONDITION_XLSX_PATH)

    conn = connect_db(args.db_path)
    try:
        result = export_analysis_workbooks_from_db(
            conn,
            condition_xlsx=condition_xlsx,
            security_codes=_split_codes(args.security_codes),
            db_path=Path(args.db_path),
            template_path=Path(args.template),
            output_dir=Path(args.output_dir),
        )
    finally:
        conn.close()

    print(f"target_companies={result.target_companies}")
    print(f"output_files={len(result.output_paths)}")
    print(f"errors={len(result.errors)}")
    print(f"warnings={len(result.warnings)}")
    for path in result.output_paths[:10]:
        print(f"output_path={path}")
    for warning in result.warnings[:10]:
        print(f"warning={warning}")
    for error in result.errors[:10]:
        print(f"error={error}")

    if result.errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

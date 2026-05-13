from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.edinet_period_prune_service import prune_old_edinet_period_data


def _split_form_codes(value: str) -> tuple[str, ...]:
    codes = tuple(item.strip() for item in str(value or "").split(",") if item.strip())
    return codes or ("030000",)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dry-run or apply pruning of old EDINET filing data while keeping latest periods."
    )
    parser.add_argument("--keep-latest", type=int, default=11)
    parser.add_argument(
        "--form-codes",
        default="030000",
        help="Comma-separated EDINET form codes to prune independently. Example: 030000,043A00",
    )
    parser.add_argument(
        "--delete-files",
        action="store_true",
        help="Also delete matching ZIP/XBRL files and remove rows from manifest JSONL files.",
    )
    parser.add_argument(
        "--include-tenbagger-learning",
        action="store_true",
        help="Do not protect tenbagger learning securities from pruning.",
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    create_tables()
    conn = get_connection()
    try:
        result = prune_old_edinet_period_data(
            conn,
            keep_latest=args.keep_latest,
            form_types=_split_form_codes(args.form_codes),
            exclude_security_codes=frozenset() if args.include_tenbagger_learning else None,
            delete_files=args.delete_files,
            apply=args.apply,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"apply={1 if result.apply else 0}")
    print(f"keep_latest={result.keep_latest}")
    print(f"form_types={','.join(result.form_types)}")
    print(f"candidate_filings={result.candidate_count}")
    for table_name, count in result.deleted_counts.items():
        print(f"{table_name}={count}")
    for key, count in result.file_counts.items():
        print(f"{key}={count}")
    print(f"output_path={result.output_path}")


if __name__ == "__main__":
    main()

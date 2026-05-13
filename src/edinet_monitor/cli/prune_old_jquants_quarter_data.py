from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.jquants_period_prune_service import prune_old_jquants_quarter_data


def _split_quarter_types(value: str) -> tuple[str, ...]:
    quarters = tuple(item.strip().upper() for item in str(value or "").split(",") if item.strip())
    return quarters or ("1Q", "2Q", "3Q")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dry-run or apply pruning of old J-Quants quarter statement data."
    )
    parser.add_argument("--keep-latest", type=int, default=11)
    parser.add_argument(
        "--quarter-types",
        default="1Q,2Q,3Q",
        help="Comma-separated quarter types to prune independently. Example: 1Q,2Q,3Q",
    )
    parser.add_argument(
        "--delete-files",
        action="store_true",
        help="Also remove matching records from J-Quants fins_summary raw JSONL files.",
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
        result = prune_old_jquants_quarter_data(
            conn,
            keep_latest=args.keep_latest,
            quarter_types=_split_quarter_types(args.quarter_types),
            exclude_security_codes=frozenset() if args.include_tenbagger_learning else None,
            delete_files=args.delete_files,
            apply=args.apply,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"apply={1 if result.apply else 0}")
    print(f"keep_latest={result.keep_latest}")
    print(f"quarter_types={','.join(result.quarter_types)}")
    print(f"candidate_disclosures={result.candidate_count}")
    for table_name, count in result.deleted_counts.items():
        print(f"{table_name}={count}")
    for key, count in result.file_counts.items():
        print(f"{key}={count}")
    print(f"output_path={result.output_path}")


if __name__ == "__main__":
    main()

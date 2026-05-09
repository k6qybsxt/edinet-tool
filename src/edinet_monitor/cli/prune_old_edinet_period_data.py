from __future__ import annotations

import argparse

from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.edinet_period_prune_service import prune_old_edinet_period_data


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dry-run or apply pruning of old EDINET annual filing data while keeping latest periods."
    )
    parser.add_argument("--keep-latest", type=int, default=11)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output-dir", default="D:\\作業用\\log")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    create_tables()
    conn = get_connection()
    try:
        result = prune_old_edinet_period_data(
            conn,
            keep_latest=args.keep_latest,
            apply=args.apply,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"apply={1 if result.apply else 0}")
    print(f"keep_latest={result.keep_latest}")
    print(f"candidate_filings={result.candidate_count}")
    for table_name, count in result.deleted_counts.items():
        print(f"{table_name}={count}")
    print(f"output_path={result.output_path}")


if __name__ == "__main__":
    main()

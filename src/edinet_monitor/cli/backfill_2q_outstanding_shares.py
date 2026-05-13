from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.half_outstanding_shares_backfill_service import (
    backfill_2q_outstanding_shares,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill 2Q outstanding shares from half-report share table facts."
    )
    parser.add_argument("--apply", action="store_true", help="Write normalized/derived rows to DB.")
    parser.add_argument(
        "--output-dir",
        default=str(OPERATION_LOG_ROOT),
        help="Directory for the summary report.",
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    create_tables()
    conn = get_connection()
    try:
        result = backfill_2q_outstanding_shares(
            conn,
            apply=args.apply,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"apply={result['apply']}")
    print(f"target_docs={result['target_docs']}")
    print(f"candidate_actions={result['candidate_actions']}")
    print(f"ok_rows={result['ok_rows']}")
    print(f"total_rows={result['total_rows']}")
    print(f"ok_rate={result['ok_rate'] * 100:.1f}%")
    if result.get("report_path"):
        print(f"report_path={result['report_path']}")


if __name__ == "__main__":
    main()

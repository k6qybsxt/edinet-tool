from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.quarter_standalone_metric_service import (
    save_quarter_standalone_metrics,
)


def _split_csv(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build standalone-quarter metrics from cumulative 1Q/2Q/3Q/4Q values."
    )
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--codes", default="all")
    parser.add_argument("--apply", action="store_true", help="Save rows to DB. Omit for dry-run.")
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.apply:
        create_tables()
    conn = get_connection()
    try:
        result = save_quarter_standalone_metrics(
            conn,
            date_from=args.date_from,
            date_to=args.date_to,
            codes=_split_csv(args.codes),
            apply=args.apply,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"mode={'apply' if args.apply else 'dry_run'}")
    print(f"rows={len(result.rows)}")
    print(f"saved_rows={result.saved_rows}")
    print(f"warnings={len(result.warnings)}")
    print(f"output_path={result.output_path}")
    if not args.apply:
        print("dry_run_only=1")
        print("hint=DBへ保存する場合は --apply を付けてください。")


if __name__ == "__main__":
    main()

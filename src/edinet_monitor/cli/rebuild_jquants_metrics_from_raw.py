from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.jquants.raw_rebuild_service import (
    rebuild_jquants_financial_metrics_from_raw,
)


def _split_csv(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rebuild J-Quants financial metrics from stored raw JSON without API calls."
    )
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--periods", default="FY,1Q,2Q,3Q")
    parser.add_argument("--codes", default="all")
    parser.add_argument("--no-forecasts", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    if args.apply:
        create_tables()
    conn = get_connection()
    try:
        result = rebuild_jquants_financial_metrics_from_raw(
            conn,
            date_from=args.date_from,
            date_to=args.date_to,
            periods=set(_split_csv(args.periods) or ["FY", "1Q", "2Q", "3Q"]),
            include_forecasts=not args.no_forecasts,
            codes=_split_csv(args.codes),
            apply=args.apply,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"apply={int(result.apply)}")
    print(f"raw_rows={result.raw_rows}")
    print(f"metrics_built={result.metrics_built}")
    print(f"metrics_saved={result.metrics_saved}")
    print(f"skipped_rows={result.skipped_rows}")
    print(f"error_rows={result.error_rows}")
    print(f"output_path={result.output_path}")


if __name__ == "__main__":
    main()

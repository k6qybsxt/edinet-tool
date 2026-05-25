from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.jquants.client import JQuantsClient
from edinet_monitor.services.jquants.coverage_service import export_jquants_coverage
from edinet_monitor.services.jquants.audit_ingestion_service import save_jquants_listed_info
from edinet_monitor.services.jquants.ingestion_service import (
    save_jquants_daily_quotes,
    save_jquants_statements,
)


def _split_csv(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run J-Quants V2 statements/quotes backfill and coverage.")
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--codes", default="all")
    parser.add_argument("--statement-periods", default="1Q,3Q")
    parser.add_argument("--skip-statements", action="store_true")
    parser.add_argument("--skip-quotes", action="store_true")
    parser.add_argument("--include-listed-info", action="store_true")
    parser.add_argument("--save-raw-json", action="store_true")
    parser.add_argument("--raw-json-root", default=None)
    parser.add_argument("--request-interval-sec", type=float, default=None)
    parser.add_argument("--rate-limit-cooldown-sec", type=float, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    codes = _split_csv(args.codes)
    create_tables()
    conn = get_connection()
    try:
        client = JQuantsClient(
            request_interval_sec=args.request_interval_sec,
            rate_limit_cooldown_sec=args.rate_limit_cooldown_sec,
            max_retries=args.max_retries,
        )
        if not args.skip_statements:
            statement_result = save_jquants_statements(
                conn,
                client=client,
                date_from=args.date_from,
                date_to=args.date_to,
                periods=set(_split_csv(args.statement_periods) or ["1Q", "3Q"]),
                include_forecasts=True,
                codes=codes,
                output_dir=args.output_dir,
                save_raw_json=args.save_raw_json,
                raw_json_storage_root=args.raw_json_root,
            )
            print(f"statements_saved={statement_result.saved_total}")
        if not args.skip_quotes:
            quote_result = save_jquants_daily_quotes(
                conn,
                client=client,
                date_from=args.date_from,
                date_to=args.date_to,
                codes=codes,
                output_dir=args.output_dir,
            )
            print(f"quotes_saved={quote_result.saved_total}")
        if args.include_listed_info:
            listed_result = save_jquants_listed_info(
                conn,
                client=client,
                date_value=args.date_to,
                codes=codes,
                output_dir=args.output_dir,
            )
            print(f"listed_info_saved={listed_result.saved_total}")
            print(f"listed_info_warnings={len(listed_result.warnings)}")
        coverage = export_jquants_coverage(
            conn,
            date_from=args.date_from,
            date_to=args.date_to,
            target="all",
            codes=codes,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()
    print(f"coverage_path={coverage.output_path}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.db_reflection_preflight_guard_service import (
    mark_db_reflection_preflight_guard_success,
    run_db_reflection_preflight_guard,
)
from edinet_monitor.services.jquants.client import JQuantsClient
from edinet_monitor.services.jquants.ingestion_service import save_jquants_daily_quotes


def _split_csv(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Save J-Quants V2 adjusted daily bars to DB.")
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--codes", default="all")
    parser.add_argument("--request-interval-sec", type=float, default=None)
    parser.add_argument("--rate-limit-cooldown-sec", type=float, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    guard_result = run_db_reflection_preflight_guard(cli_name="save_jquants_daily_quotes")
    create_tables()
    conn = get_connection()
    try:
        result = save_jquants_daily_quotes(
            conn,
            client=JQuantsClient(
                request_interval_sec=args.request_interval_sec,
                rate_limit_cooldown_sec=args.rate_limit_cooldown_sec,
                max_retries=args.max_retries,
            ),
            date_from=args.date_from,
            date_to=args.date_to,
            codes=_split_csv(args.codes),
            output_dir=args.output_dir,
        )
    finally:
        conn.close()
    mark_db_reflection_preflight_guard_success(guard_result)

    print(f"run_id={result.run_id}")
    print(f"fetched_total={result.fetched_total}")
    print(f"saved_total={result.saved_total}")
    print(f"skipped_total={result.skipped_total}")
    print(f"error_total={result.error_total}")
    print(f"output_path={result.output_path}")


if __name__ == "__main__":
    main()

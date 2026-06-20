from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.db_reflection_preflight_guard_service import (
    mark_db_reflection_preflight_guard_success,
    run_db_reflection_preflight_guard,
)
from edinet_monitor.services.jquants.audit_ingestion_service import save_jquants_listed_info
from edinet_monitor.services.jquants.client import JQuantsClient


def _split_csv(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Save J-Quants listed issue master rows and validation report.")
    parser.add_argument("--date", required=True)
    parser.add_argument("--codes", default="all")
    parser.add_argument("--request-interval-sec", type=float, default=None)
    parser.add_argument("--rate-limit-cooldown-sec", type=float, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    guard_result = run_db_reflection_preflight_guard(cli_name="save_jquants_listed_info")
    create_tables()
    conn = get_connection()
    try:
        client = JQuantsClient(
            request_interval_sec=args.request_interval_sec,
            rate_limit_cooldown_sec=args.rate_limit_cooldown_sec,
            max_retries=args.max_retries,
        )
        result = save_jquants_listed_info(
            conn,
            client=client,
            date_value=args.date,
            codes=_split_csv(args.codes),
            output_dir=args.output_dir,
        )
    finally:
        conn.close()
    mark_db_reflection_preflight_guard_success(guard_result)

    print(f"fetched_total={result.fetched_total}")
    print(f"saved_total={result.saved_total}")
    print(f"warnings={len(result.warnings)}")
    print(f"output_path={result.output_path}")


if __name__ == "__main__":
    main()

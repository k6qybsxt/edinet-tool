from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.jquants.audit_ingestion_service import save_jquants_fs_details
from edinet_monitor.services.jquants.client import JQuantsClient


def _split_csv(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Save J-Quants financial statement detail rows for audit.")
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--codes", default="all")
    parser.add_argument("--request-interval-sec", type=float, default=None)
    parser.add_argument("--rate-limit-cooldown-sec", type=float, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    create_tables()
    conn = get_connection()
    try:
        client = JQuantsClient(
            request_interval_sec=args.request_interval_sec,
            rate_limit_cooldown_sec=args.rate_limit_cooldown_sec,
            max_retries=args.max_retries,
        )
        result = save_jquants_fs_details(
            conn,
            client=client,
            date_from=args.date_from,
            date_to=args.date_to,
            codes=_split_csv(args.codes),
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"fetched_total={result.fetched_total}")
    print(f"raw_saved_total={result.raw_saved_total}")
    print(f"item_saved_total={result.item_saved_total}")
    print(f"skipped_total={result.skipped_total}")
    print(f"warnings={len(result.warnings)}")
    print(f"output_path={result.output_path}")


if __name__ == "__main__":
    main()

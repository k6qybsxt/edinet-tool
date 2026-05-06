from __future__ import annotations

import argparse

from edinet_monitor.services.jquants.client import JQuantsClient
from edinet_monitor.services.jquants.oldest_date_service import discover_oldest_fins_summary_date


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover oldest available J-Quants V2 fins_summary date.")
    parser.add_argument("--date-from", default="2016-01-01")
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--target", choices=["fins_summary"], default="fins_summary")
    parser.add_argument("--step", choices=["month"], default="month")
    parser.add_argument("--seed-codes", default="7203")
    parser.add_argument("--lookback-months", type=int, default=3)
    parser.add_argument("--request-interval-sec", type=float, default=None)
    parser.add_argument("--rate-limit-cooldown-sec", type=float, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--output-dir", default="D:\\\u4f5c\u696d\u7528")
    parser.add_argument("--storage-root", default=None)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    result = discover_oldest_fins_summary_date(
        client=JQuantsClient(
            request_interval_sec=args.request_interval_sec,
            rate_limit_cooldown_sec=args.rate_limit_cooldown_sec,
            max_retries=args.max_retries,
        ),
        date_from=args.date_from,
        date_to=args.date_to,
        output_dir=args.output_dir,
        storage_root=args.storage_root if args.storage_root else None,
        seed_codes=_split_csv(args.seed_codes),
        lookback_months=args.lookback_months,
    )
    print(f"target={result.target}")
    print(f"date_from={result.date_from}")
    print(f"date_to={result.date_to}")
    print(f"oldest_date={result.oldest_date}")
    print(f"oldest_month={result.oldest_month}")
    print(f"checked_days={result.checked_days}")
    print(f"hit_count={result.hit_count}")
    print(f"errors={len(result.errors)}")
    print(f"output_path={result.output_path}")
    print(f"manifest_path={result.manifest_path}")


def _split_csv(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "none":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


if __name__ == "__main__":
    main()

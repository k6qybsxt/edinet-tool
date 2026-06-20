from __future__ import annotations

import argparse

from edinet_monitor.config.settings import OPERATION_LOG_ROOT
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.db_reflection_preflight_guard_service import (
    mark_db_reflection_preflight_guard_success,
    run_db_reflection_preflight_guard,
)
from edinet_monitor.services.market_derived_metric_service import save_market_derived_metrics


def _split_csv(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def _split_scopes(value: str) -> set[str]:
    parts = {part.strip().lower() for part in str(value or "").split(",") if part.strip()}
    if not parts or "all" in parts:
        return {"annual", "quarter"}
    invalid = parts - {"annual", "quarter"}
    if invalid:
        raise SystemExit(f"Unsupported period scopes: {','.join(sorted(invalid))}")
    return parts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build market-derived metrics from J-Quants adjusted quotes.")
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--codes", default="all")
    parser.add_argument("--period-scopes", default="all", help="all / annual / quarter / annual,quarter")
    parser.add_argument("--max-lookback-days", type=int, default=10)
    parser.add_argument("--apply", action="store_true", help="Save rows to DB. Omit for dry-run.")
    parser.add_argument("--output-dir", default=str(OPERATION_LOG_ROOT))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    guard_result = None
    if args.apply:
        guard_result = run_db_reflection_preflight_guard(
            cli_name="save_market_derived_metrics"
        )
    if args.apply:
        create_tables()
    conn = get_connection()
    try:
        result = save_market_derived_metrics(
            conn,
            date_from=args.date_from,
            date_to=args.date_to,
            codes=_split_csv(args.codes),
            period_scopes=_split_scopes(args.period_scopes),
            max_lookback_days=args.max_lookback_days,
            apply=args.apply,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"mode={'apply' if args.apply else 'dry_run'}")
    print(f"rows={len(result.rows)}")
    print(f"missing_quotes={result.missing_quotes}")
    print(f"warnings={len(result.warnings)}")
    print(f"output_path={result.output_path}")
    if args.apply:
        mark_db_reflection_preflight_guard_success(guard_result)
    if not args.apply:
        print("dry_run_only=1")
        print("hint=DBへ保存する場合は --apply を付けてください。")


if __name__ == "__main__":
    main()

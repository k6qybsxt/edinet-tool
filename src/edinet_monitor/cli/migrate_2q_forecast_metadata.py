from __future__ import annotations

import argparse

from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.db_reflection_preflight_guard_service import (
    mark_db_reflection_preflight_guard_success,
    run_db_reflection_preflight_guard,
)
from edinet_monitor.services.quarter_forecast_metadata_migration_service import (
    migrate_quarter_forecast_metadata,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill 2Q period metadata and J-Quants forecast stages."
    )
    parser.add_argument("--apply", action="store_true", help="Apply DB updates. Omit for dry-run.")
    parser.add_argument("--output-dir", default=r"D:\作業用")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    guard_result = None
    if args.apply:
        guard_result = run_db_reflection_preflight_guard(
            cli_name="migrate_2q_forecast_metadata"
        )
    create_tables()
    conn = get_connection()
    try:
        result = migrate_quarter_forecast_metadata(
            conn,
            apply=args.apply,
            output_dir=args.output_dir,
        )
    finally:
        conn.close()

    print(f"apply={int(result.apply)}")
    print(f"annual_derived_candidates={result.annual_derived_candidates}")
    print(f"q2_derived_candidates={result.q2_derived_candidates}")
    print(f"forecast_stage_candidates={result.forecast_stage_candidates}")
    print(f"obsolete_forecast_candidates={result.obsolete_forecast_candidates}")
    print(f"annual_derived_updated={result.annual_derived_updated}")
    print(f"q2_derived_updated={result.q2_derived_updated}")
    print(f"forecast_stage_updated={result.forecast_stage_updated}")
    print(f"obsolete_forecast_deleted={result.obsolete_forecast_deleted}")
    if result.output_path:
        print(f"output_path={result.output_path}")
    if args.apply:
        mark_db_reflection_preflight_guard_success(guard_result)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import sqlite3

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.db_reflection_preflight_guard_service import (
    mark_db_reflection_preflight_guard_success,
    run_db_reflection_preflight_guard,
)
from edinet_monitor.services.obsolete_quarter_metric_service import (
    count_obsolete_quarter_metrics,
    delete_obsolete_quarter_metrics,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Count or delete obsolete quarter metrics from metric tables."
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--apply", action="store_true", help="Delete rows. Omit for dry-run.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    guard_result = None
    if args.apply:
        guard_result = run_db_reflection_preflight_guard(
            cli_name="cleanup_obsolete_quarter_metrics",
            db_path=args.db_path,
        )
    conn = get_connection(args.db_path)
    try:
        conn.row_factory = sqlite3.Row
        rows = count_obsolete_quarter_metrics(conn)
        total = sum(int(row["row_count"]) for row in rows)
        print(f"db_path={args.db_path}")
        print(f"mode={'apply' if args.apply else 'dry_run'}")
        print(f"target_rows={total}")
        for row in rows:
            print(
                f"{row['table_name']}:{row['target_scope']}:{row['metric_base']}: "
                f"{row['row_count']}"
            )
        if args.apply:
            deleted = delete_obsolete_quarter_metrics(conn)
            print(f"deleted_rows={deleted}")
            remaining = sum(int(row["row_count"]) for row in count_obsolete_quarter_metrics(conn))
            print(f"remaining_rows={remaining}")
            mark_db_reflection_preflight_guard_success(guard_result)
        else:
            print("dry_run_only=1")
            print("hint=run with --apply to delete rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

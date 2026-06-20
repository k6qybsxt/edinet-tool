from __future__ import annotations

import argparse

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import get_connection
from edinet_monitor.services.preflight_history_service import (
    PreflightHistoryCleanupOptions,
    cleanup_preflight_history,
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Delete old persisted preflight history rows.")
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--keep-days", type=int, default=180)
    parser.add_argument("--keep-critical-days", type=int, default=730)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--vacuum", action="store_true")
    parser.add_argument("--limit-preview", type=int, default=20)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    conn = get_connection(args.db_path)
    try:
        result = cleanup_preflight_history(
            conn,
            options=PreflightHistoryCleanupOptions(
                keep_days=max(int(args.keep_days), 0),
                keep_critical_days=max(int(args.keep_critical_days), 0),
                apply=bool(args.apply),
                vacuum=bool(args.vacuum),
                limit_preview=max(int(args.limit_preview), 0),
            ),
        )
    finally:
        conn.close()

    print(f"db_path={args.db_path}")
    print(f"mode={result.mode}")
    print(f"keep_days={result.keep_days}")
    print(f"keep_critical_days={result.keep_critical_days}")
    print(f"target_count={result.target_count}")
    print(f"deleted_count={result.deleted_count}")
    print(f"issue_deleted_count={result.issue_deleted_count}")
    print(f"vacuumed={result.vacuumed}")
    for row in result.preview:
        print(
            "target="
            f"{row.get('preflight_id')}|{row.get('generated_at')}|"
            f"{row.get('cli_name')}|{row.get('status')}|"
            f"critical={row.get('critical_count')}|warning={row.get('warning_count')}"
        )


if __name__ == "__main__":
    main()

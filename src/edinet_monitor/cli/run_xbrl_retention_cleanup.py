from __future__ import annotations

import argparse

from edinet_monitor.config.settings import XBRL_RETENTION_ENABLED, XBRL_RETENTION_MONTHS
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.storage.xbrl_retention_service import cleanup_old_xbrl_storage


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--keep-months",
        type=int,
        default=XBRL_RETENTION_MONTHS,
        help="Number of submit months to keep. Defaults to settings value.",
    )
    parser.add_argument(
        "--disable",
        action="store_true",
        help="Skip deletion even if settings enable retention.",
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    create_tables()

    conn = get_connection()
    try:
        summary = cleanup_old_xbrl_storage(
            conn,
            enabled=(XBRL_RETENTION_ENABLED and not args.disable),
            keep_months=args.keep_months,
        )
    finally:
        conn.close()

    print(f"xbrl_retention_enabled={int(XBRL_RETENTION_ENABLED and not args.disable)}")
    print(f"xbrl_retention_keep_months={args.keep_months}")
    print(f"xbrl_retention_status={summary['status']}")
    print(f"xbrl_retention_reason={summary['reason']}")
    print(f"xbrl_retention_reference_month={summary['reference_month']}")
    print(f"xbrl_retention_keep_from_month={summary['keep_from_month']}")
    print(f"xbrl_retention_target_total={summary['target_total']}")
    print(f"xbrl_retention_deleted_total={summary['deleted_total']}")
    print(f"xbrl_retention_missing_file_total={summary['missing_file_total']}")
    print(f"xbrl_retention_error_total={summary['error_total']}")


if __name__ == "__main__":
    main()

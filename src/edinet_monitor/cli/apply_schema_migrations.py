from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.migrations import SchemaMigrationStatus, apply_schema_migrations
from edinet_monitor.db.schema import create_tables, get_connection


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply lightweight SQLite schema migrations.")
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--all", action="store_true", help="Show all applied migrations.")
    parser.add_argument("--applied-limit", type=int, default=100)
    return parser


def _display_statuses(
    statuses: list[SchemaMigrationStatus],
    *,
    show_all: bool,
    applied_limit: int,
) -> list[SchemaMigrationStatus]:
    pending = [status for status in statuses if not status.applied]
    applied = [status for status in statuses if status.applied]
    if not show_all:
        applied = sorted(
            applied,
            key=lambda status: (status.applied_at, status.migration_id),
            reverse=True,
        )[: max(applied_limit, 0)]
        applied = sorted(applied, key=lambda status: status.migration_id)
    return [*pending, *applied]


def main() -> None:
    args = build_arg_parser().parse_args()
    db_path = Path(args.db_path)
    if args.dry_run:
        conn = get_connection(db_path)
        try:
            statuses = apply_schema_migrations(conn, dry_run=True)
        finally:
            conn.close()
    else:
        create_tables(db_path)
        conn = get_connection(db_path)
        try:
            statuses = apply_schema_migrations(conn, dry_run=True)
        finally:
            conn.close()

    for status in _display_statuses(
        statuses,
        show_all=args.all,
        applied_limit=args.applied_limit,
    ):
        state = "applied" if status.applied else "pending"
        suffix = f"\t{status.applied_at}" if status.applied_at else ""
        print(f"{status.migration_id}\t{state}\t{status.description}{suffix}")


if __name__ == "__main__":
    main()

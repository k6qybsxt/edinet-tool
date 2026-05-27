from __future__ import annotations

import argparse
from pathlib import Path

from edinet_monitor.config.settings import DB_PATH
from edinet_monitor.db.schema import create_tables, get_connection
from edinet_monitor.services.db_reflection_item_service import (
    ALLOWED_CATEGORIES,
    DbReflectionItem,
    add_db_reflection_item,
    complete_db_reflection_item,
    get_db_reflection_item,
    import_db_reflection_items_from_txt,
    list_db_reflection_items,
)


def _db_parent() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db-path", default=str(DB_PATH))
    return parser


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage pending DB reflection items.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    parent = _db_parent()

    add_parser = subparsers.add_parser("add", parents=[parent])
    add_parser.add_argument("--title", required=True)
    add_parser.add_argument("--category", choices=sorted(ALLOWED_CATEGORIES), default="other")
    add_parser.add_argument("--description", default="")
    add_parser.add_argument("--required-command", action="append", default=[])
    add_parser.add_argument("--verification-sql", action="append", default=[])
    add_parser.add_argument("--related-migration-id", action="append", default=[])
    add_parser.add_argument("--notes", default="")

    subparsers.add_parser("list", parents=[parent])

    show_parser = subparsers.add_parser("show", parents=[parent])
    show_parser.add_argument("--item-id", type=int, required=True)

    complete_parser = subparsers.add_parser("complete", parents=[parent])
    complete_parser.add_argument("--item-id", type=int, required=True)

    import_parser = subparsers.add_parser("import-txt", parents=[parent])
    import_parser.add_argument("--path", required=True)
    return parser


def _connect(db_path: str):
    path = Path(db_path)
    create_tables(path)
    return get_connection(path)


def _print_item_summary(item: DbReflectionItem) -> None:
    print(f"{item.item_id}\t{item.category}\t{item.title}\t{item.created_at}")


def _print_item_detail(item: DbReflectionItem) -> None:
    print(f"item_id={item.item_id}")
    print(f"title={item.title}")
    print(f"category={item.category}")
    print(f"created_at={item.created_at}")
    print(f"updated_at={item.updated_at}")
    print(f"source_path={item.source_path}")
    print(f"source_key={item.source_key}")
    print("description:")
    print(item.description)
    for idx, command in enumerate(item.required_commands, start=1):
        print(f"required_command_{idx}={command}")
    for idx, sql in enumerate(item.verification_sql, start=1):
        print(f"verification_sql_{idx}={sql}")
    for idx, migration_id in enumerate(item.related_migration_ids, start=1):
        print(f"related_migration_id_{idx}={migration_id}")
    if item.notes:
        print("notes:")
        print(item.notes)


def main() -> None:
    args = build_arg_parser().parse_args()
    conn = _connect(args.db_path)
    try:
        if args.command == "add":
            item = add_db_reflection_item(
                conn,
                title=args.title,
                category=args.category,
                description=args.description,
                required_commands=args.required_command,
                verification_sql=args.verification_sql,
                related_migration_ids=args.related_migration_id,
                notes=args.notes,
            )
            print(f"item_id={item.item_id}")
            print(f"title={item.title}")
            print(f"category={item.category}")
        elif args.command == "list":
            items = list_db_reflection_items(conn)
            if not items:
                print("no_items=1")
            for item in items:
                _print_item_summary(item)
        elif args.command == "show":
            item = get_db_reflection_item(conn, args.item_id)
            if item is None:
                print(f"not_found={args.item_id}")
            else:
                _print_item_detail(item)
        elif args.command == "complete":
            completed = complete_db_reflection_item(conn, args.item_id)
            if completed:
                print(f"completed_item_id={args.item_id}")
            else:
                print(f"not_found={args.item_id}")
        elif args.command == "import-txt":
            result = import_db_reflection_items_from_txt(conn, path=args.path)
            print(f"imported_count={result.imported_count}")
            print(f"skipped_count={result.skipped_count}")
            for item_id in result.item_ids:
                print(f"item_id={item_id}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
